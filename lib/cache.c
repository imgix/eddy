#include "eddy-private.h"

static int
obj_new(EdObject **objp, const void *k, size_t klen, bool rdonly)
{
	EdObject *obj = calloc(1, sizeof(*obj) + (rdonly ? 0 : klen));
	if (obj == NULL) { return ED_ERRNO; }
	if (rdonly) {
		obj->rdonly = true;
	}
	else {
		obj->rdonly = false;
		memcpy(obj->newkey, k, klen);
	}
	*objp = obj;
	return 0;
}

static size_t
obj_key_offset(void)
{
	return sizeof(EdObjectHdr);
}

static size_t
obj_meta_offset(uint16_t keylen)
{
	return ed_align_max(obj_key_offset() + keylen + 1);
}

static size_t
obj_data_offset(uint16_t keylen, uint16_t metalen, uint64_t flags)
{
	if (flags & ED_FPAGEALIGN) {
		return ed_align_pg(obj_meta_offset(keylen) + metalen);
	}
	else {
		return ed_align_max(obj_meta_offset(keylen) + metalen);
	}
}

static size_t
obj_slab_size(uint16_t keylen, uint16_t metalen, uint32_t datalen, uint64_t flags)
{
	return ed_align_pg(obj_data_offset(keylen, metalen, flags) + datalen);
}

static uint8_t *
obj_key(EdObjectHdr *hdr)
{
	return (uint8_t *)hdr + obj_key_offset();
}

static uint8_t *
obj_meta(EdObjectHdr *hdr)
{
	return (uint8_t *)hdr + obj_meta_offset(hdr->keylen);
}

static uint8_t *
obj_data(EdObjectHdr *hdr, uint64_t flags)
{
	return (uint8_t *)hdr + obj_data_offset(hdr->keylen, hdr->metalen, flags);
}

static void
obj_init(EdObject *obj, EdCache *cache, EdObjectHdr *hdr, EdBlkno no, bool rdonly, EdTime exp)
{
	const uint64_t flags = cache->idx.flags;
	size_t size = obj_slab_size(hdr->keylen, hdr->metalen, hdr->datalen, flags);
	obj->cache = cache;
	obj->key = obj_key(hdr);
	obj->keylen = hdr->keylen;
	obj->meta = obj_meta(hdr);
	obj->metalen = hdr->metalen;
	obj->data = obj_data(hdr, flags);
	obj->datalen = hdr->datalen;
	obj->datacrc = hdr->datacrc;
	obj->hdr = hdr;
	obj->blck = no;
	obj->nblcks = size/PAGESIZE;
	obj->byte = no*PAGESIZE;
	obj->nbytes = size;
	obj->exp = exp;
	obj->rdonly = rdonly;
}

static void
obj_write(void *dst, const void *src, size_t len, uint32_t *crc, uint64_t flags)
{
	if (flags & ED_FCHECKSUM) {
		*crc = ed_crc32c(*crc, src, len);
	}
	memcpy(dst, src, len);
}

static void
obj_hdr_init(EdObjectHdr *hdr, const EdObjectAttr *attr, uint64_t h,
		size_t nbytes, uint64_t flags, EdTime now)
{
	madvise(hdr, nbytes, MADV_SEQUENTIAL);
	hdr->version = 0;
	hdr->flags = 0;
	hdr->tag = 0;
	hdr->created = now;
	hdr->keylen = attr->keylen;
	hdr->metalen = attr->metalen;
	hdr->datalen = attr->datalen;
	hdr->keyhash = h;
	hdr->metacrc = 0;
	hdr->datacrc = 0;

	uint8_t *key = obj_key(hdr), *meta = obj_meta(hdr);
	memcpy(key, attr->key, attr->keylen);
	key += attr->keylen;

	// Zero out the end of the key segment.
	memset(key, 0, obj_meta(hdr) - key);

	if (attr->meta != NULL && attr->metalen > 0) {
		obj_write(meta, attr->meta, attr->metalen, &hdr->metacrc, flags);
	}
	meta += attr->metalen;

	// Zero out the end of the meta segment.
	memset(meta, 0, obj_data(hdr, flags) - meta);
}

static void
obj_hdr_final(EdObjectHdr *hdr, size_t nbytes, uint64_t flags)
{
	uint8_t *data = obj_data(hdr, flags) + hdr->datalen;
	memset(data, 0, nbytes - (data - (uint8_t *)hdr));
}

static int
obj_reserve(EdTxn *txn, int slabfd, uint64_t flags, EdBlkno *posp, size_t len)
{
	EdBlkno pos = *posp, end;
	size_t start = pos * PAGESIZE;
	bool locked = false;
	EdEntryBlock *block;

	// Lookup an entry at the current slab position. Its likely nothing will be
	// matched which will leave the block NULL. We must be sure the check this
	// case when removing replaced entries.
	int rc = ed_bpt_find(txn, ED_DB_BLOCKS, pos, (void **)&block);
	if (rc < 0) { goto done; }

	// Find then next unlocked region >= #pos. If the current #pos cannot be used,
	// start from the beginning of the next entry.
	do {
		if (ed_flck(slabfd, ED_LCK_EX, start, len, flags|ED_FNOBLOCK) < 0) {
			rc = ed_bpt_next(txn, ED_DB_BLOCKS, (void **)&block);
			if (rc < 0) { goto done; }
			pos = block->no;
			start = pos * PAGESIZE;
		}
		else {
			locked = true;
			break;
		}
	} while(1);

	// Determine the first page number after the write region.
	end = pos + len/PAGESIZE;

	// If the original find didn't match and we never iterated to the next
	// position, load the next entry.
	if (block == NULL) {
		rc = ed_bpt_next(txn, ED_DB_BLOCKS, (void **)&block);
		if (rc < 0) { goto done; }
	}

	// Loop through objects by block position and remove the key and then block
	// entries in the index.
	while (block && block->no < end && block->no >= pos) {
		// Only the first page of the object is needed.
		EdObjectHdr *old = ed_pg_map(slabfd, block->no, 1, true);
		if (old == MAP_FAILED) { rc = ED_ERRNO; goto done; }

		// Loop through each key entry to resolve collisions. Key comparison is not
		// rquireds for this resolution. We are looking for the key that maps to
		// current block number.
		EdEntryKey *key;
		for (rc = ed_bpt_find(txn, ED_DB_KEYS, old->keyhash, (void **)&key); rc == 1;
				rc = ed_bpt_next(txn, ED_DB_KEYS, (void **)&key)) {
			if (key->no == block->no) {
				rc = ed_bpt_del(txn, ED_DB_KEYS);
				break;
			}
		}
		ed_pg_unmap(old, 1);
		if (rc < 0) { goto done; }

		rc = ed_bpt_del(txn, ED_DB_BLOCKS);
		if (rc < 0) { goto done; }
	}

done:
	if (rc < 0 && locked) {
		ed_flck(slabfd, ED_LCK_UN, start, len, flags);
	}
	return rc;
}

static int
obj_upsert(EdCache *cache, const void *k, size_t klen, uint64_t h,
		EdBlkno blck, EdBlkno nblcks, EdTime exp)
{
	EdTxn *txn = cache->txn;
	EdEntryBlock blocknew = { blck, nblcks, exp };
	EdEntryKey *key, keynew = { h, blck, nblcks, exp };
	bool replace = false;
	int rc;

	// Insert the slab position into the db.
	if ((rc = ed_bpt_find(txn, ED_DB_BLOCKS, blck, NULL)) < 0 || 
		(rc = ed_bpt_set(txn, ED_DB_BLOCKS, (void *)&blocknew, true) < 0)) {
		return rc;
	}

	// Insert the key into the db.
	for (rc = ed_bpt_find(txn, ED_DB_KEYS, h, (void **)&key); rc == 1;
			rc = ed_bpt_next(txn, ED_DB_KEYS, (void **)&key)) {
		// Map the slab object.
		EdObjectHdr *old = ed_pg_map(cache->idx.slabfd, key->no, 1, true);
		if (old == MAP_FAILED) { return ED_ERRNO; }

		replace = old->keylen == klen && memcmp(obj_key(old), k, klen) == 0;
		ed_pg_unmap(old, 1);
		if (replace) { break; }
	}
	if (rc >= 0) {
		rc = ed_bpt_set(txn, ED_DB_KEYS, (void *)&keynew, replace);
	}
	return rc;
}

int
ed_cache_open(EdCache **cachep, const EdConfig *cfg)
{
	int rc;
	EdCache *cache = malloc(sizeof(*cache));
	if (cache == NULL) { return ED_ERRNO; }

	rc = ed_idx_open(&cache->idx, cfg);
	if (rc < 0) { goto error_open; }

	rc = ed_txn_new(&cache->txn, &cache->idx);
	if (rc < 0) { goto error_txn; }

	cache->ref = 1;
	cache->bytes_used = 0;
	cache->blocks_used = 0;
	*cachep = cache;
	return 0;

error_txn:
	ed_idx_close(&cache->idx);
error_open:
	free(cache);
	return rc;
}

EdCache *
ed_cache_ref(EdCache *cache)
{
	if (cache != NULL) {
		atomic_fetch_add(&cache->ref, 1);
	}
	return cache;
}

void
ed_cache_close(EdCache **cachep)
{
	EdCache *cache = *cachep;
	if (cache != NULL) {
		*cachep = NULL;
		if (atomic_fetch_sub(&cache->ref, 1) == 1) {
			ed_txn_close(&cache->txn, cache->idx.flags);
			ed_idx_close(&cache->idx);
			free(cache);
		}
	}
}

int
ed_cache_stat(EdCache *cache, FILE *out, uint64_t flags)
{
	if (out == NULL) { out = stdout; }

	EdStat *stat;
	int rc = ed_stat_new(&stat, &cache->idx, flags);
	if (rc < 0) { return rc; }

	flockfile(out);
	ed_stat_print(stat, out);
	fprintf(out,
		"slab:\n"
		"  path: %s\n"
		"  inode: %llu\n"
		"  entries: %zu\n"
		"  bytes:\n"
		"    used: %zu\n"
		"    wasted: %zu\n"
		"  blocks:\n"
		"    used: %zu\n"
		"    size: %zu\n"
		"    count: %zu\n"
		"    cursor: %zu\n",
		cache->idx.hdr->slab_path,
		cache->idx.hdr->slab_ino,
		(size_t)0,
		(size_t)cache->bytes_used,
		(size_t)0,
		(size_t)cache->blocks_used,
		(size_t)cache->idx.hdr->slab_block_size,
		(size_t)cache->idx.hdr->slab_block_count,
		(size_t)cache->idx.hdr->pos
	);

	funlockfile(out);
	ed_stat_free(&stat);
	return 0;
}

int
ed_open(EdCache *cache, EdObject **objp, const void *k, size_t klen)
{
	EdObject *obj = NULL;
	int rc = obj_new(&obj, NULL, 0, true);
	if (rc < 0) { return rc; }
	assert(obj != NULL);

	const uint64_t h = ed_hash(k, klen, cache->idx.seed);
	const EdTimeUnix now = ed_now_unix();
	EdTxn *const txn = cache->txn;

	int set = 0;
	EdEntryKey *key;

	rc = ed_txn_open(txn, cache->idx.flags|ED_FRDONLY);
	if (rc < 0) { goto done; }

	for (rc = ed_bpt_find(txn, ED_DB_KEYS, h, (void **)&key);
			rc == 1 && ed_bpt_loop(txn, ED_DB_KEYS) == 0;
			rc = ed_bpt_next(txn, ED_DB_KEYS, (void **)&key)) {
		// First check if the object is expired.
		if (ed_expired_at(cache->idx.epoch, key->exp, now)) {
			continue;
		}

		off_t off = key->no * PAGESIZE;
		off_t len = key->count * PAGESIZE;

		// Try to get a shared lock on the slab region. If it cannot be locked, a
		// writer is replacing this slab location.
		if (ed_flck(cache->idx.slabfd, ED_LCK_SH, off, len, cache->idx.flags|ED_FNOBLOCK) < 0) {
			continue;
		}

		// Map the slab object.
		EdObjectHdr *hdr = ed_pg_map(cache->idx.slabfd, key->no, key->count, false);
		if (hdr == MAP_FAILED) {
			rc = ED_ERRNO;
			ed_flck(cache->idx.slabfd, ED_LCK_UN, off, len, cache->idx.flags);
			break;
		}

		// Resolve any hash collisions with a full key comparison. This will *very*
		// likely match. If it does, set up the object and end the loop.
		if (hdr->keylen == klen && memcmp(obj_key(hdr), k, klen) == 0) {
			obj_init(obj, cache, hdr, key->no, true, key->exp);
			set = 1;
			break;
		}

		// We have a hash collision so unlock and unmap the slab region and continue
		// searching with the next entry.
		ed_flck(cache->idx.slabfd, ED_LCK_UN, off, len, cache->idx.flags);
		ed_pg_unmap(hdr, key->count);
	}

done:
	ed_txn_close(&cache->txn, cache->idx.flags|ED_FRESET);
	if (rc == 1) {
		madvise(obj->hdr, obj->nbytes, MADV_SEQUENTIAL);
	}
	else {
		free(obj);
		obj = NULL;
	}
	*objp = obj;
	return rc < 0 ? rc : set;
}

int
ed_create(EdCache *cache, EdObject **objp, const EdObjectAttr *attr)
{
	EdObject *obj = NULL;
	int rc = obj_new(&obj, attr->key, attr->keylen, false);
	if (rc < 0) { return rc; }
	assert(obj != NULL);

	const EdTimeUnix unow = ed_now_unix();
	const EdTime now = ed_time_from_unix(cache->idx.epoch, unow);
	const EdTime exp = ed_expiry_at(cache->idx.epoch, attr->ttl, unow);
	const uint64_t h = ed_hash(attr->key, attr->keylen, cache->idx.seed);
	const uint64_t flags = cache->idx.flags;
	const size_t nbytes = obj_slab_size(attr->keylen, attr->metalen, attr->datalen, flags);
	const EdBlkno nblcks = nbytes/PAGESIZE;
	const int slabfd = cache->idx.slabfd;
	EdTxn *const txn = cache->txn;

	EdObjectHdr *hdr = MAP_FAILED;
	bool locked = false;
	EdBlkno blck;

	// Open a transaction. This allows to get the current slab position safely.
	// If this fails, return the error code, but any furtur failures must goto
	// the done label.
	rc = ed_txn_open(txn, flags);
	if (rc < 0) { goto done; }

	blck = ed_txn_block(txn);
	rc = obj_reserve(txn, slabfd, flags, &blck, nbytes);
	if (rc < 0) { goto done; }

	// Map the new object header in the slab.
	hdr = ed_pg_map(slabfd, blck, nblcks, true);
	if (hdr == MAP_FAILED) {
		rc = ED_ERRNO;
		goto done;
	}

	// Add the next write position to the transaction.
	ed_txn_set_block(txn, blck + nblcks);

	// Commit changes and initialize the new header.
	rc = ed_txn_commit(&cache->txn, flags|ED_FRESET);
	if (rc < 0) { goto done; }

	obj_hdr_init(hdr, attr, h, nbytes, flags, now);
	obj_init(obj, cache, hdr, blck, false, exp);

done:
	// Clean up resources if there was an error.
	if (rc < 0) {
		if (hdr != MAP_FAILED) {
			ed_pg_unmap(hdr, nblcks);
		}
		if (locked) {
			ed_flck(slabfd, ED_LCK_UN, blck*PAGESIZE, nbytes, flags);
		}
		if (ed_txn_isopen(txn)) {
			ed_txn_close(&cache->txn, flags|ED_FRESET);
		}
		free(obj);
		obj = NULL;
	}
	*objp = obj;
	return rc;
}

int64_t
ed_write(EdObject *obj, const void *buf, size_t len)
{
	if (obj->rdonly) {
		return ED_EOBJECT_RDONLY;
	}
	if (len > UINT32_MAX || (uint64_t)obj->dataseek + len > (uint64_t)obj->datalen) {
		return ED_EOBJECT_TOOBIG;
	}
	const uint64_t flags = obj->cache->idx.flags;
	obj_write(obj->data + obj->dataseek, buf, len, &obj->datacrc, flags);
	obj->dataseek += (uint32_t)len;
	if (obj->datalen == obj->dataseek) {
		obj->hdr->datacrc = obj->datacrc;
		obj_hdr_final(obj->hdr, obj->nbytes, flags);
	}
	return (int64_t)len;
}

const void *
ed_value(EdObject *obj, size_t *len)
{
	*len = obj->datalen;
	return obj->data;
}

const void *
ed_meta(EdObject *obj, size_t *len)
{
	*len = obj->metalen;
	return obj->meta;
}

int
ed_close(EdObject **objp)
{
	EdObject *obj = *objp;
	if (obj == NULL) { return 0; }
	*objp = NULL;

	EdCache *cache = obj->cache;
	uint64_t flags = cache->idx.flags;
	int slabfd = cache->idx.slabfd;
	int rc = 0;
	uint64_t h = obj->hdr->keyhash;
	bool locked = true;

	ed_pg_unmap(obj->hdr, obj->nblcks);
	if (!(flags & ED_FNOSYNC)) {
		fsync(slabfd);
	}

	if (!obj->rdonly) {
		if (obj->datalen == obj->dataseek) {
			rc = ed_txn_open(cache->txn, flags);
			if (rc < 0) { goto done; }

			rc = obj_upsert(cache, obj->newkey, obj->keylen, h,
					obj->blck, obj->nblcks, obj->exp);
			if (rc < 0) { goto done; }

			ed_flck(slabfd, ED_LCK_UN, obj->byte, obj->nbytes, flags);
			locked = false;

			rc = ed_txn_commit(&cache->txn, flags|ED_FRESET);
		}
		else {
			rc = ED_EOBJECT_TOOSMALL;
		}
	}

done:
	if (locked) {
		ed_flck(slabfd, ED_LCK_UN, obj->byte, obj->nbytes, flags);
	}
	if (rc < 0 && ed_txn_isopen(cache->txn)) {
		ed_txn_close(&cache->txn, flags|ED_FRESET);
	}
	free(obj);
	return rc;
}

void
ed_discard(EdObject **objp)
{
	EdObject *obj = *objp;
	if (obj == NULL) { return; }
	*objp = NULL;

	EdCache *cache = obj->cache;
	ed_pg_unmap(obj->hdr, obj->nblcks);
	ed_flck(cache->idx.slabfd, ED_LCK_UN, obj->byte, obj->nbytes, cache->idx.flags);
	free(obj);
}

int
ed_set_ttl(EdObject *obj, time_t ttl)
{
	if (obj->rdonly) {
		return ED_EOBJECT_RDONLY;
	}
	obj->exp = ed_expiry_at(obj->cache->idx.epoch, ttl, ed_now_unix());
	return 0;
}

int
ed_set_tag(EdObject *obj, uint16_t tag)
{
	if (obj->rdonly) {
		return ED_EOBJECT_RDONLY;
	}
	obj->hdr->tag = tag;
	return 0;
}

EdTimeTTL
ed_ttl(const EdObject *obj, EdTimeUnix from)
{
	if (from < 0) { from = ed_now_unix(); }
	return ed_ttl_at(obj->cache->idx.epoch, obj->exp, from);
}

EdTimeUnix
ed_expiry(const EdObject *obj)
{
	return ed_time_to_unix(obj->cache->idx.epoch, obj->exp);
}

EdTimeUnix
ed_created_at(const EdObject *obj)
{
	return ed_time_to_unix(obj->cache->idx.epoch, obj->hdr->created);
}

uint16_t
ed_tag(const EdObject *obj)
{
	return obj->hdr->tag;
}

