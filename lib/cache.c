#include "eddy-private.h"

static int
obj_new(EdObject **objp, const void *k, size_t klen, bool rdonly)
{
	EdObject *obj = calloc(1, sizeof(*obj) + klen);
	if (obj == NULL) { return ED_ERRNO; }
	obj->rdonly = rdonly;
	memcpy(obj->newkey, k, klen);
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
obj_data_offset(uint16_t keylen, uint16_t metalen)
{
	return ed_align_pg(obj_meta_offset(keylen) + metalen);
}

static size_t
obj_slab_size(uint16_t keylen, uint16_t metalen, uint32_t datalen)
{
	return ed_align_pg(obj_data_offset(keylen, metalen) + datalen);
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
obj_data(EdObjectHdr *hdr)
{
	return (uint8_t *)hdr + obj_data_offset(hdr->keylen, hdr->metalen);
}

static void
obj_init(EdObject *obj, EdCache *cache, EdObjectHdr *hdr, EdBlkno no, bool rdonly)
{
	size_t size = obj_slab_size(hdr->keylen, hdr->metalen, hdr->datalen);
	obj->cache = cache;
	obj->key = obj_key(hdr);
	obj->keylen = hdr->keylen;
	obj->meta = obj_meta(hdr);
	obj->metalen = hdr->metalen;
	obj->data = obj_data(hdr);
	obj->datalen = hdr->datalen;
	obj->dataseek = 0;
	obj->datacrc = hdr->datacrc;
	obj->hdr = hdr;
	obj->blck = no;
	obj->nblcks = size/PAGESIZE;
	obj->byte = no*PAGESIZE;
	obj->nbytes = size;
	obj->rdonly = rdonly;
}

static void
obj_hdr_init(EdObjectHdr *hdr, EdObjectAttr *attr, uint64_t h, size_t nbytes, uint64_t flags)
{
	hdr->keylen = attr->keylen;
	hdr->metalen = attr->metalen;
	hdr->datalen = attr->datalen;
	hdr->keyhash = h;
	hdr->metacrc = 0;
	hdr->datacrc = 0;
	memcpy(obj_key(hdr), attr->key, attr->keylen);
	if (flags & ED_FZERO) {
		memset(obj_key(hdr) + attr->keylen, 0,
				nbytes - obj_key_offset() - attr->keylen);
	}
	else {
		obj_key(hdr)[attr->keylen] = '\0';
	}
}

static int
obj_get(EdCache *cache, const void *k, size_t klen, EdObject *obj)
{
	EdTxn *txn = cache->txn;
	uint64_t h = ed_hash(k, klen, cache->idx.seed);
	EdTimeUnix now = ed_now_unix();

	int rc = ed_txn_open(txn, cache->idx.flags|ED_FRDONLY);
	if (rc < 0) { return rc; }

	int set = 0;
	EdEntryKey *key;
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
			obj->expiry = ed_expiry_at(cache->idx.epoch, key->exp, now);
			obj_init(obj, cache, hdr, key->no, true);
			set = 1;
			break;
		}

		// We have a hash collision so unlock and unmap the slab region and continue
		// searching with the next entry.
		ed_flck(cache->idx.slabfd, ED_LCK_UN, off, len, cache->idx.flags);
		ed_pg_unmap(hdr, key->count);
	}
	ed_txn_close(&cache->txn, cache->idx.flags|ED_FRESET);
	if (rc == 1) {
		madvise(obj->hdr, obj->nbytes, MADV_SEQUENTIAL);
	}
	return rc < 0 ? rc : set;
}

static int
obj_reserve_slab(EdTxn *txn, int slabfd, uint64_t flags, EdBlkno *posp, size_t len)
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
obj_upsert(EdCache *cache, EdObjectAttr *attr, uint64_t h,
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

		replace = old->keylen == attr->keylen &&
			memcmp(obj_key(old), attr->key, attr->keylen) == 0;
		ed_pg_unmap(old, 1);
		if (replace) { break; }
	}
	if (rc >= 0) {
		rc = ed_bpt_set(txn, ED_DB_KEYS, (void *)&keynew, replace);
	}
	return rc;
}

/**
 * @brief  Creates an exclusive writable gap in the slab.
 * @param  cache  Cache pointer
 * @param  obj   Object pointer to assign slab information
 * @return  0 on success <0 on error
 */
static int
obj_reserve(EdCache *cache, EdObjectAttr *attr, EdTimeUnix expiry, EdObject *obj)
{
	uint64_t h = ed_hash(attr->key, attr->keylen, cache->idx.seed);
	bool locked = false;
	EdTxn *txn = cache->txn;
	int rc = 0;
	int slabfd = cache->idx.slabfd;
	uint64_t flags = cache->idx.flags;

	EdObjectHdr *hdr = MAP_FAILED;

	size_t nbytes = obj_slab_size(attr->keylen, attr->metalen, attr->datalen);
	EdBlkno blck, nblcks = nbytes/PAGESIZE;
	EdTime exp = ed_time_from_unix(cache->idx.epoch, expiry);

	// Open a transaction. This allows to get the current slab position safely.
	// If this fails, return the error code, but any furtur failures must goto
	// the done label.
	rc = ed_txn_open(txn, flags);
	if (rc < 0) { return rc; }

	blck = ed_txn_block(txn);
	rc = obj_reserve_slab(txn, slabfd, flags, &blck, nbytes);
	if (rc < 0) { goto done; }

	// Map the new object header in the slab.
	hdr = ed_pg_map(slabfd, blck, nblcks, true);
	if (hdr == MAP_FAILED) {
		rc = ED_ERRNO;
		goto done;
	}

	// Add the next write position to the transaction.
	ed_txn_set_block(txn, blck + nblcks);

	// Make object available in the index.
	rc = obj_upsert(cache, attr, h, blck, nblcks, exp);
	if (rc < 0) { goto done; }

	// Commit changes and initialize the new header.
	rc = ed_txn_commit(&cache->txn, flags|ED_FRESET);

	if (rc >= 0) {
		obj_hdr_init(hdr, attr, h, nbytes, flags);
		obj_init(obj, cache, hdr, blck, false);
	}

done:
	// Clean up resources if there was an error.
	if (rc < 0) {
		if (hdr != MAP_FAILED) {
			ed_pg_unmap(hdr, nblcks);
		}
		if (locked) {
			ed_flck(cache->idx.slabfd, ED_LCK_UN, blck*PAGESIZE, nbytes, cache->idx.flags);
		}
		ed_txn_close(&cache->txn, cache->idx.flags|ED_FRESET);
	}
	else {
		madvise(hdr, nbytes, MADV_SEQUENTIAL);
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
ed_open(EdCache *cache, EdObject **objp, const void *key, size_t len)
{
	EdObject *obj;
	int rc = obj_new(&obj, NULL, 0, true);
	if (rc < 0) { return rc; }

	rc = obj_get(cache, key, len, obj);
	if (rc <= 0) {
		free(obj);
		obj = NULL;
	}
	*objp = obj;
	return rc;
}

int
ed_create(EdCache *cache, EdObject **objp, EdObjectAttr *attr)
{
	// TODO: this needs to be a three-stage write.
	// 1) find writable location and kick out entries there
	// 2) write contents to slab
	// 3) on close upsert the key and slab position
	EdObject *obj;
	int rc = obj_new(&obj, attr->key, attr->keylen, false);
	if (rc < 0) { return rc; }

	rc = obj_reserve(cache, attr, ed_unix_from_ttl(attr->ttl), obj);
	if (rc < 0) {
		free(obj);
		return rc;
	}

	if (attr->meta != NULL) {
		if (cache->idx.flags & ED_FCHECKSUM) {
			obj->hdr->metacrc = ed_crc32c(0, attr->meta, obj->metalen);
		}
		memcpy(obj->meta, attr->meta, obj->metalen);
	}

	*objp = obj;
	return rc;
}

int64_t
ed_write(EdObject *obj, const void *buf, size_t len)
{
	if (len > UINT32_MAX || (uint64_t)obj->dataseek + len > (uint64_t)obj->datalen) {
		return ED_EOBJECT_LENGTH;
	}
	if (obj->cache->idx.flags & ED_FCHECKSUM) {
		ed_crc32c(obj->datacrc, buf, len);
	}
	memcpy(obj->data + obj->dataseek, buf, len);
	obj->dataseek += (uint32_t)len;
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

void
ed_close(EdObject **objp)
{
	EdObject *obj = *objp;
	if (obj == NULL) { return; }
	*objp = NULL;

	ed_flck(obj->cache->idx.slabfd, ED_LCK_UN,
			obj->byte, obj->nbytes, obj->cache->idx.flags);
	ed_pg_unmap(obj->hdr, obj->nblcks);
	free(obj);
}

