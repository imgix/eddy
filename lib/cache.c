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
obj_slab_size(uint16_t keylen, uint16_t metalen, uint32_t datalen, uint16_t block_size, uint64_t flags)
{
	return ED_ALIGN_SIZE(obj_data_offset(keylen, metalen, flags) + datalen, block_size);
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
obj_init_basic(EdObject *obj, EdCache *cache, EdObjectHdr *hdr, EdBlkno vno, bool rdonly, EdTime exp)
{
	const uint16_t block_size = cache->slab_block_size;
	size_t size = obj_slab_size(hdr->keylen, hdr->metalen, hdr->datalen, block_size,
			cache->idx.flags);
	obj->cache = cache;
	obj->key = obj_key(hdr);
	obj->keylen = hdr->keylen;
	obj->metalen = hdr->metalen;
	obj->metacrc = hdr->metacrc;
	obj->datalen = hdr->datalen;
	obj->datacrc = hdr->datacrc;
	obj->hdr = hdr;
	obj->xid = hdr->xid;
	obj->vno = vno;
	obj->nblcks = size/block_size;
	obj->byte = (vno % cache->slab_block_count) * block_size;
	obj->nbytes = size;
	obj->exp = exp;
	obj->rdonly = rdonly;
	snprintf(obj->id, sizeof(obj->id), "%" PRIx64 ":%" PRIx64, obj->xid, vno);
}

static void
obj_init(EdObject *obj, EdCache *cache, EdObjectHdr *hdr, EdBlkno vno, bool rdonly, EdTime exp)
{
	obj_init_basic(obj, cache, hdr, vno, rdonly, exp);
	obj->meta = obj_meta(hdr);
	obj->data = obj_data(hdr, cache->idx.flags);
}

static int
obj_verify(const EdObject *obj, const uint64_t flags)
{
	if ((flags & ED_FCHECKSUM) && !(flags & ED_FNOVERIFY)) {
		if (obj->metalen && ed_crc32c(0, obj->meta, obj->metalen) != obj->metacrc) {
			return ED_EOBJECT_METACRC;
		}
		if (obj->datalen && ed_crc32c(0, obj->data, obj->datalen) != obj->datacrc) {
			return ED_EOBJECT_DATACRC;
		}
	}
	return 0;
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
obj_hdr_final(EdObjectHdr *hdr, size_t nbytes, uint64_t flags)
{
	uint8_t *data = obj_data(hdr, flags) + hdr->datalen;
	memset(data, 0, nbytes - (data - (uint8_t *)hdr));
}

static bool
obj_overlap(const EdEntryBlock *block, EdBlkno start, EdBlkno end)
{
	return block->no < end && start < block->no + block->count;
}

static int
obj_reserve(EdCache *cache, EdTxn *txn, uint64_t flags, EdBlkno *vnop, size_t len)
{
	const int slabfd = cache->idx.slabfd;
	const uint16_t block_size = cache->slab_block_size;
	const EdBlkno block_count = cache->slab_block_count;
	const EdBlkno nmin = ED_ALIGN_SIZE(sizeof(EdObjectHdr) + ED_MAX_KEY + 1, block_size);
	EdBlkno vno = *vnop, no = vno % block_count, end;
	size_t start = no * block_size;
	bool searched = false;
	bool locked = false;
	EdEntryBlock *block;
	int rc;

	// Find then next unlocked region >= #vno. If the current #vno cannot be used,
	// start from the beginning of the next entry.
	do {
		// If the range sticks out past the end of the file, search at the beginning.
		// I was creating a wrapped mmap here previously. For simplicity, that has
		// been removed, but it could come back without any file format changes.
		if (start + len > block_count*block_size) {
			start = 0;
			vno += block_count - no;
			no = vno % block_count;
			searched = false;
		}

		if (!searched) {
			// Lookup an entry at the current slab position. Its likely nothing will be
			// matched which will leave the block NULL. We must be sure the check this
			// case when removing replaced entries.
			rc = ed_bpt_find(txn, ED_DB_BLOCKS, no, (void **)&block);
			if (rc < 0) { goto done; }
			searched = true;
		}

		if (ed_flck(slabfd, ED_LCK_EX, start, len, flags|ED_FNOBLOCK) < 0) {
			rc = ed_bpt_next(txn, ED_DB_BLOCKS, (void **)&block);
			if (rc < 0) { goto done; }
			vno += block->count;
			no = block->no;
			start = no * block_size;
		}
		else {
			locked = true;
			break;
		}
	} while(1);

	// Determine the first page number after the write region.
	end = no + len/block_size;

	// If the original find didn't match and we never iterated to the next
	// position, load the next entry.
	if (block == NULL) {
		rc = ed_bpt_next(txn, ED_DB_BLOCKS, (void **)&block);
		if (rc < 0) { goto done; }
	}

	// Loop through objects by block position and remove the key and then block
	// entries in the index.
	while (block && obj_overlap(block, no, end)) {
		// Only the first page of the object is needed.
		EdObjectHdr *old = ed_blk_map(slabfd, block->no, nmin, block_size, true);
		if (old == MAP_FAILED) { rc = ED_ERRNO; goto done; }

		// Loop through each key entry to resolve collisions. Key comparison is not
		// rquireds for this resolution. We are looking for the key that maps to
		// current block number.
		EdEntryKey *key;
		for (rc = ed_bpt_find(txn, ED_DB_KEYS, old->keyhash, (void **)&key);
				rc == 1 && ed_bpt_loop(txn, ED_DB_KEYS) == 0;
				rc = ed_bpt_next(txn, ED_DB_KEYS, (void **)&key)) {
			if ((key->vno % block_count) == block->no) {
				rc = ed_bpt_del(txn, ED_DB_KEYS);
				if (rc >= 0) {
					rc = ed_bpt_next(txn, ED_DB_KEYS, (void **)&key);
				}
				break;
			}
		}
		ed_blk_unmap(old, nmin, block_size);
		if (rc < 0) { goto done; }

		rc = ed_bpt_del(txn, ED_DB_BLOCKS);
		if (rc < 0) { goto done; }

		rc = ed_bpt_next(txn, ED_DB_BLOCKS, (void **)&block);
		if (rc < 0) { goto done; }
	}

	*vnop = vno;

done:
	if (rc < 0 && locked) {
		ed_flck(slabfd, ED_LCK_UN, start, len, flags);
	}
	return rc;
}

static int
obj_upsert(EdCache *cache, const void *k, size_t klen, uint64_t h,
		EdBlkno vno, EdBlkno nblcks, EdTime exp)
{
	EdTxn *txn = cache->txn;
	const uint16_t block_size = cache->slab_block_size;
	const EdBlkno block_count = cache->slab_block_count;
	const EdBlkno nmin = ED_ALIGN_SIZE(sizeof(EdObjectHdr) + ED_MAX_KEY + 1, block_size);
	EdEntryBlock blocknew = ed_entry_block_make(vno, nblcks, block_count, txn->xid);
	EdEntryKey *key, keynew = ed_entry_key_make(h, vno, nblcks, exp);
	bool replace = false;
	int rc;

	// Insert the slab position into the db.
	if ((rc = ed_bpt_find(txn, ED_DB_BLOCKS, blocknew.no, NULL)) < 0 || 
		(rc = ed_bpt_set(txn, ED_DB_BLOCKS, (void *)&blocknew, true) < 0)) {
		return rc;
	}

	// Insert the key into the db.
	for (rc = ed_bpt_find(txn, ED_DB_KEYS, h, (void **)&key);
			rc == 1 && ed_bpt_loop(txn, ED_DB_KEYS) == 0;
			rc = ed_bpt_next(txn, ED_DB_KEYS, (void **)&key)) {
		// Map the slab object.
		EdObjectHdr *old = ed_blk_map(cache->idx.slabfd, key->vno % block_count, nmin, block_size, true);
		if (old == MAP_FAILED) { return ED_ERRNO; }

		replace = old->keylen == klen && memcmp(obj_key(old), k, klen) == 0;
		if (replace && !(cache->idx.flags & ED_FKEEPOLD)) {
			old->exp = ED_TIME_DELETE;
		}
		ed_blk_unmap(old, nmin, block_size);
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
	cache->slab_block_count = cache->idx.hdr->slab_block_count;
	cache->slab_block_size = cache->idx.hdr->slab_block_size;
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
		__sync_fetch_and_add(&cache->ref, 1);
	}
	return cache;
}

void
ed_cache_close(EdCache **cachep)
{
	EdCache *cache = *cachep;
	if (cache != NULL) {
		*cachep = NULL;
		if (__sync_fetch_and_sub(&cache->ref, 1) == 1) {
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
		"  inode: %" PRIu64 "\n"
		"  blocks:\n"
		"    size: %zu\n"
		"    count: %zu\n"
		"    cursor: %zu\n"
		"    current: %zu\n"
		,
		cache->idx.hdr->slab_path,
		cache->idx.hdr->slab_ino,
		(size_t)cache->slab_block_size,
		(size_t)cache->slab_block_count,
		(size_t)cache->idx.hdr->vno,
		(size_t)(cache->idx.hdr->vno % cache->slab_block_count)
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
	const uint16_t block_size = cache->slab_block_size;
	const EdBlkno block_count = cache->slab_block_count;
	const uint64_t flags = cache->idx.flags;
	const EdTimeUnix now = ed_now_unix();
	EdTxn *const txn = cache->txn;

	int set = 0;
	EdEntryKey *key;

	rc = ed_txn_open(txn, flags|ED_FRDONLY);
	if (rc < 0) { goto done; }

	for (rc = ed_bpt_find(txn, ED_DB_KEYS, h, (void **)&key);
			rc == 1 && ed_bpt_loop(txn, ED_DB_KEYS) == 0;
			rc = ed_bpt_next(txn, ED_DB_KEYS, (void **)&key)) {
		// First check if the object is expired.
		if (ed_expired_at(cache->idx.epoch, key->exp, now)) {
			continue;
		}

		off_t off = (key->vno % block_count) * block_size;
		off_t len = key->count * block_size;

		// Try to get a shared lock on the slab region. If it cannot be locked, a
		// writer is replacing this slab location.
		if (ed_flck(cache->idx.slabfd, ED_LCK_SH, off, len, flags|ED_FNOBLOCK) < 0) {
			continue;
		}

		// Map the slab object.
		EdObjectHdr *hdr = ed_blk_map(cache->idx.slabfd, key->vno % block_count, key->count, block_size, false);
		if (hdr == MAP_FAILED) {
			rc = ED_ERRNO;
			ed_flck(cache->idx.slabfd, ED_LCK_UN, off, len, flags);
			break;
		}

		// Resolve any hash collisions with a full key comparison. This will *very*
		// likely match. If it does, set up the object and end the loop.
		if (hdr->keylen == klen && memcmp(obj_key(hdr), k, klen) == 0) {
			set = 1;
			obj_init(obj, cache, hdr, key->vno, true, key->exp);
			break;
		}

		// We have a hash collision so unlock and unmap the slab region and continue
		// searching with the next entry.
		ed_flck(cache->idx.slabfd, ED_LCK_UN, off, len, flags);
		ed_blk_unmap(hdr, key->count, block_size);
	}

done:
	ed_txn_close(&cache->txn, flags|ED_FRESET);
	if (set == 1) {
		madvise(obj->hdr, obj->nbytes, MADV_SEQUENTIAL);
		int vrc = obj_verify(obj, flags);
		if (vrc < 0) { rc = vrc; }
	}
	if (rc < 0) {
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
	const uint64_t h = ed_hash(attr->key, attr->keylen, cache->idx.seed);
	const uint64_t flags = cache->idx.flags;
	const uint16_t block_size = cache->slab_block_size;
	const EdBlkno block_count = cache->slab_block_count;
	const size_t nbytes = obj_slab_size(attr->keylen, attr->metalen, attr->datalen, block_size, flags);
	const EdBlkno nblcks = nbytes/block_size;
	const int slabfd = cache->idx.slabfd;
	EdTxn *const txn = cache->txn;

	EdObjectHdr *hdr = MAP_FAILED;
	bool locked = false;
	EdBlkno vno;

	// Open a transaction. This allows to get the current slab position safely.
	// If this fails, return the error code, but any furtur failures must goto
	// the done label.
	rc = ed_txn_open(txn, flags);
	if (rc < 0) { goto done; }

	vno = ed_txn_vno(txn);
	rc = obj_reserve(cache, txn, flags, &vno, nbytes);
	if (rc < 0) { goto done; }

	// Map the new object header in the slab.
	hdr = ed_blk_map(slabfd, vno % block_count, nblcks, block_size, true);
	if (hdr == MAP_FAILED) {
		rc = ED_ERRNO;
		goto done;
	}

	// Add the next write position to the transaction.
	ed_txn_set_vno(txn, vno + nblcks);

	// Commit changes and initialize the new header.
	rc = ed_txn_commit(&cache->txn, flags|ED_FRESET);
	if (rc < 0) { goto done; }

	// Initializse the object header.
	madvise(hdr, nbytes, MADV_SEQUENTIAL);
	hdr->xid = 0;
	hdr->created = now;
	hdr->exp = 0;
	hdr->flags = 0;
	hdr->keylen = attr->keylen;
	hdr->metalen = attr->metalen;
	hdr->datalen = attr->datalen;
	hdr->keyhash = h;
	hdr->metacrc = 0;
	hdr->datacrc = 0;

	// Copy the key into the tail of the header segment.
	uint8_t *key = obj_key(hdr), *meta = obj_meta(hdr);
	memcpy(key, attr->key, attr->keylen);
	key += attr->keylen;

	// Zero out the end of the key segment.
	memset(key, 0, obj_meta(hdr) - key);

	// Write the meta data segment if provided.
	if (attr->meta != NULL && attr->metalen > 0) {
		obj_write(meta, attr->meta, attr->metalen, &hdr->metacrc, flags);
	}
	meta += attr->metalen;

	// Zero out the end of the meta segment.
	memset(meta, 0, obj_data(hdr, flags) - meta);

	obj_init(obj, cache, hdr, vno, false, ED_TIME_INF);

done:
	// Clean up resources if there was an error.
	if (rc < 0) {
		if (hdr != MAP_FAILED) {
			ed_blk_unmap(hdr, nblcks, block_size);
		}
		if (locked) {
			ed_flck(slabfd, ED_LCK_UN, (vno % block_count) * block_size, nbytes, flags);
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

static int
update_expiry(EdCache *cache, const void *k, size_t klen, EdTime exp, EdTimeUnix now, bool restore)
{
	const uint16_t block_size = cache->slab_block_size;
	const EdBlkno block_count = cache->slab_block_count;
	const uint64_t h = ed_hash(k, klen, cache->idx.seed);
	EdTxn *const txn = cache->txn;

	int rc = 0, set = 0;
	EdEntryKey *key;

	rc = ed_txn_open(txn, cache->idx.flags);
	if (rc < 0) { return rc; }

	for (rc = ed_bpt_find(txn, ED_DB_KEYS, h, (void **)&key);
			rc == 1 && ed_bpt_loop(txn, ED_DB_KEYS) == 0;
			rc = ed_bpt_next(txn, ED_DB_KEYS, (void **)&key)) {
		// First check if the object is expired.
		if (!restore && ed_expired_at(cache->idx.epoch, key->exp, now)) {
			continue;
		}

		// Map the slab object.
		EdObjectHdr *hdr = ed_blk_map(cache->idx.slabfd, key->vno % block_count, 1, block_size, true);
		if (hdr == MAP_FAILED) {
			rc = ED_ERRNO;
			break;
		}

		// Resolve any hash collisions with a full key comparison. This will *very*
		// likely match. If it does, set up the object and end the loop.
		if (hdr->keylen == klen && memcmp(obj_key(hdr), k, klen) == 0) {
			EdEntryKey keynew = *key;
			keynew.exp = exp;
			rc = ed_bpt_set(txn, ED_DB_KEYS, (void *)&keynew, true);
			if (rc >= 0) {
				hdr->exp = exp;
				set = 1;
			}
		}

		ed_blk_unmap(hdr, key->count, block_size);

		if (set == 1) {
			break;
		}
	}

	if (set == 1) {
		ed_txn_commit(&cache->txn, cache->idx.flags|ED_FRESET);
	}
	else {
		ed_txn_close(&cache->txn, cache->idx.flags|ED_FRESET);
	}

	return rc < 0 ? rc : set;
}

int
ed_update_ttl(EdCache *cache, const void *k, size_t klen, EdTimeTTL ttl, bool restore)
{
	EdTimeUnix now = ed_now_unix();
	EdTime exp = ed_expiry_at(cache->idx.epoch, ttl, now);
	return update_expiry(cache, k, klen, exp, now, restore);
}

int
ed_update_expiry(EdCache *cache, const void *k, size_t klen, EdTimeUnix expiry, bool restore)
{
	EdTimeUnix now = ed_now_unix();
	EdTime exp = ed_time_from_unix(cache->idx.epoch, expiry);
	return update_expiry(cache, k, klen, exp, now, restore);
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

	if (!obj->rdonly) {
		if (obj->datalen == obj->dataseek) {
			rc = ed_txn_open(cache->txn, flags);
			if (rc < 0) { goto done; }

			rc = obj_upsert(cache, obj->newkey, obj->keylen, h,
					obj->vno, obj->nblcks, obj->exp);
			if (rc < 0) { goto done; }

			obj->hdr->exp = obj->exp;
			obj->hdr->xid = cache->txn->xid;
			ed_flck(slabfd, ED_LCK_UN, obj->byte, obj->nbytes, flags);
			locked = false;

			rc = ed_txn_commit(&cache->txn, flags|ED_FRESET);

			if (rc >= 0 && !(flags & ED_FNOSYNC)) {
				fsync(slabfd);
			}
		}
		else {
			rc = ED_EOBJECT_TOOSMALL;
		}
	}

done:
	ed_blk_unmap(obj->hdr, obj->nblcks, cache->slab_block_size);

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
	ed_blk_unmap(obj->hdr, obj->nblcks, cache->slab_block_size);
	ed_flck(cache->idx.slabfd, ED_LCK_UN, obj->byte, obj->nbytes, cache->idx.flags);
	free(obj);
}

int
ed_set_ttl(EdObject *obj, EdTimeTTL ttl)
{
	if (obj->rdonly) {
		return ED_EOBJECT_RDONLY;
	}
	obj->exp = ed_expiry_at(obj->cache->idx.epoch, ttl, ed_now_unix());
	return 0;
}

int
ed_set_expiry(EdObject *obj, EdTimeUnix expiry)
{
	if (obj->rdonly) {
		return ED_EOBJECT_RDONLY;
	}
	obj->exp = ed_time_from_unix(obj->cache->idx.epoch, expiry);
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

const char *
ed_id(const EdObject *obj)
{
	return obj->id;
}

int
ed_list_open(EdCache *cache, EdList **listp, const char *id)
{
	EdTxnId xmin = 0, xmax;
	EdBlkno vmin = 0, vmax;
	const EdBlkno block_count = cache->slab_block_count;

	// If an id is not provided, start from the oldest entry.
	if (id != NULL) {
		char *end;
		xmin = strtoul(id, &end, 16);
		if (*end != ':') { return ED_EOBJECT_ID; }
		vmin = strtoul(end+1, &end, 16);
		if (*end != '\0') { return ED_EOBJECT_ID; }
	}

	EdList *list = calloc(1, sizeof(*list));
	if (list == NULL) { return ED_ERRNO; }

	int rc = ed_txn_new(&list->txn, &cache->idx);
	if (rc < 0) { goto error; }

	rc = ed_txn_open(list->txn, cache->idx.flags|ED_FRDONLY);
	if (rc < 0) { goto error; }

	xmax = cache->idx.conn->xid;
	vmax = ed_txn_vno(list->txn);
	if (id == NULL) {
		xmin = 0;
		vmin = vmax > block_count ? vmax - block_count : 0;
		list->inc = true;
	}

	// Move to the next entry block position if needed.
	rc = ed_bpt_find(list->txn, ED_DB_BLOCKS, vmin % block_count, NULL);
	if (rc < 0) { goto error; }
	if (rc == 0) {
		EdEntryBlock *block;
		rc = ed_bpt_next(list->txn, ED_DB_BLOCKS, (void **)&block);
		if (rc < 0) { goto error; }
		vmin = block->no + vmin/block_count * block_count;
		xmin = block->xid;
		list->inc = true;
	}

	list->cache = cache;
	list->now = ed_now_unix();
	list->xmin = list->xcur = xmin;
	list->xmax = xmax;
	list->vmin = list->vcur = vmin;
	list->vmax = vmax;
	*listp = list;
	return 0;

error:
	ed_txn_close(&list->txn, cache->idx.flags);
	free(list);
	return rc;
}

static void
list_clear(EdList *list, const uint16_t block_size, const EdBlkno block_need)
{
	if (list->obj.hdr != NULL) {
		ed_blk_unmap(list->obj.hdr, block_need, block_size);
		memset(&list->obj, 0, sizeof(list->obj));
	}
}

int
ed_list_next(EdList *list, const EdObject **objp)
{
	// TODO: optimize unmap/map when its the same page

	int rc = 0;
	const EdCache *const cache = list->cache;
	const uint16_t block_size = cache->slab_block_size;
	const EdBlkno block_count = cache->slab_block_count;
	const EdBlkno block_need = ED_COUNT_SIZE(sizeof(EdObjectHdr) + ED_MAX_KEY, block_size);
	EdObjectHdr *hdr = MAP_FAILED;

	for (;;) {
		list_clear(list, block_size, block_need);

		const EdBlkno vcur = list->vcur;

		if (vcur >= list->vmax) {
			goto done;
		}

		const EdBlkno no = vcur % block_count;

		hdr = ed_blk_map(cache->idx.slabfd, no, block_need, block_size, true);
		if (hdr == MAP_FAILED) {
			rc = ED_ERRNO;
			goto done;
		}

		obj_init_basic(&list->obj, list->cache, hdr, vcur, true, hdr->exp);

		EdEntryBlock *block;
		rc = ed_bpt_next(list->txn, ED_DB_BLOCKS, (void **)&block);
		if (rc < 0) { goto done; }
		if (ed_bpt_loop(list->txn, ED_DB_BLOCKS) > 1) {
			rc = 0;
			goto done;
		}
		if (block->no < no) {
			list->vcur += block->no + (block_count - no);
		}
		else {
			list->vcur += block->no - no;
		}

		if (!list->inc) {
			list->inc = true;
			continue;
		}

		if (!ed_expired_at(cache->idx.epoch, hdr->exp, list->now)) {
			*objp = &list->obj;
			return 1;
		}
	}

done:
	*objp = NULL;
	ed_txn_close(&list->txn, list->cache->idx.flags);
	return rc;
}

void
ed_list_close(EdList **listp)
{
	EdList *list = *listp;
	if (list == NULL) { return; }
	*listp = NULL;

	const uint16_t block_size = list->cache->slab_block_size;
	const EdBlkno block_need = ED_COUNT_SIZE(sizeof(EdObjectHdr) + ED_MAX_KEY, block_size);
	list_clear(list, block_size, block_need);

	ed_txn_close(&list->txn, list->cache->idx.flags);
	free(list);
}

