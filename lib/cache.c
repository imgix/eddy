#include "eddy-private.h"

int
ed_cache_open(EdCache **cachep, const EdConfig *cfg)
{
	EdCache *cache = malloc(sizeof(*cache));
	if (cache == NULL) { return ED_ERRNO; }

	int rc = ed_idx_open(&cache->idx, cfg);
	if (rc < 0) {
		free(cache);
		return rc;
	}

	cache->ref = 1;
	cache->bytes_used = 0;
	cache->blocks_used = 0;
	*cachep = cache;
	return 0;
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
	EdObject *obj = calloc(1, sizeof(*obj));
	if (obj == NULL) { return ED_ERRNO; }

	obj->cache = cache;
	obj->key = (uint8_t *)key;
	obj->keylen = len;

	int rc = ed_idx_get(&cache->idx, obj, false);
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
	EdObject *obj = calloc(1, sizeof(*obj));
	if (obj == NULL) { return ED_ERRNO; }

	obj->cache = cache;
	obj->key = (uint8_t *)attr->key;
	obj->keylen = attr->key_size;
	obj->metalen= attr->meta_size;
	obj->datalen = attr->object_size;
	obj->expiry = ed_unix_from_ttl(attr->ttl);

	int rc = ed_idx_reserve(&cache->idx, obj);
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
	if (len > UINT32_MAX || (uint64_t)obj->datacur + len > (uint64_t)obj->datalen) {
		return ED_EOBJECT_LENGTH;
	}
	if (obj->cache->idx.flags & ED_FCHECKSUM) {
		ed_crc32c(obj->crc, buf, len);
	}
	memcpy(obj->data + obj->datacur, buf, len);
	obj->datacur += (uint32_t)len;
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
	objp = NULL;

	ed_flck(obj->cache->idx.slabfd, ED_LCK_UN,
			obj->no, obj->count * PAGESIZE, obj->cache->idx.flags);
	ed_pg_unmap(obj->hdr, obj->count);
	free(obj);

}

