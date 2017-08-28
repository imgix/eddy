#include "eddy-private.h"

int
ed_cache_open(EdCache **cachep, const EdConfig *cfg)
{
	EdCache *cache = malloc(sizeof(*cache));
	if (cache == NULL) { return ED_ERRNO; }

	int rc = ed_idx_open(&cache->idx, cfg, &cache->fd);
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
			close(cache->fd);
			free(cache);
		}
	}
}

int
ed_cache_stat(EdCache *cache, FILE *out, int flags)
{
	if (out == NULL) { out = stdout; }

	int rc = ed_idx_stat(&cache->idx, out, flags);
	if (rc < 0) { return rc; }
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
	return 0;
}

int
ed_open(EdCache *cache, EdObject **objp, const void *key, size_t len)
{
	(void)cache;
	(void)key;
	(void)len;
	*objp = NULL;
	return ed_esys(ENOTSUP);
}

int
ed_create(EdCache *cache, EdObject **objp, EdObjectAttr *attr)
{
	(void)cache;
	(void)attr;
	*objp = NULL;
	return ed_esys(ENOTSUP);
}

