#include "eddy-private.h"

int
ed_cache_open(EdCache **cachep, const EdConfig *cfg)
{
	EdCache *cache = malloc(sizeof(*cache));
	if (cache == NULL) { return ED_ERRNO; }

	int rc = ed_idx_open(&cache->index, cfg, &cache->fd);
	if (rc < 0) {
		free(cache);
		return rc;
	}

	cache->ref = 1;
	cache->bytes_used = 0;
	cache->pages_used = 0;
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
			ed_idx_close(&cache->index);
			close(cache->fd);
			free(cache);
		}
	}
}

int
ed_cache_stat(EdCache *cache, FILE *out, int flags)
{
	if (out == NULL) { out = stdout; }

	int rc = ed_idx_stat(&cache->index, out, flags);
	if (rc < 0) { return rc; }
	fprintf(out,
		"slab:\n"
		"  path: %s\n"
		"  inode: %llu\n"
		"  entries: %zu\n"
		"  bytes:\n"
		"    used: %zu\n"
		"    wasted: %zu\n"
		"  pages:\n"
		"    used: %zu\n"
		"    size: %zu\n"
		"    count: %zu\n"
		"    cursor: %zu\n",
		cache->index.hdr->slab_path,
		cache->index.hdr->slab_ino,
		(size_t)0,
		(size_t)cache->bytes_used,
		(size_t)0,
		(size_t)cache->pages_used,
		(size_t)PAGESIZE,
		(size_t)cache->index.hdr->slab_page_count,
		(size_t)cache->index.hdr->pos
	);
	return 0;
}

int
ed_open(EdCache *cache, EdObject **objp, const void *key, size_t len)
{
	(void)cache;
	(void)key;
	(void)len;
#if 0
	uint64_t h = ed_hash(key, len, cache->index.seed);
	EdBSearch srch;
	EdNodeKey *k;
	int rc;
	printf("get: key=%.*s, hash=%llu\n", (int)len, key, h);

	rc = ed_idx_lock(&cache->index, ED_LCK_EX, true);
	if (rc < 0) { return 0; }
	
	rc = ed_idx_load_trees(&cache->index);
	if (rc < 0) { goto unlock; }

	rc = ed_btree_search(&cache->index.keys, cache->index.alloc.fd, h, sizeof(*k), &srch);
	if (rc == 1) {
		k = srch.entry;
		printf("get: ttl=%ld\n", ed_ttl_now(cache->index.epoch, k->exp));
	}
	ed_bsearch_final(&srch);
	if (rc < 0) { goto unlock; }

unlock:
	ed_idx_lock(&cache->index, ED_LCK_UN, true);
#endif
	*objp = NULL;
	return ed_esys(ENOTSUP);
}

int
ed_create(EdCache *cache, EdObject **objp, EdObjectAttr *attr)
{
	(void)cache;
	(void)attr;
#if 0
	EdBSearch srch;
	EdNodeKey key = {
		.hash = ed_hash(attr->key, attr->key_size, cache->index.seed),
		.exp = ed_expire(cache->index.epoch, attr->expiry),
		.meta = ED_PG_NONE,
		.slab = ED_BLK_NONE,
	};
	printf("set: key=%.*s, hash=%llu, ttl=%ld\n", (int)attr->key_size, attr->key, key.hash, attr->expiry);

	int rc;

	rc = ed_idx_lock(&cache->index, ED_LCK_EX, true);
	if (rc < 0) { return 0; }

	rc = ed_idx_load_trees(&cache->index);
	if (rc < 0) { goto unlock; }

	rc = ed_btree_search(&cache->index.keys, cache->index.alloc.fd, key.hash, sizeof(key), &srch);
	if (rc < 0) { goto unlock; }
	rc = ed_bsearch_ins(&srch, &key, &cache->index.alloc);
	ed_bsearch_final(&srch);
	if (rc < 0) { goto unlock; }

unlock:
	ed_idx_save_trees(&cache->index);
	ed_idx_lock(&cache->index, ED_LCK_UN, true);
#endif
	*objp = NULL;
	return ed_esys(ENOTSUP);
}

