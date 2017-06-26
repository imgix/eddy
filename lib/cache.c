#include "eddy-private.h"

static int64_t
cache_size(int fd, struct stat *stat)
{
	if (fstat(fd, stat) < 0) { return ED_ERRNO; }
	if (S_ISREG(stat->st_mode)) {
		if (stat->st_size <= 0 || (intmax_t)stat->st_size > (intmax_t)INT64_MAX) {
			return ED_ECACHE_SIZE;
		}
		return (int64_t)stat->st_size;
	}
	if (S_ISCHR(stat->st_mode)) {
#if defined(BLKGETSIZE64)
		uint64_t size;
		if (ioctl(fd, BLKGETSIZE64, &size) < 0) { return ED_ERRNO; }
		if (size == 0 || size > (uint64_t)INT64_MAX) {
			return ED_ECACHE_SIZE;
		}
		return (int64_t)size;
#elif defined(DIOCGMEDIASIZE)
		off_t size;
		if (ioctl(fd, DIOCGMEDIASIZE, &size) < 0) { return ED_ERRNO; }
		if (size <= 0 || (intmax_t)size > (intmax_t)INT64_MAX) {
			return ED_ECACHE_SIZE;
		}
		return (int64_t)size;
#elif defined(DKIOCGETBLOCKSIZE) && defined(DKIOCGETBLOCKCOUNT)
		uint32_t size;
		uint64_t count;
		if (ioctl(fd, DKIOCGETBLOCKSIZE, &size) < 0 ||
				ioctl(fd, DKIOCGETBLOCKCOUNT, &count) < 0) {
			return ED_ERRNO;
		}
		if (size == 0 || count == 0 || count > (uint64_t)(INT64_MAX / (int64_t)size)) {
			return ED_ECACHE_SIZE;
		}
		return (int64_t)size * (int64_t)count;
#else
# warning Character devices not yet supported on this platform
#endif
	}
	return ED_ECACHE_MODE;
}

int
ed_cache_open(EdCache **cachep, const EdConfig *cfg)
{
	int ec = 0, fd = -1;
	EdCache *cache = NULL;
	struct stat stat;

	fd = open(cfg->cache_path, O_CLOEXEC|O_RDWR);
	if (fd < 0) {
		ec = errno == EISDIR ? ED_ECACHE_MODE : ED_ERRNO;
		goto error;
	}

	int64_t size = cache_size(fd, &stat);
	if (size < 0) {
		ec = (int)size;
		goto error;
	}

	const char *index_path = cfg->index_path;
	char buf[8192];
	if (index_path == NULL) {
		int len = snprintf(buf, sizeof(buf), "%s-index", cfg->cache_path);
		if (len < 0) {
			ec = ED_ERRNO;
			goto error;
		}
		if (len >= (int)sizeof(buf)) {
			ec = ED_ECONFIG_CACHE_NAME;
			goto error;
		}
		index_path = buf;
	}

	cache = malloc(sizeof(*cache));
	if (cache == NULL) {
		ec = ED_ERRNO;
		goto error;
	}

	ec = ed_index_open(&cache->index, index_path, size, cfg->flags, (uint64_t)stat.st_ino);
	if (ec < 0) { goto error; }

	cache->ref = 1;
	cache->fd = fd;
	cache->bytes_used = 0;
	cache->pages_used = 0;
	*cachep = cache;
	return 0;

error:
	free(cache);
	if (fd > -1) { close(fd); }
	return ec;
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
			ed_index_close(&cache->index);
			close(cache->fd);
			free(cache);
		}
	}
}

int
ed_cache_stat(EdCache *cache, FILE *out, int flags)
{
	if (out == NULL) { out = stdout; }

	int rc = ed_index_stat(&cache->index, out, flags);
	if (rc < 0) { return rc; }
	fprintf(out,
		"entries: %zu\n"
		"bytes:\n"
		"  used: %zu\n"
		"  wasted: %zu\n"
		"pages:\n"
		"  used: %zu\n"
		"  size: %zu\n"
		"  count: %zu\n"
		"  cursor: %zu\n",
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
	int rc = ed_index_load_trees(&cache->index);
	if (rc < 0) { return rc; }

	uint64_t h = ed_hash(key, len, cache->index.seed);
	printf("key=%.*s, hash: %llu\n", (int)len, key, h);
	EdBSearch srch;
	EdNodeKey *k;
	rc = ed_btree_search(&cache->index.keys, cache->index.alloc.fd, h, sizeof(*k), &srch);
	if (rc == 1) {
		k = srch.entry;
		printf("get: hash=%llu, ttl=%ld\n", k->hash, ed_ttl_now(cache->index.epoch, k->exp));
	}
	ed_bsearch_final(&srch);
	if (rc < 0) { return rc; }
	*objp = NULL;
	return ed_esys(ENOTSUP);
}

int
ed_create(EdCache *cache, EdObject **objp, EdObjectAttr *attr)
{
	EdBSearch srch;
	EdNodeKey key = {
		.hash = ed_hash(attr->key, attr->key_size, cache->index.seed),
		.exp = ed_expire(cache->index.epoch, attr->expiry),
		.meta = ED_PAGE_NONE,
		.slab = ED_BLK_NONE,
	};
	printf("set: key=%.*s, hash=%llu, ttl=%ld\n", (int)attr->key_size, attr->key, key.hash, attr->expiry);

	int rc;

	rc = ed_index_lock(&cache->index, ED_LOCK_EX, true);
	if (rc < 0) { return 0; }

	rc = ed_index_load_trees(&cache->index);
	if (rc < 0) { goto done; }

	rc = ed_btree_search(&cache->index.keys, cache->index.alloc.fd, key.hash, sizeof(key), &srch);
	if (rc < 0) { goto done; }
	rc = ed_bsearch_ins(&srch, &key, &cache->index.alloc);
	ed_bsearch_final(&srch);
	if (rc < 0) { goto done; }

done:
	ed_index_save_trees(&cache->index);
	ed_index_lock(&cache->index, ED_LOCK_UN, true);
	*objp = NULL;
	return ed_esys(ENOTSUP);
}

