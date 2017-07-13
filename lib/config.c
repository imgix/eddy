#include "eddy-private.h"

_Static_assert(sizeof(EdConfigP) <= sizeof(((EdConfig *)0)->_private),
		"EdConfig _private field too small");

int
ed_config_open(EdConfig *cfg)
{
	EdConfigP priv = ed_configp_make();
	struct stat cache_stat;
	int err;

	// open the cache file or device
	priv.cache_fd = open(cfg->cache_path, O_CLOEXEC|O_RDWR);
	if (priv.cache_fd < 0) {
		err = errno == EISDIR ? ED_ECACHE_MODE : ED_ERRNO;
		goto error;
	}
	if (fstat(priv.cache_fd, &cache_stat) < 0) {
		err = ED_ERRNO;
		goto error;
	}
	if (!ED_IS_MODE(cache_stat.st_mode)) {
		err = ED_ECACHE_MODE;
		goto error;
	}

	// save cache file information
	priv.dev = cache_stat.st_dev;
	priv.mode = cache_stat.st_mode;
	priv.uid = cache_stat.st_uid;
	priv.gid = cache_stat.st_gid;
	priv.size = cache_stat.st_size;
	priv.blksize = cfg->blksize > 0 ?
		(cfg->blksize + (PAGESIZE - 1)) / PAGESIZE * PAGESIZE :
		PAGESIZE * 4;
	priv.blocks = cache_stat.st_size / priv.blksize;

	// open the index file
	const char *index_path = cfg->index_path;
	char buf[8192];
	if (index_path == NULL) {
		int len = snprintf(buf, sizeof(buf), "%s-index", cfg->cache_path);
		if (len < 0) {
			err = ED_ERRNO;
			goto error;
		}
		if (len >= (int)sizeof(buf)) {
			err = ED_ECONFIG_CACHE_NAME;
			goto error;
		}
		index_path = buf;
	}
	priv.index_path = strdup(index_path);
	if (priv.index_path == NULL) {
		err = ED_ERRNO;
		goto error;
	}

	memcpy(cfg->_private, &priv, sizeof(priv));

	return 0;

error:
	ed_configp_close(&priv);
	return err;
}

void
ed_config_close(EdConfig *cfg)
{
	ed_configp_close(ed_configp_get(cfg));
}

void
ed_configp_close(EdConfigP *priv)
{
	if (priv->cache_fd > -1) {
		close(priv->cache_fd);
		priv->cache_fd = -1;
	}
	free(priv->index_path);
	priv->index_path = NULL;
}

