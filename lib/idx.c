#include "eddy-private.h"

_Static_assert(sizeof(EdPgIdx) <= PAGESIZE,
		"EdPgIdx too big");
_Static_assert(offsetof(EdPgIdx, tree) % 16 == 0,
		"EdPgIdx tree member is not 16-bytes aligned");
_Static_assert(sizeof(EdBpt) + ED_ENTRY_BLOCK_COUNT*sizeof(EdEntryBlock) <= PAGESIZE,
		"ED_PAGE_BLOCK_COUNT is too high");
_Static_assert(sizeof(EdBpt) + ED_ENTRY_KEY_COUNT*sizeof(EdEntryKey) <= PAGESIZE,
		"ED_ENTRY_KEY_COUNT is too high");

#define PG_ROOT_GC 1
#define PG_NEXTRA 1
#define PG_NINIT(nconns) (ED_IDX_PAGES(nconns) + PG_NEXTRA)

#define ed_idx_flags(f) ((f) & ~ED_FRESET)


static const EdPgIdx INDEX_DEFAULT = {
	.base = { 0, ED_PG_INDEX },
	.magic = { 'E', 'D', 'D', 'Y' },
#if BYTE_ORDER == LITTLE_ENDIAN
	.endian = 'l',
#elif BYTE_ORDER == BIG_ENDIAN
	.endian = 'B',
#else
# error Unkown byte order
#endif
	.mark = 0xfc,
	.version = 2,
	.size_page = PAGESIZE,
	.slab_block_size = PAGESIZE,
	.nconns = 32,
	.xid = 1,
	.gc_head = ED_PG_NONE,
	.gc_tail = ED_PG_NONE,
	.tree = { ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE },
	.active = {
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
	},
};

static const EdConn CONN_DEFAULT = {
	.pid = 0,
	.active = 0,
	.xid = 0,
	.npending = 0,
	.pending = {
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
		ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE, ED_PG_NONE,
	},
};

static int
hdr_verify(const EdPgIdx *hdr, const struct stat *s)
{
	if (s->st_size < (off_t)sizeof(*hdr)) { return ED_EINDEX_SIZE; }
	if (memcmp(hdr->magic, INDEX_DEFAULT.magic, sizeof(hdr->magic)) != 0) {
		return ED_EINDEX_MAGIC;
	}
	if (hdr->endian != INDEX_DEFAULT.endian) { return ED_EINDEX_ENDIAN; }
	if (hdr->mark != INDEX_DEFAULT.mark) { return ED_EINDEX_MARK; }
	if (hdr->version != INDEX_DEFAULT.version) { return ED_EINDEX_VERSION; }
	if (hdr->size_page != INDEX_DEFAULT.size_page) { return ED_EINDEX_PAGE_SIZE; }
	return 0;
}

static int
hdr_verify_slab(const EdPgIdx *hdr, int64_t size, ino_t ino)
{
	if (hdr->slab_block_count != (EdBlkno)(size/hdr->slab_block_size)) { return ED_ESLAB_BLOCK_COUNT; }
	if (hdr->slab_ino != (uint64_t)ino) { return ED_ESLAB_INODE; }
	return 0;
}

static long long
allocate_file(uint64_t flags, int fd, long long size, const char *type)
{
	ed_verbose(flags, "allocating %lld bytes for %s...", size, type);
	int rc = ed_mkfile(fd, size);
	if (rc < 0) {
		ed_verbose(flags, "failed (%s)\n", ed_strerror(rc));
		return rc;
	}
	ed_verbose(flags, "ok\n");
	return size;
}

static int64_t
slab_init(int fd, const EdConfig *cfg, const struct stat *s)
{
	if (ED_IS_FILE(s->st_mode)) {
		if ((intmax_t)s->st_size > (intmax_t)INT64_MAX) {
			return ED_ESLAB_SIZE;
		}
		if (cfg->slab_size && s->st_size != cfg->slab_size && (cfg->flags & ED_FALLOCATE)) {
			return allocate_file(cfg->flags, fd, cfg->slab_size, "slab");
		}
		return (int64_t)s->st_size;
	}
	if (ED_IS_DEVICE(s->st_mode)) {
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
			return ED_ESLAB_SIZE;
		}
		return (int64_t)size * (int64_t)count;
#else
# warning Devices not yet supported on this platform
#endif
	}
	return ED_ESLAB_MODE;
}

#define OPEN(path, f, ifset) \
	open(path, (O_CLOEXEC|O_RDWR | (((f) & (ifset)) ? O_CREAT : 0)), 0600)

/**
 * @brief  Locks the next available process connection slot
 *
 * Using the mmapped region in the header, this will search for the first unlocked
 * process connection slot. Initially this is only attempted on slots the have
 * been properly closed. However, if a slot is marked as locked with a possible
 * stale connection, a lock is attempted in an effort to detect an improperly
 * closed slot.
 *
 * @param  hdr  Memmory mapped index header
 * @param  connp  Target connection reference
 * @param  fd  An open file descriptor to the file containing #hdr
 * @param  xmin  Attempt lock recovery if holding a transaction id less than this id
 * @param  pid  Current process id
 * @return 0 on success, <0 on error
 */
static int
conn_acquire(EdPgIdx *hdr, EdConn **connp, int fd, EdTxnId xmin, int pid)
{
	int nconns = (int)hdr->nconns;
	int rc = ed_esys(EAGAIN);
	bool all = false;
	for (int x = 0; x < 2; x++, all = true) {
		EdConn *c = hdr->conns;
		for (int i = 0; i < nconns; i++, c++) {
			if (!all && c->pid > 0 && (c->xid == 0 || c->xid >= xmin)) { continue; }
			rc = ed_flck(fd, ED_LCK_EX,
					offsetof(EdPgIdx, conns) + i*sizeof(*hdr->conns), sizeof(*hdr->conns),
					ED_FNOBLOCK);
			if (rc == 0) {
				c->pid = pid;
				c->xid = 0;
				*connp = c;
				return 0;
			}
			if (rc != ed_esys(EAGAIN)) {
				return rc;
			}
		}
	}

	return rc;
}

/**
 * @brief  Unmarks and unlocks the process connection
 * @param  hdr  Memmory mapped index header
 * @param  connp  Current connection reference
 * @param  fd  An open file descriptor to the file containing #hdr
 */
static void
conn_release(EdPgIdx *hdr, EdConn **connp, int fd)
{
	EdConn *conn = *connp;
	if (conn == NULL) { return; }
	*connp = NULL;
	conn->pid = 0;
	conn->active = 0;
	conn->xid = 0;
	assert(conn->npending <= ed_len(conn->pending));
	ed_flck(fd, ED_LCK_UN, (uint8_t *)conn - (uint8_t *)hdr, sizeof(*conn), ED_FNOBLOCK);
}

static void
ed_idx_clear(EdIdx *idx)
{
	idx->hdr = NULL;
	idx->path = NULL;
	idx->fd = -1;
	idx->slabfd = -1;
	idx->gc_head = NULL;
	idx->gc_tail = NULL;
	idx->flags = 0;
	idx->txn = NULL;
	idx->conn = NULL;
	idx->nconns = 0;
}

int
ed_idx_open(EdIdx *idx, const EdConfig *cfg)
{
	ed_idx_clear(idx);
	ed_lck_init(&idx->lck, ED_IDX_LCK_WRITE_OFF, ED_IDX_LCK_WRITE_LEN);

	EdPgIdx *hdr = MAP_FAILED, hdrnew = INDEX_DEFAULT;
	struct stat stat;
	int fd = -1, sfd = -1, rc = 0, pid = getpid();
	uint64_t flags = cfg->flags;
	unsigned nconns = cfg->max_conns;
	if (nconns == 0) { nconns = hdrnew.nconns; }
	else if (nconns > 256) { nconns = 512; }

	if (ed_rnd_u64(-1, &hdrnew.seed) <= 0) { return ED_EINDEX_RANDOM; }
	hdrnew.epoch = ed_now_unix();
	hdrnew.flags = ed_fsave(flags);
	hdrnew.gc_head = PG_ROOT_GC;
	hdrnew.gc_tail = PG_ROOT_GC;
	hdrnew.tail_start = PG_NINIT(nconns);
	hdrnew.tail_count = ED_ALLOC_COUNT;
	if (cfg->slab_path == NULL) {
		int len = snprintf(hdrnew.slab_path, sizeof(hdrnew.slab_path)-1, "%s-slab", cfg->index_path);
		if (len < 0) { return ED_ERRNO; }
		if (len >= (int)sizeof(hdrnew.slab_path)) {
			return ED_ECONFIG_SLAB_NAME;
		}
	}
	else {
		size_t len = strnlen(cfg->slab_path, sizeof(hdrnew.slab_path));
		if (len >= sizeof(hdrnew.slab_path)) { return ED_ECONFIG_SLAB_NAME; }
		memcpy(hdrnew.slab_path, cfg->slab_path, len);
	}
	hdrnew.nconns = nconns;

	idx->fd = fd = OPEN(cfg->index_path, flags, ED_FCREATE|ED_FREPLACE);
	if (fd < 0) { rc = ED_ERRNO; goto error; }

	idx->nconns = nconns;
	idx->hdr = hdr = ed_pg_map(fd, 0, PG_NINIT(nconns));
	if (hdr == MAP_FAILED) { rc = ED_ERRNO; goto error; }

	EdPgGc *gc = (EdPgGc *)((uint8_t *)hdr + PG_ROOT_GC*PAGESIZE);
	idx->gc_head = idx->gc_tail = gc;

	rc = ed_flck(fd, ED_LCK_EX, ED_IDX_LCK_OPEN_OFF, ED_IDX_LCK_OPEN_LEN, cfg->flags);
	if (rc == 0) {
		do {
			const char *slab_path = hdrnew.slab_path;

			if (fstat(fd, &stat) < 0) { rc = ED_ERRNO; break; }
			if (!S_ISREG(stat.st_mode)) { rc = ED_EINDEX_MODE; break; }
			if (stat.st_size == 0 && (flags & ED_FCREATE)) { flags |= ED_FREPLACE; }

			if (!(flags & ED_FREPLACE)) {
				rc = hdr_verify(hdr, &stat);
				if (rc < 0) { break; }
				slab_path = hdr->slab_path;
			}

			idx->slabfd = sfd = OPEN(slab_path, flags, ED_FALLOCATE);
			if (sfd < 0 || fstat(sfd, &stat) < 0) { rc = ED_ERRNO; break; }

			int64_t slab_size = slab_init(sfd, cfg, &stat);
			if (slab_size < 0) { rc = (int)slab_size; break; }

			if (!(flags & ED_FREPLACE)) {
				rc = hdr_verify_slab(hdr, slab_size, stat.st_ino);
				break;
			}

			hdrnew.slab_block_count = (EdBlkno)(slab_size/hdrnew.slab_block_size);
			hdrnew.slab_ino = (uint64_t)stat.st_ino;

			ftruncate(fd, 0);

			size_t size = PG_NINIT(nconns) * PAGESIZE;
			rc = allocate_file(flags, fd, size + (ED_ALLOC_COUNT * PAGESIZE), "index");
			if (rc < 0) { break; }

			memcpy(hdr, &hdrnew, sizeof(hdrnew));
			for (uint16_t i = 0; i < nconns; i++) {
				memcpy(&hdr->conns[i], &CONN_DEFAULT, sizeof(CONN_DEFAULT));
			}
			gc->base.no = PG_ROOT_GC;
			gc->base.type = ED_PG_GC;
			gc->next = ED_PG_NONE;

			if (!(cfg->flags & ED_FNOSYNC)) {
				fsync(fd);
			}
		} while (0);

		rc = conn_acquire(hdr, &idx->conn, fd, hdr->xid > 16 ? hdr->xid - 16 : 0, pid);

		ed_flck(fd, ED_LCK_UN, ED_IDX_LCK_OPEN_OFF, ED_IDX_LCK_OPEN_LEN, cfg->flags);
	}
	if (rc < 0) { goto error; }

	rc = ed_txn_new(&idx->txn, idx);
	if (rc < 0) { goto error; }

	idx->flags = ed_idx_flags(hdr->flags | ed_fopen(flags));
	idx->pid = pid;
	idx->path = strdup(cfg->index_path);

	return 0;

error:
	ed_idx_close(idx);
	return rc;
}

void
ed_idx_close(EdIdx *idx)
{
	// DO NOT READ OR WRITE INTO ANY MAPPED PAGES UNLESS THE PID MATCHES.
	// This is used when cleaning up from failures when opening. That may mean
	// mapped pages have not yet been allocated in the underlying file.

	if (idx == NULL) { return; }
	if (idx->pid == getpid()) {
		ed_txn_close(&idx->txn, idx->flags);
		conn_release(idx->hdr, &idx->conn, idx->fd);
		if (idx->fd > -1) { close(idx->fd); }
		if (idx->slabfd > -1) { close(idx->slabfd); }
		ed_lck_final(&idx->lck);
	}
	if (idx->gc_tail && idx->gc_tail != MAP_FAILED && idx->gc_tail != idx->gc_head) {
		ed_pg_unmap(idx->gc_tail, 1);
	}
	if (idx->gc_head && idx->gc_head != MAP_FAILED) {
		ed_pg_unmap(idx->gc_head, 1);
	}
	if (idx->hdr && idx->hdr != MAP_FAILED) {
		ed_pg_unmap(idx->hdr, ED_IDX_PAGES(idx->nconns));
	}
	free(idx->path);
	ed_idx_clear(idx);
}

EdTxnId
ed_idx_xmin(EdIdx *idx, EdTime now)
{
	if (now == 0) {
		now = ed_time_from_unix(idx->hdr->epoch, ed_now_unix());
	}

	EdTxnId xid = idx->hdr->xid - 1;
	EdTxnId xmin = xid > 16 ? xid - 16 : 0;
	EdTime tmin = now - 10;
	EdConn *c = idx->hdr->conns, *conn = idx->conn;
	int nconns = idx->nconns;

	for (int i = 0; i < nconns; i++, c++) {
		if (c->pid == 0 || c->xid == 0) { continue; }
		if (c != conn && (c->xid < xmin || (tmin > 0 && c->active > 0 && tmin < c->active))) {
			off_t pos = offsetof(EdPgIdx, conns) + i*sizeof(*c);
			if (ed_flck(idx->fd, ED_LCK_EX, pos, sizeof(*c), ED_FNOBLOCK) == 0) {
				memset(c, 0, sizeof(*c));
				ed_flck(idx->fd, ED_LCK_UN, pos, sizeof(*c), ED_FNOBLOCK);
				continue;
			}
		}
		if (c->xid < xid) { xid = c->xid; }
	}
	return xid;
}

void
idx_set_obj(EdObject *obj, EdObjectHdr *hdr)
{
	obj->key = (uint8_t *)hdr + sizeof(*hdr);
	obj->keylen = hdr->keylen;
	obj->meta = (uint8_t *)obj->key + ed_align_max(hdr->keylen);
	obj->metalen = hdr->metalen;
	obj->data = (uint8_t *)obj->meta + ed_align_pg(hdr->metalen);
	obj->datalen = hdr->datalen;
	obj->hdr = hdr;
}

int
ed_idx_get(EdIdx *idx, const void *k, size_t klen, EdObject *obj)
{
	if (idx->pid != getpid()) { return ED_EINDEX_FORK; }

	uint64_t h = ed_hash(k, klen, idx->hdr->seed);
	EdTimeUnix now = ed_now_unix();

	int rc = ed_txn_open(idx->txn, idx->flags|ED_FRDONLY);
	if (rc < 0) { return rc; }

	EdEntryKey *nkey;
	rc = ed_bpt_find(idx->txn, 0, h, (void **)&nkey);
	for (; rc == 1; rc = ed_bpt_next(idx->txn, 0, (void **)&nkey)) {
		if (!ed_expired_at(idx->hdr->epoch, nkey->exp, now)) {
			EdObjectHdr *hdr = ed_pg_map(obj->cache->idx.slabfd, nkey->no, nkey->count);
			if (hdr == MAP_FAILED) {
				rc = ED_ERRNO;
				break;
			}
			if (hdr->keylen == klen && memcmp((uint8_t *)hdr+sizeof(*hdr), k, klen) == 0) {
				obj->expiry = ed_expiry_at(idx->hdr->epoch, nkey->exp, now);
				idx_set_obj(obj, hdr);
				break;
			}
			ed_pg_unmap(hdr, nkey->count);
		}
	}
	ed_txn_close(&idx->txn, idx->flags|ED_FRESET);
	return rc;
}

int
ed_idx_lock(EdIdx *idx, EdLckType type)
{
	if (idx->pid != getpid()) { return ED_EINDEX_FORK; }
	return ed_lck(&idx->lck, idx->fd, type, idx->flags);
}

EdTxnId
ed_idx_acquire_xid(EdIdx *idx)
{
	if (idx->pid != getpid()) { return 0; }
	EdConn *conn = idx->conn;
	conn->xid = idx->hdr->xid;
	conn->active = ed_time_from_unix(idx->hdr->epoch, ed_now_unix());
	return conn->xid;
}

void
ed_idx_release_xid(EdIdx *idx)
{
	if (idx->pid != getpid()) { return; }
	EdConn *conn = idx->conn;
	if (conn->xid > 0) {
		conn->xid = 0;
		conn->active = ed_time_from_unix(idx->hdr->epoch, ed_now_unix());
	}
}

int
ed_idx_acquire_snapshot(EdIdx *idx, EdBpt **trees)
{
	if (idx->pid != getpid()) { return ED_EINDEX_FORK; }
	ed_idx_acquire_xid(idx);
	for (int i = 0; i < ED_NDB; i++) {
		if (ed_pg_load(idx->fd, (EdPg **)&trees[i], idx->hdr->tree[i]) == MAP_FAILED) {
			int rc = ED_ERRNO;
			for (; i >= 0; i--) {
				if (trees[i]) { ed_pg_unmap(trees[i], 1); }
			}
			ed_idx_release_xid(idx);
			return rc;
		}
	}
	return 0;
}

void
ed_idx_release_snapshot(EdIdx *idx, EdBpt **trees)
{
	for (size_t i = 0; i < ED_NDB; i++) {
		if (trees[i]) {
			ed_pg_unmap(trees[i], 1);
			trees[i] = NULL;
		}
	}
	ed_idx_release_xid(idx);
}

int
ed_idx_repair_leaks(EdIdx *idx, EdStat *stat, uint64_t flags)
{
	if (idx->pid != getpid()) { return ED_EINDEX_FORK; }
	flags = flags | idx->flags;

	// TODO check connections against stat->xid with FNOBLOCK support

	bool locked = false;
	EdPgno leaks[64];
	EdPgno npg = stat->no, nleaks = 0;
	int rc = 0;

	for (EdPgno no = 0; no <= npg; no++) {
		if (ed_stat_has_leak(stat, no)) {
			leaks[nleaks++] = no;
		}
		if (nleaks == ed_len(leaks) || (nleaks > 0 && no == npg)) {
			if (!locked) {
				rc = ed_lck(&idx->lck, idx->fd, ED_LCK_EX, flags);
				if (rc < 0) { break; }
				locked = true;
			}
			rc = ed_free_pgno(idx, 0, leaks, nleaks);
			if (rc < 0) { break; }
		}
	}

	if (locked) {
		ed_lck(&idx->lck, idx->fd, ED_LCK_UN, flags);
	}
	return rc;
}

