#include "eddy-private.h"

_Static_assert(sizeof(EdIdxHdr) <= PAGESIZE,
		"EdIdxHdr too big");
_Static_assert(sizeof(EdBpt) + ED_NODE_BLOCK_COUNT*sizeof(EdNodeBlock) <= PAGESIZE,
		"ED_PAGE_BLOCK_COUNT is too high");
_Static_assert(sizeof(EdBpt) + ED_NODE_KEY_COUNT*sizeof(EdNodeKey) <= PAGESIZE,
		"ED_NODE_KEY_COUNT is too high");

#define BITMASK(b) (1 << ((b) % 8))
#define BITSLOT(b) ((b) / 8)
#define BITSET(a, b) ((a)[BITSLOT(b)] |= BITMASK(b))
#define BITCLEAR(a, b) ((a)[BITSLOT(b)] &= ~BITMASK(b))
#define BITTEST(a, b) ((a)[BITSLOT(b)] & BITMASK(b))
#define BITNSLOTS(nb) ((nb + 8 - 1) / 8)

#define PG_ROOT_FREE 1
#define PG_NHDR(nconns) ed_count_pg(offsetof(EdIdxHdr, conns) + sizeof(EdConn)*nconns)
#define PG_NEXTRA 1
#define PG_NINIT(nconns) (PG_NHDR(nconns) + PG_NEXTRA)

#define ed_idx_flags(f) ((f) & ~ED_FRESET)


static const EdIdxHdr INDEX_DEFAULT = {
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
	.size_align = ED_MAX_ALIGN,
	.alloc_count = ED_ALLOC_COUNT,
	.slab_block_size = PAGESIZE,
	.alloc = {
		.size_page = PAGESIZE,
		.gc_head = ED_PG_NONE,
		.gc_tail = ED_PG_NONE,
	},
	.key_tree = ED_PG_NONE,
	.block_tree = ED_PG_NONE,
	.nconns = 64,
};

static int
hdr_verify(const EdIdxHdr *hdr, const struct stat *s)
{
	if (s->st_size < (off_t)sizeof(*hdr)) { return ED_EINDEX_SIZE; }
	if (memcmp(hdr->magic, INDEX_DEFAULT.magic, sizeof(hdr->magic)) != 0) {
		return ED_EINDEX_MAGIC;
	}
	if (hdr->endian != INDEX_DEFAULT.endian) { return ED_EINDEX_ENDIAN; }
	if (hdr->mark != INDEX_DEFAULT.mark) { return ED_EINDEX_MARK; }
	if (hdr->version != INDEX_DEFAULT.version) { return ED_EINDEX_VERSION; }
	if (hdr->size_align != ED_MAX_ALIGN) { return ED_EINDEX_MAX_ALIGN; }
	if (hdr->alloc_count != ED_ALLOC_COUNT) { return ED_EINDEX_ALLOC_COUNT; }
	if (hdr->alloc.size_page != INDEX_DEFAULT.alloc.size_page) { return ED_EINDEX_PAGE_SIZE; }
	return 0;
}

static int
hdr_verify_slab(const EdIdxHdr *hdr, int64_t size, ino_t ino)
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
 * @param  fd  An open file descriptor to the file containing #hdr
 * @param  xmin  Attempt lock recovery if holding a transaction id less than this id
 * @return >=0 the connection index, <0 on error
 */
static int
conn_acquire(EdIdxHdr *hdr, int fd, EdTxnId xmin)
{
	int nconns = (int)hdr->nconns;
	int rc = ed_esys(EAGAIN);
	bool all = false;
	for (int x = 0; x < 2; x++, all = true) {
		EdConn *c = hdr->conns;
		for (int i = 0; i < nconns; i++, c++) {
			if (!all && c->pid > 0 && (c->xid == 0 || c->xid >= xmin)) { continue; }
			rc = ed_flck(fd, ED_LCK_EX,
					offsetof(EdIdxHdr, conns) + i*sizeof(*hdr->conns), sizeof(*hdr->conns),
					ED_FNOBLOCK);
			if (rc == 0) {
				c->pid = getpid();
				c->xid = 0;
				return i;
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
 * @param  idx  The index into the connection array
 * @param  fd  An open file descriptor to the file containing #hdr
 */
static void
conn_release(EdIdxHdr *hdr, int i, int fd)
{
	if (i < 0) { return; }
	memset(&hdr->conns[i], 0, sizeof(*hdr->conns));
	ed_flck(fd, ED_LCK_UN,
			offsetof(EdIdxHdr, conns) + i*sizeof(*hdr->conns), sizeof(*hdr->conns),
			ED_FNOBLOCK);
}

int
ed_idx_open(EdIdx *idx, const EdConfig *cfg, int *slab_fd)
{
	EdIdxHdr *hdr = MAP_FAILED, hdrnew = INDEX_DEFAULT;
	struct stat stat;
	int fd = -1, sfd = -1, rc = 0, cix = -1;
	uint64_t flags = cfg->flags;
	unsigned nconns = cfg->max_conns;
	if (nconns == 0) { nconns = hdrnew.nconns; }
	else if (nconns > 256) { nconns = 512; }

	if (ed_rnd_u64(-1, &hdrnew.seed) <= 0) { return ED_EINDEX_RANDOM; }
	hdrnew.epoch = ed_now_unix();
	hdrnew.flags = ed_fsave(flags);
	hdrnew.slab_block_size = cfg->slab_block_size ? cfg->slab_block_size : PAGESIZE;
	hdrnew.alloc.free_list = PG_ROOT_FREE;
	hdrnew.alloc.tail = (EdAllocTail){ PG_NINIT(nconns), 0 };
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

	fd = OPEN(cfg->index_path, flags, ED_FCREATE|ED_FREPLACE);
	if (fd < 0) { rc = ED_ERRNO; goto error; }

	hdr = ed_pg_map(fd, 0, PG_NINIT(nconns));
	if (hdr == MAP_FAILED) { rc = ED_ERRNO; goto error; }

	EdPgFree *free_list = (EdPgFree *)((uint8_t *)hdr + PG_ROOT_FREE*PAGESIZE);

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

			sfd = OPEN(slab_path, flags, ED_FALLOCATE);
			if (sfd < 0 || fstat(sfd, &stat) < 0) { rc = ED_ERRNO; break; }

			int64_t slab_size = slab_init(sfd, cfg, &stat);
			if (slab_size < 0) { rc = (int)slab_size; break; }

			if (!(flags & ED_FREPLACE)) {
				rc = hdr_verify_slab(hdr, slab_size, stat.st_ino);
				break;
			}

			hdrnew.slab_block_count = (EdBlkno)(slab_size/hdrnew.slab_block_size);
			hdrnew.slab_ino = (uint64_t)stat.st_ino;

			size_t size = PG_NINIT(nconns) * PAGESIZE;
			rc = allocate_file(flags, fd, size + (ED_ALLOC_COUNT * PAGESIZE), "index");
			if (rc < 0) { break; }

			memset(hdr, 0, size);
			memcpy(hdr, &hdrnew, sizeof(hdrnew));
			free_list->base.no = PG_ROOT_FREE;
			free_list->base.type = ED_PG_FREE_HEAD;
			free_list->count = 0;
			if (msync(hdr, size, MS_SYNC) < 0) { rc = ED_ERRNO; break; }
		} while (0);
		ed_flck(fd, ED_LCK_UN, ED_IDX_LCK_OPEN_OFF, ED_IDX_LCK_OPEN_LEN, cfg->flags);
	}
	if (rc < 0) { goto error; }

	cix = conn_acquire(hdr, fd, hdr->xid > 16 ? hdr->xid - 16 : 0);
	if (cix < 0) {
		rc = cix;
		goto error;
	}

	uint64_t f = ed_idx_flags(hdr->flags | ed_fopen(flags));
	ed_lck_init(&idx->xtype.lck, ED_IDX_LCK_WRITE_OFF, ED_IDX_LCK_WRITE_LEN);
	ed_alloc_init(&idx->xtype.alloc, &hdr->alloc, fd, f);
	idx->xtype.alloc.free = free_list;
	idx->xtype.gxid = &hdr->xid;
	idx->xtype.conns = hdr->conns;
	idx->xtype.nconns = hdr->nconns;
	idx->xtype.conn = cix;
	idx->xtype.epoch = hdr->epoch;
	idx->flags = f;
	idx->seed = hdr->seed;
	idx->hdr = hdr;
	idx->keys = NULL;
	idx->blocks = NULL;
	idx->txn = NULL;

	EdTxnRef ref[] = {
		{ &hdr->key_tree, sizeof(EdNodeKey) },
		{ &hdr->block_tree, sizeof(EdNodeBlock) },
	};

	rc = ed_txn_new(&idx->txn, &idx->xtype, ref, ed_len(ref));
	if (rc < 0) {
		ed_lck_final(&idx->xtype.lck);
		ed_alloc_close(&idx->xtype.alloc);
		goto error;
	}

	*slab_fd = sfd;
	return 0;

error:
	if (hdr != MAP_FAILED) { ed_pg_unmap(hdr, PG_NINIT(nconns)); }
	if (sfd >= 0) { close(sfd); }
	if (fd >= 0) { close(fd); }
	return rc;
}

void
ed_idx_close(EdIdx *idx)
{
	if (idx == NULL) { return; }
	ed_alloc_close(&idx->xtype.alloc);
	ed_txn_close(&idx->txn, idx->flags);
	conn_release(idx->hdr, idx->xtype.conn, idx->xtype.alloc.fd);
	ed_lck_final(&idx->xtype.lck);
	ed_pg_unmap(idx->hdr, PG_NHDR(idx->hdr->nconns));
	idx->hdr = NULL;
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
	uint64_t h = ed_hash(k, klen, idx->seed);
	EdTimeUnix now = ed_now_unix();

	int rc = ed_txn_open(idx->txn, idx->flags|ED_FRDONLY);
	if (rc < 0) { return rc; }

	EdNodeKey *nkey;
	rc = ed_bpt_find(idx->txn, 0, h, (void **)&nkey);
	for (; rc == 1; rc = ed_bpt_next(idx->txn, 0, (void **)&nkey)) {
		if (!ed_expired_at(idx->xtype.epoch, nkey->exp, now)) {
			EdObjectHdr *hdr = ed_pg_map(obj->cache->fd, nkey->no, nkey->count);
			if (hdr == MAP_FAILED) {
				rc = ED_ERRNO;
				break;
			}
			if (hdr->keylen == klen && memcmp((uint8_t *)hdr+sizeof(*hdr), k, klen) == 0) {
				obj->expiry = ed_expiry_at(idx->xtype.epoch, nkey->exp, now);
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
	return ed_lck(&idx->xtype.lck, idx->xtype.alloc.fd, type, idx->flags);
}

// Tests and sets a page number in the bit vector.
static int
verify_mark(uint8_t *vec, EdPgno no)
{
	if (BITTEST(vec, no)) {
		fprintf(stderr, "%s: %u\n", ed_strerror(ED_EINDEX_PAGE_REF), no);
		return ED_EINDEX_PAGE_REF;
	}
	BITSET(vec, no);
	return 0;
}

static int
stat_pages(uint8_t *vec, EdPgno pgno, EdPgno *pages, EdPgno cnt, FILE *out)
{
	fprintf(out,
		"    - page: %u\n"
		"      size: %u\n"
		"      pages: [",
		pgno, cnt);

	if (cnt == 0) {
		fprintf(out, "]\n");
		return 0;
	}
	for (EdPgno i = 0; i < cnt; i++) {
		int rc = verify_mark(vec, pages[i]);
		if (rc < 0) { return rc; }
		if (i % 16 == 0) { fprintf(out, "\n       "); }
		fprintf(out, " %5u", pages[i]);
		if (i < cnt-1) { fprintf(out, ","); }
	}
	fprintf(out, "\n      ]\n");
	return 0;
}

// Verifies the integrity of a free list. This will recur to any child free pages.
static int
stat_free(EdIdx *idx, uint8_t *vec, EdPgFree *fs, FILE *out)
{
	EdPgno c = fs->count;
	int rc = stat_pages(vec, fs->base.no, fs->pages, c, out);
	if (rc == 0 && c > 0) {
		EdPgFree *p = ed_pg_map(idx->xtype.alloc.fd, fs->pages[0], 1);
		if (p == MAP_FAILED) { return ED_ERRNO; }
		if (p->base.type == ED_PG_FREE_CHLD || p->base.type == ED_PG_FREE_HEAD) {
			rc = stat_free(idx, vec, (EdPgFree *)p, out);
		}
		ed_pg_unmap(p, 1);
	}
	return rc;
}

static int
stat_tail(EdIdx *idx, uint8_t *vec, FILE *out)
{
	EdAllocTail tail = atomic_load(&idx->hdr->alloc.tail);
	EdPgno buf[ED_ALLOC_COUNT], n = ED_ALLOC_COUNT - tail.off;
	for (EdPgno i = 0; i < n; i++) {
		buf[i] = tail.start + tail.off + i;
	}
	return stat_pages(vec, tail.start+tail.off, buf, n, out);
}

int
ed_idx_stat(EdIdx *idx, FILE *out, int flags)
{
	struct stat s;
	if (fstat(idx->xtype.alloc.fd, &s) < 0) { return ED_ERRNO; }

	int rc = 0;
	EdPgno pgno = s.st_size / PAGESIZE;

	if (out == NULL) { out = stdout; }
	flockfile(out);

	fprintf(out,
		"index:\n"
		"  size: %zu\n"
		"  pages:\n"
		"    count: %zu\n"
		"    header: %zu\n"
		"    dynamic: %zu\n",
		(size_t)s.st_size,
		(size_t)pgno,
		(size_t)PG_NHDR(idx->hdr->nconns),
		(size_t)(pgno - PG_NHDR(idx->hdr->nconns))
	);

	if (flags & ED_FSTAT_EXTEND) {
		uint8_t vec[(pgno/8)+1];
		memset(vec, 0, (pgno/8)+1);

		for (EdPgno i = 0; i < PG_NHDR(idx->hdr->nconns); i++) {
			BITSET(vec, i);
		}

		fprintf(out, "    free:\n");
		stat_tail(idx, vec, out);

		rc = ed_idx_lock(idx, ED_LCK_SH);
		if (rc < 0) { goto done; }

		BITSET(vec, idx->xtype.alloc.hdr->free_list);
		rc = stat_free(idx, vec, ed_alloc_free_list(&idx->xtype.alloc), out);

		ed_idx_lock(idx, ED_LCK_UN);
		if (rc < 0) { goto done; }

		fprintf(out, "    lost: [");
		bool first = true;
		for (EdPgno i = 0; i < pgno; i++) {
			if (!BITTEST(vec, i)) {
				if (first) {
					fprintf(out, "%u", i);
					first = false;
				}
				else {
					fprintf(out, ", %u", i);
				}
			}
		}
		fprintf(out, "]\n");
	}

done:
	funlockfile(out);
	return rc;
}

