#include "eddy-private.h"

/**
 * +------------------------------+
 * | Page 0. Index Header         |
 * |                              |
 * +------------------------------+
 * | Page 1. Initial Free Slab    |
 * |                              |
 * +------------------------------+
 * | Page 2. Block B-Tree Root    |
 * |                              |
 * +------------------------------+
 * | Page 3. Hash B-Tree Root     |
 * |                              |
 * +------------------------------+
 * | Page 4-20. Initial Free Tail |
 * /                              /
 * +------------------------------+
 */

_Static_assert(sizeof(EdBTree) + ED_NODE_PAGE_COUNT*sizeof(EdNodePage) <= PAGESIZE,
		"ED_PAGE_PAGE_COUNT is too high");
_Static_assert(sizeof(EdBTree) + ED_NODE_KEY_COUNT*sizeof(EdNodeKey) <= PAGESIZE,
		"ED_NODE_KEY_COUNT is too high");

#define BITMASK(b) (1 << ((b) % 8))
#define BITSLOT(b) ((b) / 8)
#define BITSET(a, b) ((a)[BITSLOT(b)] |= BITMASK(b))
#define BITCLEAR(a, b) ((a)[BITSLOT(b)] &= ~BITMASK(b))
#define BITTEST(a, b) ((a)[BITSLOT(b)] & BITMASK(b))
#define BITNSLOTS(nb) ((nb + 8 - 1) / 8)

#define PG_ROOT_FREE 1
#define PG_NHDR 1
#define PG_NEXTRA 1
#define PG_NINIT (PG_NHDR + PG_NEXTRA)


static const EdIndexHdr INDEX_DEFAULT = {
	.base = { 0, ED_PGINDEX },
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
	.key_tree = ED_PAGE_NONE,
	.block_tree = ED_PAGE_NONE,
	.alloc = {
		.size_page = PAGESIZE,
		.size_block = PAGESIZE,
	},
	.size_align = ED_MAX_ALIGN,
	.alloc_count = ED_ALLOC_COUNT,
};

// Locks the index header. Blocks based on the wait parameter.
static int
lock_file(int fd, EdLock type, bool wait)
{
	struct flock l = {
		.l_type = (int)type,
		.l_whence = SEEK_SET,
		.l_start = 0,
		.l_len = PAGESIZE,
	};
	int rc, op = wait ? F_SETLKW : F_SETLK;
	while ((rc = fcntl(fd, op, &l)) < 0 &&
			(rc = ED_ERRNO) == ed_esys(EINTR)) {}
	return rc;
}

static int
hdr_verify(const EdIndexHdr *hdr, struct stat *s, uint64_t ino, EdPgno pages)
{
	if (s->st_size < (off_t)sizeof(*hdr)) { return ED_EINDEX_SIZE; }
	if (memcmp(hdr->magic, INDEX_DEFAULT.magic, sizeof(hdr->magic)) != 0) {
		return ED_EINDEX_MAGIC;
	}
	if (hdr->endian != INDEX_DEFAULT.endian) { return ED_EINDEX_ENDIAN; }
	if (hdr->mark != INDEX_DEFAULT.mark) { return ED_EINDEX_MARK; }
	if (hdr->slab_ino != ino) { return ED_EINDEX_INODE; }
	if (hdr->slab_page_count != pages) { return ED_EINDEX_PAGE_COUNT; }
	if (hdr->version != INDEX_DEFAULT.version) { return ED_EINDEX_VERSION; }
	if (hdr->alloc.size_page != INDEX_DEFAULT.alloc.size_page) { return ED_EINDEX_PAGE_SIZE; }
	if (hdr->size_align != ED_MAX_ALIGN) { return ED_EINDEX_MAX_ALIGN; }
	if (hdr->alloc_count != ED_ALLOC_COUNT) { return ED_EINDEX_ALLOC_COUNT; }
	return 0;
}

int
ed_index_open(EdIndex *index, const char *path,
		int64_t slabsize, uint64_t flags, uint64_t ino)
{
	if (slabsize <= 0) { return ED_EINDEX_PAGE_COUNT; }

	struct stat stat;
	int fd = -1, rc = 0;

	EdIndexHdr *hdr = MAP_FAILED, hdrnew = INDEX_DEFAULT;
	hdrnew.flags = ED_FSAVE(flags);
	hdrnew.epoch = (int64_t)time(NULL);
	hdrnew.slab_ino = ino;
	hdrnew.slab_page_count = slabsize/PAGESIZE;
	hdrnew.alloc.free_list = PG_ROOT_FREE;
	hdrnew.alloc.tail = (EdPgtail){ PG_NINIT, 0 };

	if (ed_rnd_u64(&hdrnew.seed) <= 0) {
		return ED_EINDEX_RANDOM;
	}

	fd = open(path, O_CLOEXEC|O_RDWR|O_CREAT, 0600);
	if (fd < 0) { return ED_ERRNO; }

	hdr = ed_pgmap(fd, 0, PG_NINIT);
	if (hdr == MAP_FAILED) { close(fd); return ED_ERRNO; }

	EdPgfree *free_list = (EdPgfree *)((uint8_t *)hdr + PG_ROOT_FREE*PAGESIZE);

	rc = lock_file(fd, ED_LOCK_EX, true);
	if (rc == 0) {
		do {
			if (fstat(fd, &stat) < 0) { rc = ED_ERRNO; break; }
			if (!S_ISREG(stat.st_mode)) { rc = ED_EINDEX_MODE; break; }
			if (!(flags & ED_FCREATE) && stat.st_size) {
				rc = hdr_verify(hdr, &stat, ino, hdrnew.slab_page_count);
				if (rc == 0 || !(flags & ED_FREBUILD)) { break; }
			}

			size_t size = PG_NINIT * PAGESIZE;
			rc = ed_mkfile(fd, size + (ED_ALLOC_COUNT * PAGESIZE));
			if (rc == 0) {
				memset(hdr, 0, size);
				memcpy(hdr, &hdrnew, sizeof(hdrnew));
				free_list->base.no = PG_ROOT_FREE;
				free_list->base.type = ED_PGFREE_HEAD;
				free_list->count = 0;
				if (msync(hdr, size, MS_SYNC) < 0) {
					rc = ED_ERRNO;
				}
			}
		} while (0);
		lock_file(fd, ED_LOCK_UN, true);
	}

	if (rc < 0) {
		if (hdr != MAP_FAILED) { ed_pgunmap(hdr, PG_NINIT); }
		close(fd);
	}
	else {
		uint64_t f = hdr->flags | ED_FOPEN(flags);
		ed_pgalloc_init(&index->alloc, &hdr->alloc, fd, f);
		index->alloc.free = free_list;
		index->flags = f;
		index->seed = hdr->seed;
		index->epoch = hdr->epoch;
		index->hdr = hdr;
		index->keys = NULL;
		index->blocks = NULL;
		pthread_rwlock_init(&index->rw, NULL);
	}
	return rc;
}

void
ed_index_close(EdIndex *index)
{
	if (index == NULL) { return; }
	ed_pgalloc_close(&index->alloc);
	ed_pgunmap(index->hdr, PG_NHDR);
	index->hdr = NULL;
}

int
ed_index_load_trees(EdIndex *index)
{
	if (ed_pgload(index->alloc.fd, (EdPg **)&index->keys, index->hdr->key_tree) == MAP_FAILED ||
		ed_pgload(index->alloc.fd, (EdPg **)&index->blocks, index->hdr->block_tree) == MAP_FAILED) {
		return ED_ERRNO;
	}
	return 0;
}

int
ed_index_save_trees(EdIndex *index)
{
	ed_pgmark(&index->keys->base, &index->hdr->key_tree, &index->alloc.dirty);
	ed_pgmark(&index->blocks->base, &index->hdr->block_tree, &index->alloc.dirty);
	ed_pgalloc_sync(&index->alloc);
	return 0;
}

int
ed_index_lock(EdIndex *index, EdLock type, bool wait)
{
	int rc = 0;
	if (type == ED_LOCK_EX) {
		rc = wait ?
			pthread_rwlock_wrlock(&index->rw) :
			pthread_rwlock_trywrlock(&index->rw);
	}
	else if (type == ED_LOCK_SH) {
		rc = wait ?
			pthread_rwlock_rdlock(&index->rw) :
			pthread_rwlock_tryrdlock(&index->rw);
	}
	if (rc != 0) { return ed_esys(rc); }

	rc = lock_file(index->alloc.fd, type, wait);

	if (rc != 0 || type == ED_LOCK_UN) {
		pthread_rwlock_unlock(&index->rw);
	}
	return rc;
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
stat_free(EdIndex *index, uint8_t *vec, EdPgfree *fs, FILE *out)
{
	EdPgno c = fs->count;
	int rc = stat_pages(vec, fs->base.no, fs->pages, c, out);
	if (rc == 0 && c > 0) {
		EdPgfree *p = ed_pgmap(index->alloc.fd, fs->pages[0], 1);
		if (p == MAP_FAILED) { return ED_ERRNO; }
		if (p->base.type == ED_PGFREE_CHLD || p->base.type == ED_PGFREE_HEAD) {
			rc = stat_free(index, vec, (EdPgfree *)p, out);
		}
		ed_pgunmap(p, 1);
	}
	return rc;
}

static int
stat_tail(EdIndex *index, uint8_t *vec, FILE *out)
{
	EdPgtail tail = atomic_load(&index->hdr->alloc.tail);
	EdPgno buf[ED_ALLOC_COUNT], n = ED_ALLOC_COUNT - tail.off;
	for (EdPgno i = 0; i < n; i++) {
		buf[i] = tail.start + tail.off + i;
	}
	return stat_pages(vec, tail.start+tail.off, buf, n, out);
}

int
ed_index_stat(EdIndex *index, FILE *out, int flags)
{
	struct stat s;
	if (fstat(index->alloc.fd, &s) < 0) { return ED_ERRNO; }

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
		(size_t)PG_NHDR,
		(size_t)(pgno - PG_NHDR)
	);

	if (flags & ED_FSTAT_EXTEND) {
		uint8_t vec[(pgno/8)+1];
		memset(vec, 0, (pgno/8)+1);

		for (EdPgno i = 0; i < PG_NHDR; i++) {
			BITSET(vec, i);
		}

		fprintf(out, "    free:\n");
		stat_tail(index, vec, out);

		rc = ed_index_lock(index, ED_LOCK_EX, true);
		if (rc < 0) { goto done; }

		BITSET(vec, index->alloc.hdr->free_list);
		rc = stat_free(index, vec, ed_pgfree_list(&index->alloc), out);

		ed_index_lock(index, ED_LOCK_UN, true);
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

