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
#define PG_FIXED (PG_ROOT_FREE + 1)

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

// Syncs the index and/or free list as needed.
static void
sync_index(EdIndex *index)
{
	if (ed_pgsync(index->hdr, 1, index->flags, index->index_dirty) == 0) {
		index->index_dirty = 0;
	}
	if (ed_pgsync(index->free, 1, index->flags, index->free_dirty) == 0) {
		index->free_dirty = 0;
	}
}

static EdPgfree *
get_free_list(EdIndex *index)
{
	return ed_pgload(index->fd, (EdPg **)&index->keys, index->hdr->free_list);
}

// Maps a number of pages and assigns them to the array.
//   >0: the pages successfully mapped
//   <0: error code
static int
map_live_pages(EdIndex *index, EdPgno no, EdPg **p, EdPgno n)
{
	uint8_t *pages = ed_pgmap(index->fd, no, n);
	if (pages == MAP_FAILED) { return ED_ERRNO; }
	for (EdPgno i = 0; i < n; i++, pages += PAGESIZE) {
		EdPg *live = (EdPg *)pages;
		live->no = no + i;
		p[i] = live;
	}
	return (int)n;
}

// Lock-free page allocation from the tail pages. In the incredibly unlikely
// case that the mmap fails, a lock will be taken in an attempt to place the
// failed pages in the free list.
// Returns:
//   >0: number of pages allocated from the tail
//    0: new tail page is not available
//   <0: error code
static int
page_alloc_tail(EdIndex *index, EdPg **p, EdPgno n)
{
	EdIndexHdr *hdr = index->hdr;
	do {
		EdAllocTail old = atomic_load(&hdr->tail);
		if (old.off > ED_ALLOC_COUNT) { return ed_esys(EBADF); }

		EdPgno avail = ED_ALLOC_COUNT - old.off;
		if (avail == 0) { return 0; }
		if (avail < n) { n = avail; }

		EdAllocTail tail = { old.start, old.off + n };
		if (atomic_compare_exchange_weak(&hdr->tail, &old, tail)) {
			index->index_dirty = 1;
			EdPgno no = old.start + old.off;
			int rc = map_live_pages(index, no, p, n);
			if (rc < 0) {
				// The map should not really fail, but if it does, try to put it into
				// the free list.
				bool freed = false;
				EdPgfree *fs = get_free_list(index);
				if (fs != MAP_FAILED) {
					if (ed_index_lock(index, ED_LOCK_EX, true) == 0) {
						for (; n > 0 && fs->count < ED_PGFREE_COUNT; n--, no++) {
							fs->pages[fs->count++] = no;
						}
						index->free_dirty = 1;
						if (n == 0) { freed = true; }
						ed_index_lock(index, ED_LOCK_UN, true);
					}
				}
				if (!freed) {
					fprintf(stderr, "*** %u page(s) at %u lost from mmap error: %s\n",
							n, no, strerror(ed_ecode(rc)));
				}
			}
			return rc;
		}
	} while (1);
}

// Locked page allocation from the free list or a new expanded tail.
// Returns:
//   >0: number of pages allocated from the tail or free list
//   <0: error code
static int
page_alloc_lock(EdIndex *index, EdPg **p, EdPgno n)
{
	EdIndexHdr *hdr = index->hdr;
	EdPgfree *fs = get_free_list(index);
	if (fs == MAP_FAILED) { return ED_ERRNO; }

	switch (fs->count) {

	// The free list is exhausted so more space needs to be allocated. Expand the
	// file by ED_ALLOC_COUNT pages and store any extra pages in the tail. This
	// could ensure all remainging pages are allocated at once. But allocating
	// more that ED_ALLOC_COUNT makes error handling tricker, and in practive,
	// we won't be allocating that many at a time.
	case 0: {
		EdAllocTail tail = atomic_load(&hdr->tail);
		assert(tail.off == ED_ALLOC_COUNT);
		tail.start += ED_ALLOC_COUNT;
		tail.off = 0;

		if (n > ED_ALLOC_COUNT) { n = ED_ALLOC_COUNT; }
		off_t size = (off_t)(tail.start + ED_ALLOC_COUNT) * PAGESIZE;
		if (ftruncate(index->fd, size) < 0) { return ED_ERRNO; }
		int rc = map_live_pages(index, tail.start, p, n);
		// only mark the pages as used if the map succeeds
		if (rc > 0) { tail.off = (EdPgno)rc; }
		atomic_store(&hdr->tail, tail);
		index->size = size;
		index->index_dirty = 2;
		return rc;
	}

	// Multiple free lists are stored as a "linked list" with the subsequent lis
	// stored in the first index. When a single page remains, map that page and
	// assert its a free page. This page needs to be mapped either way.
	case 1: {
		EdPg *tmp = ed_pgmap(index->fd, fs->pages[0], 1);
		if (tmp == MAP_FAILED) { return ED_ERRNO; }
		if (fs->base.type == ED_PGFREE_CHLD) {
			// If the first page is a free list, promote it to the main free list.
			// The old free list page can now be changed to a live page.
			p[0] = (EdPg *)fs;
			index->free = fs = (EdPgfree *)tmp;
			hdr->free_list = tmp->no;
			index->index_dirty = 2;
		}
		else {
			// Otherwise, tick down the count to zero and let the next allocation
			// handle creating more space.
			fs->count = 0;
			index->free_dirty = 1;
			p[0] = tmp;
		}
		return 1;
	}

	// Multiple pages are available in the free list. This will allocate as many
	// pages as requested as long as they are sequential in the list.
	default: {
		EdPgno max = fs->count - (fs->base.type == ED_PGFREE_CHLD);
		if (max > n) { max = n; }
		// Select the page range to search.
		EdPgno *pg = fs->pages + fs->count, *pgm = pg - 1, *pge = pg - max;
		// Scan for sequential pages in descending order from the end of the list.
		for (; pgm > pge && pgm[0] == pgm[-1]-1; pgm--) {}
		int rc = map_live_pages(index, pg[-1], p, pg - pgm);
		if (rc > 0) {
			fs->count -= (EdPgno)rc;
			index->free_dirty = 1;
		}
		return rc;
	}

	}
}

// Allocates a page from the index file. This will first attempt a lock-free
// allocation from any tail pages. If none are available, a lock will be taken,
// and a page will be pulled from the free list. If no page is available, the
// file will be expanded.
int
ed_page_alloc(EdIndex *index, EdPg **pages, EdPgno n, bool locked)
{
	int rc = 0;
	EdPgno rem = n;

	EdPg **p = pages;

	// Try the lock-free allocation from the tail pages.
	rc = page_alloc_tail(index, p, rem);
	if (rc < 0) { goto done; }
	p += rc; rem -= rc;
	if (rem == 0) { goto done; }

	if (!locked) {
		// Take an exclusive lock if nothing was available.
		rc = ed_index_lock(index, ED_LOCK_EX, true);
		if (rc < 0) { goto done; }

		// Check the tail again in case another client triggered an expansion.
		rc = page_alloc_tail(index, p, rem);
		if (rc < 0) { goto unlock; }
		p += rc; rem -= rc;
	}

	// Allocate from the free list or expand the file.
	while (rem > 0) {
		rc = page_alloc_lock(index, p, rem);
		if (rc < 0) { goto unlock; }
		p += rc; rem -= rc;
	}

unlock:
	if (!locked) {
		ed_index_lock(index, ED_LOCK_UN, true);
	}

done:
	sync_index(index);
	if (rc < 0) {
		ed_page_free(index, pages, p - pages, false);
		return rc;
	}
	return n;
}

// Frees a disused page. This will not reclaim the disk space used for it,
// however, it will become available for later allocations.
void
ed_page_free(EdIndex *index, EdPg **pages, EdPgno n, bool locked)
{
	for (; *pages == NULL && n > 0; pages++, n--) {}
	if (n == 0) { return; }

	EdPgfree *fs = NULL;
	int rc = locked ? 0 : ed_index_lock(index, ED_LOCK_EX, true);
	if (rc == 0) {
		fs = get_free_list(index);
		if (fs == MAP_FAILED) { rc = ED_ERRNO; }
	}
	if (rc < 0) {
		EdPgno no = pages[0]->no;
		EdPgno count = 0;
		for (; n > 0; pages++, n--) {
			if (*pages) {
				ed_pgunmap(*pages, 1);
				count++;
			}
		}
		fprintf(stderr, "*** %u page(s) at %u lost while freeing: %s\n",
				count, no, strerror(ed_ecode(rc)));
		return;
	}

	// TODO: if any pages are sequential insert them together?
	for (; n > 0; pages++, n--) {
		EdPg *p = *pages;
		if (p == NULL) { continue; }
		// If there is space remaining, add the page to the list.
		if (fs->count < ED_PGFREE_COUNT) {
			// This is an attempt to keep free pages in order. This slows down frees,
			// but it can minimize the number of mmap calls during multi-page allocation
			// from the free list.
			EdPgno *pg = fs->pages, pno = p->no, cnt = fs->count, i;
			for (i = fs->base.type == ED_PGFREE_CHLD; i < cnt && pg[i] > pno; i++) {}
			memmove(pg+i+1, pg+i, sizeof(*pg) * (cnt-i));
			pg[i] = pno;
			fs->count++;
			index->free_dirty = 1;
		}
		// Otherwise, promote the freeing page to a new list and set the previous
		// list as the first member.
		else {
			// TODO: optimize split pages?
			p = (EdPg *)fs;
			fs = (EdPgfree *)*pages;
			fs->base.type = ED_PGFREE_CHLD;
			fs->count = 1;
			fs->pages[0] = p->no;
			index->hdr->free_list = fs->base.no;
			index->free = fs;
			index->index_dirty = 1;
			index->free_dirty = 2;
		}
		ed_pgunmap(p, 1);
	}

	sync_index(index);
	if (!locked) {
		ed_index_lock(index, ED_LOCK_UN, true);
	}
}

static int
hdr_verify(const EdIndexHdr *hdr, struct stat *s, uint64_t ino, EdPgno pages)
{
	if (s->st_size < (off_t)sizeof(*hdr)) { return ED_EINDEX_SIZE; }
	if (memcmp(hdr->magic, ED_INDEX_HDR_DEFAULT.magic, sizeof(hdr->magic)) != 0) {
		return ED_EINDEX_MAGIC;
	}
	if (hdr->endian != ED_INDEX_HDR_DEFAULT.endian) { return ED_EINDEX_ENDIAN; }
	if (hdr->mark != ED_INDEX_HDR_DEFAULT.mark) { return ED_EINDEX_MARK; }
	if (hdr->slab_ino != ino) { return ED_EINDEX_INODE; }
	if (hdr->slab_page_count != pages) { return ED_EINDEX_PAGE_COUNT; }
	if (hdr->version != ED_INDEX_HDR_DEFAULT.version) { return ED_EINDEX_VERSION; }
	if (hdr->size_page != ED_INDEX_HDR_DEFAULT.size_page) { return ED_EINDEX_PAGE_SIZE; }
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

	EdIndexHdr *hdr = MAP_FAILED, hdrnew = ED_INDEX_HDR_DEFAULT;
	hdrnew.flags = ED_FSAVE(flags);
	hdrnew.epoch = (int64_t)time(NULL);
	hdrnew.slab_ino = ino;
	hdrnew.slab_page_count = slabsize/PAGESIZE;
	hdrnew.free_list = PG_ROOT_FREE;
	hdrnew.tail = (EdAllocTail){ PG_FIXED, 0 };

	if (ed_rnd_u64(&hdrnew.seed) <= 0) {
		return ED_EINDEX_RANDOM;
	}

	fd = open(path, O_CLOEXEC|O_RDWR|O_CREAT, 0600);
	if (fd < 0) { return ED_ERRNO; }

	hdr = ed_pgmap(fd, 0, PG_FIXED);
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

			size_t size = PG_FIXED * PAGESIZE;
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
		if (hdr != MAP_FAILED) { ed_pgunmap(hdr, PG_FIXED); }
		close(fd);
	}
	else {
		index->fd = fd;
		index->index_dirty = 0;
		index->free_dirty = 0;
		index->flags = hdr->flags | ED_FOPEN(flags);
		index->seed = hdr->seed;
		index->epoch = hdr->epoch;
		index->hdr = hdr;
		index->free = free_list;
		index->keys = NULL;
		index->blocks = NULL;
		pthread_rwlock_init(&index->rw, NULL);
		index->size = stat.st_size;
	}
	return rc;
}

void
ed_index_close(EdIndex *index)
{
	if (index == NULL) { return; }
	ed_pgunmap(index->hdr, PG_FIXED);
	index->hdr = NULL;
}

int
ed_index_load_trees(EdIndex *index)
{
	if (ed_pgload(index->fd, (EdPg **)&index->keys, index->hdr->key_tree) == MAP_FAILED ||
		ed_pgload(index->fd, (EdPg **)&index->blocks, index->hdr->block_tree) == MAP_FAILED) {
		return ED_ERRNO;
	}
	return 0;
}

int
ed_index_save_trees(EdIndex *index)
{
	ed_pgmark(&index->keys->base, &index->hdr->key_tree, &index->index_dirty);
	ed_pgmark(&index->blocks->base, &index->hdr->block_tree, &index->index_dirty);
	sync_index(index);
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

	rc = lock_file(index->fd, type, wait);

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
		EdPgfree *p = ed_pgmap(index->fd, fs->pages[0], 1);
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
	EdAllocTail tail = atomic_load(&index->hdr->tail);
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
	if (fstat(index->fd, &s) < 0) { return ED_ERRNO; }

	int rc = 0;
	EdPgno pgno = s.st_size / PAGESIZE;

	if (out == NULL) { out = stdout; }
	flockfile(out);

	fprintf(out,
		"index:\n"
		"  size: %zu\n"
		"  pages:\n"
		"    count: %zu\n"
		"    fixed: %zu\n"
		"    dynamic: %zu\n",
		(size_t)s.st_size,
		(size_t)pgno,
		(size_t)PG_FIXED,
		(size_t)(pgno - PG_FIXED)
	);

	if (flags & ED_FSTAT_EXTEND) {
		uint8_t vec[(pgno/8)+1];
		memset(vec, 0, (pgno/8)+1);

		for (EdPgno i = 0; i < PG_FIXED; i++) {
			BITSET(vec, i);
		}

		fprintf(out, "    free:\n");
		stat_tail(index, vec, out);

		rc = ed_index_lock(index, ED_LOCK_EX, true);
		if (rc < 0) { goto done; }

		rc = stat_free(index, vec, get_free_list(index), out);

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

