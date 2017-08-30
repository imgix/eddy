#include "eddy-private.h"

_Static_assert(sizeof(EdPgFree) == PAGESIZE,
		"EdPgFree size invalid");

/**
 * @brief  Maps a number of pages and assigns them to the array
 * @param  alloc  Page allocator
 * @param  no  Starting page number
 * @param  p  Array to store mapped pages into
 * @param  n  Number of pages to map
 * @return  >0 if all pages successfully mapped,
 *          <0 error code
 */
static int
map_live_pages(EdPgAlloc *alloc, EdPgno no, EdPg **p, EdPgno n)
{
	uint8_t *pages = ed_pg_map(alloc->fd, no, n);
	if (pages == MAP_FAILED) { return ED_ERRNO; }
	for (EdPgno i = 0; i < n; i++, pages += PAGESIZE) {
		EdPg *live = (EdPg *)pages;
		live->no = no + i;
		p[i] = live;
	}
	return (int)n;
}

/**
 * @brief  Lock-free allocation from the tail pages
 * @param  alloc  Page allocator
 * @param  p  Array to store allocated pages into
 * @param  n  Number of pages to allocate
 * @return  >0 the number of pages allocated from the tail,
 *          0 if new tail page is not available,
 *          <0 error code
 */
static int
page_alloc_tail(EdPgAlloc *alloc, EdPg **p, EdPgno n)
{
	EdPgAllocHdr *hdr = alloc->hdr;
	do {
		EdPgTail old = atomic_load(&hdr->tail);
		if (old.off > ED_ALLOC_COUNT) { return ed_esys(EBADF); }

		EdPgno avail = ED_ALLOC_COUNT - old.off;
		if (avail == 0) { return 0; }
		if (avail < n) { n = avail; }

		EdPgTail tail = { old.start, old.off + n };
		if (atomic_compare_exchange_weak(&hdr->tail, &old, tail)) {
			EdPgno no = old.start + old.off;
			int rc = map_live_pages(alloc, no, p, n);
			if (rc < 0) {
				// The map should not really fail, but if it does, try to put it into
				// the free list.
				EdPgFree *fs = ed_pg_alloc_free_list(alloc);
				if (fs != MAP_FAILED) {
					for (; n > 0 && fs->count < ED_PG_FREE_COUNT; n--, no++) {
						fs->pages[fs->count++] = no;
					}
				}
				if (n > 0) {
					fprintf(stderr, "*** %u page(s) at %u lost from mmap error: %s\n",
							n, no, strerror(ed_ecode(rc)));
				}
			}
			return rc;
		}
	} while (1);
}

/**
 * @brief  Locked page allocation from the free list or a new expanded tail.
 * @param  alloc  Page allocator
 * @param  p  Array to store allocated pages into
 * @param  n  Number of pages to allocate
 * @return  >0 the number of pages allocated from the tail or free list,
 *          <0 error code
 */
static int
page_alloc_free_or_expand(EdPgAlloc *alloc, EdPg **p, EdPgno n)
{
	EdPgAllocHdr *hdr = alloc->hdr;
	EdPgFree *fs = ed_pg_alloc_free_list(alloc);
	if (fs == MAP_FAILED) { return ED_ERRNO; }

	switch (fs->count) {

	// The free list is exhausted so more space needs to be allocated. Expand the
	// file by ED_ALLOC_COUNT pages and store any extra pages in the tail. This
	// could ensure all remainging pages are allocated at once. But allocating
	// more that ED_ALLOC_COUNT makes error handling tricker, and in practive,
	// we won't be allocating that many at a time.
	case 0: {
		EdPgTail tail = atomic_load(&hdr->tail);
		assert(tail.off == ED_ALLOC_COUNT);
		tail.start += ED_ALLOC_COUNT;
		tail.off = 0;

		if (n > ED_ALLOC_COUNT) { n = ED_ALLOC_COUNT; }
		off_t size = (off_t)(tail.start + ED_ALLOC_COUNT) * PAGESIZE;
		if (ftruncate(alloc->fd, size) < 0) { return ED_ERRNO; }
		int rc = map_live_pages(alloc, tail.start, p, n);
		// only mark the pages as used if the map succeeds
		if (rc > 0) { tail.off = (EdPgno)rc; }
		atomic_store(&hdr->tail, tail);
		return rc;
	}

	// Multiple free lists are stored as a "linked list" with the subsequent list
	// stored in the first index. When a single page remains, map that page and
	// assert its a free page. This page needs to be mapped either way.
	case 1: {
		EdPg *tmp = ed_pg_map(alloc->fd, fs->pages[0], 1);
		if (tmp == MAP_FAILED) { return ED_ERRNO; }
		if (fs->base.type == ED_PG_FREE_CHLD) {
			// If the first page is a free list, promote it to the main free list.
			// The old free list page can now be changed to a live page.
			p[0] = (EdPg *)fs;
			alloc->free = fs = (EdPgFree *)tmp;
			hdr->free_list = tmp->no;
		}
		else {
			// Otherwise, tick down the count to zero and let the next allocation
			// handle creating more space.
			fs->count = 0;
			p[0] = tmp;
		}
		return 1;
	}

	// Multiple pages are available in the free list. This will allocate as many
	// pages as requested as long as they are sequential in the list.
	default: {
		EdPgno max = fs->count - (fs->base.type == ED_PG_FREE_CHLD);
		if (max > n) { max = n; }
		// Select the page range to search.
		EdPgno *pg = fs->pages + fs->count, *pgm = pg - 1, *pge = pg - max;
		// Scan for sequential pages in descending order from the end of the list.
		for (; pgm > pge && pgm[0] == pgm[-1]-1; pgm--) {}
		int rc = map_live_pages(alloc, pg[-1], p, pg - pgm);
		if (rc > 0) {
			fs->count -= (EdPgno)rc;
		}
		return rc;
	}

	}
}

int
ed_pg_alloc(EdPgAlloc *alloc, EdPg **pages, EdPgno n, bool exclusive)
{
	if (n == 0) { return 0; }

	// Try the lock-free allocation from the tail pages.
	if (!exclusive) { return page_alloc_tail(alloc, pages, n); }

	int rc = 0;
	EdPgno rem = n;
	EdPg **p = pages;

	// First take from the tail. It may be preferrable to leave tail pages for
	// unlocked allocations, but currently that is rarely used.
	rc = page_alloc_tail(alloc, p, rem);
	if (rc >= 0) {
		p += rc; rem -= rc;
		// Allocate from the free list or expand the file.
		while (rem > 0) {
			rc = page_alloc_free_or_expand(alloc, p, rem);
			if (rc < 0) {
				ed_pg_free(alloc, pages, p - pages);
				break;
			}
			p += rc; rem -= rc;
		}
	}

	if (rc < 0) { return rc; }
	return (int)n;
}

static EdPgFree *
pg_push_free(EdPgAlloc *alloc, EdPgFree *old, EdPgFree *new)
{
	// Initialize the new free list with the previous as its first member.
	new->base.type = ED_PG_FREE_CHLD;
	new->count = 1;
	new->pages[0] = old->base.no;

	// Remove the old list and place the new one.
	ed_pg_unmap(old, 1);
	alloc->hdr->free_list = new->base.no;
	alloc->free = new;

	return new;
}

void
ed_pg_free(EdPgAlloc *alloc, EdPg **pages, EdPgno n)
{
	for (; n > 0 && *pages == NULL; pages++, n--) {}
	if (n == 0) { return; }

	EdPgFree *fs = ed_pg_alloc_free_list(alloc);
	if (fs == MAP_FAILED) { 
		int err = errno;
		EdPgno no = pages[0]->no;
		EdPgno count = 0;
		for (; n > 0; pages++, n--) {
			if (*pages) {
				ed_pg_unmap(*pages, 1);
				count++;
			}
		}
		fprintf(stderr, "*** %u page(s) at %u lost while freeing: %s\n",
				count, no, strerror(err));
		return;
	}

	// TODO: if any pages are sequential insert them together?
	for (; n > 0; pages++, n--) {
		EdPg *p = *pages;
		if (p == NULL) { continue; }
		// If there is space remaining, add the page to the list.
		if (fs->count < ED_PG_FREE_COUNT) {
			// This is an attempt to keep free pages in order. This slows down frees,
			// but it can minimize the number of mmap calls during multi-page allocation
			// from the free list.
			EdPgno *pg = fs->pages, pno = p->no, cnt = fs->count, i;
			for (i = fs->base.type == ED_PG_FREE_CHLD; i < cnt && pg[i] > pno; i++) {}
			memmove(pg+i+1, pg+i, sizeof(*pg) * (cnt-i));
			pg[i] = pno;
			fs->count++;
			ed_pg_unmap(p, 1);
		}
		// Otherwise, promote the freeing page to a new list.
		else {
			fs = pg_push_free(alloc, fs, (EdPgFree *)*pages);
		}
	}
}

void
ed_pgno_free(EdPgAlloc *alloc, EdPgno *pages, EdPgno n)
{
	for (; n > 0 && *pages == ED_PG_NONE; pages++, n--) {}
	if (n == 0) { return; }

	int err = 0;

	EdPgFree *fs = ed_pg_alloc_free_list(alloc);
	if (fs == MAP_FAILED) {
		err = errno;
		goto done;
	}

	// TODO: if any pages are sequential insert them together?
	for (; n > 0; pages++, n--) {
		EdPgno pno = *pages;
		if (pno == ED_PG_NONE) { continue; }
		// If there is space remaining, add the page to the list.
		if (fs->count < ED_PG_FREE_COUNT) {
			// This is an attempt to keep free pages in order. This slows down frees,
			// but it can minimize the number of mmap calls during multi-page allocation
			// from the free list.
			EdPgno *pg = fs->pages, cnt = fs->count, i;
			for (i = fs->base.type == ED_PG_FREE_CHLD; i < cnt && pg[i] > pno; i++) {}
			memmove(pg+i+1, pg+i, sizeof(*pg) * (cnt-i));
			pg[i] = pno;
			fs->count++;
		}
		// Otherwise, promote the freeing page to a new list.
		else {
			EdPgFree *nfs = ed_pg_map(alloc->fd, pno, 1);
			if (nfs == MAP_FAILED) {
				err = errno;
				goto done;
			}
			fs = pg_push_free(alloc, fs, nfs);
		}
	}

done:
	for (EdPgno i = n; i > 0; pages++, i--) {
		if (*pages == ED_PG_NONE) { n--; }
	}
	if (err) {
		fprintf(stderr, "*** %u page(s) at %u lost while freeing: %s\n",
				n, pages[0], strerror(err));
	}
	return;
}

int
ed_pg_alloc_new(EdPgAlloc *alloc, const char *path, size_t meta, uint64_t flags)
{
	int fd = open(path, O_CLOEXEC|O_RDWR|O_CREAT, 0600);
	if (fd < 0) { return ED_ERRNO; }

	int rc = 0;
	uint8_t *map = MAP_FAILED;
	struct stat stat;

	if (fstat(fd, &stat) < 0) {
		rc = ED_ERRNO;
		goto done;
	}

	EdPg *top;
	EdPgAllocHdr *hdr;
	EdPgFree *free_list;

	off_t hdrpages = ed_count_pg(ed_align_max(sizeof(*top)) + ed_align_max(sizeof(*hdr)) + meta);
	off_t allpages = hdrpages + 1 + ED_ALLOC_COUNT;

	if ((map = ed_pg_map(fd, 0, hdrpages + 1)) == MAP_FAILED) {
		rc = ED_ERRNO;
		goto done;
	}

	top = (EdPg *)map;
	hdr = (EdPgAllocHdr *)(map + ed_align_max(sizeof(*top)));
	free_list = (EdPgFree *)(map + PAGESIZE);

	if (stat.st_size < allpages*PAGESIZE) {
		if (ftruncate(fd, allpages*PAGESIZE) < 0) {
			rc = ED_ERRNO;
			goto done;
		}
		top->no = 0;
		top->type = 0;
		free_list->base.no = hdrpages;
		free_list->base.type = ED_PG_FREE_HEAD;
		free_list->count = 0;
		hdr->size_page = PAGESIZE;
		hdr->free_list = hdrpages;
		hdr->tail = (EdPgTail){ hdrpages+1, 0 };
		hdr->gc_head = ED_PG_NONE;
		hdr->gc_tail = ED_PG_NONE;
	}

	ed_pg_alloc_init(alloc, hdr, fd, flags);
	alloc->free = free_list;
	alloc->from_new = true;
	alloc->gc_head = NULL;
	alloc->gc_tail = NULL;

done:
	if (rc < 0) {
		if (map != MAP_FAILED) { ed_pg_unmap(map, 2); }
		close(fd);
	}
	return rc;
}

void *
ed_pg_alloc_meta(EdPgAlloc *alloc)
{
	return (uint8_t *)alloc->hdr + ed_align_max(sizeof(*alloc->hdr));
}

void
ed_pg_alloc_init(EdPgAlloc *alloc, EdPgAllocHdr *hdr, int fd, uint64_t flags)
{
	alloc->hdr = hdr;
	alloc->pg = (void *)((uintptr_t)hdr/PAGESIZE * PAGESIZE);
	alloc->free = NULL;
	alloc->flags = flags;
	alloc->fd = fd;
	alloc->from_new = false;
}

void
ed_pg_alloc_close(EdPgAlloc *alloc)
{
	if (alloc->hdr && alloc->hdr != MAP_FAILED) {
		if (alloc->from_new) {
			ed_pg_unmap(alloc->pg, 1);
		}
		if (alloc->free != NULL) {
			ed_pg_unmap(alloc->free, 1);
		}
		alloc->hdr = NULL;
		alloc->pg = NULL;
	}
	if (alloc->gc_tail != NULL && alloc->gc_tail != alloc->gc_head) {
		ed_pg_unmap(alloc->gc_tail, 1);
	}
	if (alloc->gc_head != NULL) {
		ed_pg_unmap(alloc->gc_head, 1);
	}
	alloc->gc_head = NULL;
	alloc->gc_tail = NULL;
	if (alloc->fd > -1) {
		close(alloc->fd);
	}
}

EdPgFree *
ed_pg_alloc_free_list(EdPgAlloc *alloc)
{
	return ed_pg_load(alloc->fd, (EdPg **)&alloc->free, alloc->hdr->free_list);
}

