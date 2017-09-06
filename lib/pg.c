#include "eddy-private.h"

_Static_assert(sizeof(EdPgFree) == PAGESIZE,
		"EdPgFree size invalid");
_Static_assert(sizeof(EdPgGc) == PAGESIZE,
		"EdPgGc size invalid");
_Static_assert(offsetof(EdPgGc, data) % ed_alignof(EdPgGcList) == 0,
		"EdPgGc data not properly aligned");

#define GC_LIST_PAGE_SIZE (sizeof(((EdPgGcList *)0)->pages[0]))

void *
ed_pg_map(int fd, EdPgno no, EdPgno count)
{
	if (no == ED_PG_NONE) {
		errno = EINVAL;
		return MAP_FAILED;
	}
	void *p = mmap(NULL, (size_t)count*PAGESIZE, PROT_READ|PROT_WRITE, MAP_SHARED,
			fd, (off_t)no*PAGESIZE);
#ifdef ED_MMAP_DEBUG
	if (p != MAP_FAILED) { ed_pg_track(no, p, count); }
#endif
	return p;
}

int
ed_pg_unmap(void *p, EdPgno count)
{
#ifdef ED_MMAP_DEBUG
	ed_pg_untrack(p, count);
#endif
	return munmap(p, (size_t)count*PAGESIZE);
}

void *
ed_pg_load(int fd, EdPg **pgp, EdPgno no)
{
	EdPg *pg = *pgp;
	if (pg != NULL) {
		if (pg->no == no) { return pg; }
		ed_pg_unmap(pg, 1);
	}
	if (no == ED_PG_NONE) {
		*pgp = pg = NULL;
	}
	else {
		pg = ed_pg_map(fd, no, 1);
		*pgp = pg == MAP_FAILED ? NULL : pg;
	}
	return pg;
}

void
ed_pg_unload(EdPg **pgp)
{
	EdPg *pg = *pgp;
	if (pg != NULL) {
		*pgp = NULL;
		ed_pg_unmap(pg, 1);
	}
}

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
map_live_pages(EdAlloc *alloc, EdPgno no, EdPg **p, EdPgno n)
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
page_alloc_tail(EdAlloc *alloc, EdPg **p, EdPgno n)
{
	EdAllocHdr *hdr = alloc->hdr;
	do {
		EdAllocTail old = atomic_load(&hdr->tail);
		if (old.off > ED_ALLOC_COUNT) { return ed_esys(EBADF); }

		EdPgno avail = ED_ALLOC_COUNT - old.off;
		if (avail == 0) { return 0; }
		if (avail < n) { n = avail; }

		EdAllocTail tail = { old.start, old.off + n };
		if (atomic_compare_exchange_weak(&hdr->tail, &old, tail)) {
			EdPgno no = old.start + old.off;
			int rc = map_live_pages(alloc, no, p, n);
			if (rc < 0) {
				// The map should not really fail, but if it does, try to put it into
				// the free list.
				EdPgFree *fs = ed_alloc_free_list(alloc);
				if (fs != MAP_FAILED) {
					for (; n > 0 && fs->count < ED_PG_NFREE; n--, no++) {
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
page_alloc_free_or_expand(EdAlloc *alloc, EdPg **p, EdPgno n)
{
	EdAllocHdr *hdr = alloc->hdr;
	EdPgFree *fs = ed_alloc_free_list(alloc);
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
ed_alloc(EdAlloc *alloc, EdPg **pages, EdPgno n, bool exclusive)
{
	if (n == 0) { return 0; }

	int rc = 0;
	EdPgno rem = n;
	EdPg **p = pages;

	// First take from the tail. It may be preferrable to leave tail pages for
	// unlocked allocations, but currently that is rarely used.
	rc = page_alloc_tail(alloc, p, rem);

	// When not exclusive, only use lock-free allocation from the tail pages.
	if (!exclusive) { return rc; }

	if (rc >= 0) {
		p += rc; rem -= rc;
		// Allocate from the free list or expand the file.
		while (rem > 0) {
			rc = page_alloc_free_or_expand(alloc, p, rem);
			if (rc < 0) {
				ed_free(alloc, pages, p - pages);
				break;
			}
			p += rc; rem -= rc;
		}
	}

	if (rc < 0) { return rc; }
	return (int)n;
}

static EdPgFree *
pg_push_free(EdAlloc *alloc, EdPgFree *old, EdPgFree *new)
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
ed_free(EdAlloc *alloc, EdPg **pages, EdPgno n)
{
	for (; n > 0 && *pages == NULL; pages++, n--) {}
	if (n == 0) { return; }

	EdPgFree *fs = ed_alloc_free_list(alloc);
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
		if (fs->count < ED_PG_NFREE) {
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
ed_free_pgno(EdAlloc *alloc, EdPgno *pages, EdPgno n)
{
	for (; n > 0 && *pages == ED_PG_NONE; pages++, n--) {}
	if (n == 0) { return; }

	int err = 0;

	EdPgFree *fs = ed_alloc_free_list(alloc);
	if (fs == MAP_FAILED) {
		err = errno;
		goto done;
	}

	// TODO: if any pages are sequential insert them together?
	for (; n > 0; pages++, n--) {
		EdPgno pno = *pages;
		if (pno == ED_PG_NONE) { continue; }
		// If there is space remaining, add the page to the list.
		if (fs->count < ED_PG_NFREE) {
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
ed_alloc_new(EdAlloc *alloc, const char *path, size_t meta, uint64_t flags)
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
	EdAllocHdr *hdr;
	EdPgFree *free_list;

	off_t hdrpages = ed_count_pg(ed_align_max(sizeof(*top)) + ed_align_max(sizeof(*hdr)) + meta);
	off_t allpages = hdrpages + 1 + ED_ALLOC_COUNT;

	if ((map = ed_pg_map(fd, 0, hdrpages + 1)) == MAP_FAILED) {
		rc = ED_ERRNO;
		goto done;
	}

	top = (EdPg *)map;
	hdr = (EdAllocHdr *)(map + ed_align_max(sizeof(*top)));
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
		hdr->tail = (EdAllocTail){ hdrpages+1, 0 };
		hdr->gc_head = ED_PG_NONE;
		hdr->gc_tail = ED_PG_NONE;
	}

	ed_alloc_init(alloc, hdr, fd, flags);
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
ed_alloc_meta(EdAlloc *alloc)
{
	return (uint8_t *)alloc->hdr + ed_align_max(sizeof(*alloc->hdr));
}

void
ed_alloc_init(EdAlloc *alloc, EdAllocHdr *hdr, int fd, uint64_t flags)
{
	alloc->hdr = hdr;
	alloc->pg = (void *)((uintptr_t)hdr/PAGESIZE * PAGESIZE);
	alloc->free = NULL;
	alloc->flags = flags;
	alloc->fd = fd;
	alloc->from_new = false;
}

void
ed_alloc_close(EdAlloc *alloc)
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
ed_alloc_free_list(EdAlloc *alloc)
{
	return ed_pg_load(alloc->fd, (EdPg **)&alloc->free, alloc->hdr->free_list);
}

/**
 * @brief  Calculates the maximum page count of a new list for a given size
 * @param  size  Number of bytes available
 * @return  Number of pages that would fit in a new list
 */
static EdPgno
gc_list_npages(size_t size)
{
	if (size <= offsetof(EdPgGcList, pages)) { return 0; }
	return (size - offsetof(EdPgGcList, pages)) / GC_LIST_PAGE_SIZE;
}

/**
 * @brief  Calculates the number of pages that can be added to a gc page
 * @param  pgc  GC page or NULL
 * @param  xid  Transaction ID to possibly merge with the current list
 * @return  Number of pages that would fit in the remaining gc page space
 */
static EdPgno
gc_list_npages_for(EdPgGc *pgc, EdTxnId xid)
{
	if (pgc == NULL) { return 0; }
	EdPgGcList *list = (EdPgGcList *)(pgc->data + pgc->tail);
	if (xid <= list->xid) { return pgc->remain / GC_LIST_PAGE_SIZE; }

	ssize_t tail = ed_align_type((ssize_t)sizeof(pgc->data) - pgc->remain, EdPgGcList);
	return gc_list_npages(sizeof(pgc->data) - tail);
}

/**
 * @brief  Calculates the list byte size requirement for a given number of pages
 * @param  npages  Number of pages
 * @return  Size in bytes required to store a list and pages
 */
static size_t
gc_list_size(EdPgno npages)
{
	return ed_align_type(offsetof(EdPgGcList, pages) + npages*GC_LIST_PAGE_SIZE, EdPgGcList);
}

/**
 * @brief  Pushed a new empty list onto the object
 * @param  pgc  GC page or NULL
 * @param  xid  Transaction ID to possibly merge with the current list
 * @return  A list object to append pages to, or NULL when a new pages is needed
 */
static EdPgGcList *
gc_list_next(EdPgGc *pgc, EdTxnId xid)
{
	// If NULL, a new pages is always needed.
	if (pgc == NULL) { return NULL; }

	EdPgGcList *list = (EdPgGcList *)(pgc->data + pgc->tail);

	// Older xid pages can be merged into a new xid.
	if (xid <= list->xid) {
		return pgc->remain < GC_LIST_PAGE_SIZE ? NULL : list;
	}

	ssize_t tail = ed_align_type((ssize_t)sizeof(pgc->data) - pgc->remain, EdPgGcList);
	ssize_t remain = (ssize_t)sizeof(pgc->data) - tail - offsetof(EdPgGcList, pages);

	// If the page is full, return NULL.
	if (remain < (ssize_t)sizeof(list->pages[0])) {
		return NULL;
	}

	// Load the next list in the current gc page.
	pgc->tail = tail;
	pgc->remain = remain;
	list = (EdPgGcList *)(pgc->data + pgc->tail);
	list->xid = xid;
	list->npages = 0;
	pgc->nlists++;
	return list;
}

/**
 * @brief  Initializes the first list of a gc page
 * @param  pgc  GC page
 * @param  xid  Transaction ID to initialize the list with
 * @return  A list object to append pages to
 */
static EdPgGcList *
gc_list_init(EdPgGc *pgc, EdTxnId xid)
{
	pgc->base.type = ED_PG_GC;
	pgc->next = ED_PG_NONE;
	pgc->head = 0;
	pgc->tail = 0;
	pgc->remain = sizeof(pgc->data);
	pgc->nlists = 0;
	pgc->npages = 0;
	memset(pgc->data, 0, sizeof(pgc->data));
	return gc_list_next(pgc, xid);
}

/**
 * @brief  Helper function to assign both a page pointer and page number address
 * @param  pg  Indirect page pointer to update
 * @param  no  Pointer to page number to update
 * @param  new  New page to assign to #pg and #no
 */
static void
gc_set(EdPgGc **pg, EdPgno *no, EdPgGc *new)
{
	if (new) {
		*pg = new;
		*no = new->base.no;
	}
	else {
		*pg = NULL;
		*no = ED_PG_NONE;
	}
}

int
ed_gc_put(EdAlloc *alloc, EdTxnId xid, EdPg **pg, EdPgno n)
{
	if (n < 1) { return 0; }

	EdPgGc *tail = ed_pg_load(alloc->fd, (EdPg **)&alloc->gc_tail, alloc->hdr->gc_tail);

	// Determine how many pages can be discarded into the current gc page.
	EdPgno avail = gc_list_npages_for(tail, xid);
	EdPgno remain = avail > n ? 0 : n - avail;

	// Allocate all new pages in a single allocation if needed.
	size_t used_pages = 0;
	size_t alloc_pages = ED_COUNT_SIZE(remain, ED_GC_LIST_MAX);
	EdPgGc *new[alloc_pages];
	if (alloc_pages > 0) {
		int rc = ed_alloc(alloc, (EdPg **)new, alloc_pages, true);
		if (rc < 0) { return rc; }
	}

	do {
		EdPgGcList *list = gc_list_next(tail, xid);

		// If the current list cannot hold any pages, grab the next allocated page.
		if (list == NULL) {
			assert(used_pages < alloc_pages);
			EdPgGc *next = new[used_pages++];
			if (tail) { tail->next = next->base.no; }
			list = gc_list_init(next, xid);

			// Update linked list poiners, cleaning up when necessary.
			if (alloc->gc_head == NULL) {
				gc_set(&alloc->gc_head, &alloc->hdr->gc_head, next);
			}
			else if (alloc->gc_head != alloc->gc_tail) {
				ed_pg_unmap(alloc->gc_tail, 1);
			}
			gc_set(&alloc->gc_tail, &alloc->hdr->gc_tail, next);

			tail = next;
		}

		// Add as many pages as possible to the list page.
		uint16_t rem = tail->remain / sizeof(list->pages[0]);
		if ((EdPgno)rem > n) { rem = (uint16_t)n; }
		for (uint16_t i = 0, off = list->npages; i < rem; i++) {
			list->pages[i+off] = pg[i]->no;
			ed_pg_unmap(pg[i], 1);
			pg[i] = NULL;
		}
		list->npages += rem;
		tail->remain -= rem * sizeof(list->pages[0]);
		tail->npages += rem;
		pg += rem;
		n -= rem;
	} while (n > 0);

	// All allocated pages should have been used.
	assert(used_pages == alloc_pages);

	return 0;
}

int
ed_gc_run(EdAlloc *alloc, EdTxnId xid, int limit)
{
	EdPgGc *head = ed_pg_load(alloc->fd, (EdPg **)&alloc->gc_head, alloc->hdr->gc_head);
	uint16_t pos = head ? head->head : 0;
	int rc = 0;

	for (; head != NULL && limit > 0; limit--) {
		EdPgGcList *list = (EdPgGcList *)(head->data + pos);
		if (list->xid == 0 || list->xid >= xid) { break; }
		ed_free_pgno(alloc, list->pages, list->npages);
		rc += list->npages;
		head->npages -= list->npages;

		// While more lists are available, bump to the next one.
		if (--head->nlists > 0) {
			pos += (uint16_t)gc_list_size(list->npages);
			continue;
		}
		// Stop if the tail is reached.
		else if (alloc->hdr->gc_head == alloc->hdr->gc_tail) {
			// If full, free the page and mark as empty
			if (head->remain < sizeof(EdPgGcList)) {
				ed_free(alloc, (EdPg **)&head, 1);
				gc_set(&alloc->gc_head, &alloc->hdr->gc_head, NULL);
				gc_set(&alloc->gc_tail, &alloc->hdr->gc_tail, NULL);
			}
			break;
		}

		// Map the next page in the list, and free the old head.
		EdPgGc *next = ed_pg_map(alloc->fd, head->next, 1);
		if (next == MAP_FAILED) { rc = ED_ERRNO; break; }
		pos = next->head;
		ed_free(alloc, (EdPg **)&head, 1);
		head = next;
		gc_set(&alloc->gc_head, &alloc->hdr->gc_head, head);
	}

	if (head) { head->head = pos; }
	return rc;
}

