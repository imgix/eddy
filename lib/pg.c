#include "eddy-private.h"

_Static_assert(sizeof(EdPgGc) == PAGESIZE,
		"EdPgGc size invalid");
_Static_assert(offsetof(EdPgGc, data) % ed_alignof(EdPgGcList) == 0,
		"EdPgGc data not properly aligned");

void *
ed_pg_map(int fd, EdPgno no, EdPgno count, bool need)
{
	if (no == ED_PG_NONE) {
		errno = EINVAL;
		return MAP_FAILED;
	}
	int flags = MAP_SHARED;
#ifdef MAP_POPULATE
	if (need) { flags |= MAP_POPULATE; }
#else
	(void)need;
#endif
	void *p = mmap(NULL, (size_t)count*PAGESIZE, PROT_READ|PROT_WRITE, flags,
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
ed_pg_load(int fd, EdPg **pgp, EdPgno no, bool need)
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
		pg = ed_pg_map(fd, no, 1, need);
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
 * @param  idx  Index object
 * @param  no  Starting page number
 * @param  p  Array to store mapped pages into
 * @param  n  Number of pages to map
 * @return  >0 if all pages successfully mapped,
 *          <0 error code
 */
static int
map_live_pages(EdIdx *idx, EdPgno no, EdPg **p, EdPgno n)
{
	if (n == 0) { return 0; }

	uint8_t *pages = ed_pg_map(idx->fd, no, n, true);
	if (pages == MAP_FAILED) { return ED_ERRNO; }
	for (EdPgno i = 0; i < n; i++, pages += PAGESIZE) {
		EdPg *live = (EdPg *)pages;
		live->no = no + i;
		p[i] = live;
	}
	return (int)n;
}

/**
 * @brief  Maps a number of pages and assigns them to the array
 * @param  idx  Index object
 * @param  no  Sorted array of page numbers
 * @param  p  Array to store mapped pages into
 * @param  n  Number of pages to map
 * @param  need  Hint that the pages will be needed soon
 * @return  >0 if all pages successfully mapped,
 *          <0 error code
 */
static int
map_sorted_pages(EdIdx *idx, EdPgno *no, EdPg **p, EdPgno n, bool need)
{
	if (n == 0) { return 0; }

	int rc;
	EdPgno mapped = 0;
	for (EdPgno i = 1; i <= n; i++) {
		if (i == n || no[i] != no[i-1] + 1) {
			uint8_t *pages = ed_pg_map(idx->fd, no[mapped], i - mapped, need);
			if (pages == MAP_FAILED) { rc = ED_ERRNO; goto error; }
			for (; mapped < i; mapped++, pages += PAGESIZE) {
				p[mapped] = (EdPg *)pages;
			}
		}
	}
	return (int)n;

error:
	for (EdPgno i = 0; i < mapped; i++) { munmap(p[i], PAGESIZE); }
	return rc;
}

static int
map_end_pages(EdIdx *idx, EdPg **p, EdPgno n)
{
	if (n == 0) { return 0; }

	EdPgno start = idx->hdr->tail_start;
	EdPgno count = idx->hdr->tail_count;

	if (n > count) {
		count += ED_ALIGN_SIZE(n, ED_ALLOC_COUNT);
		off_t size = (off_t)(start + count) * PAGESIZE;
		if (ftruncate(idx->fd, size) < 0) { return ED_ERRNO; }
	}

	int rc = map_live_pages(idx, start, p, n);
	if (rc > 0) {
		start += n;
		count -= n;
	}
	idx->hdr->tail_start = start;
	idx->hdr->tail_count = count;
	return rc;
}

static uint16_t
gc_list_remain(EdPgGc *pgc, EdPgGcList *list)
{
	ssize_t remain = (ssize_t)sizeof(pgc->data)
		- (ssize_t)pgc->state.tail
		- (ssize_t)offsetof(EdPgGcList, pages)
		- (ssize_t)sizeof(list->pages[0]) * list->npages;
	assert(remain >= 0 && remain <= (ssize_t)sizeof(pgc->data));
	return remain;
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
	return (size - offsetof(EdPgGcList, pages)) / ED_GC_LIST_PAGE_SIZE;
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
	EdPgGcList *list = (EdPgGcList *)(pgc->data + pgc->state.tail);
	uint16_t remain = gc_list_remain(pgc, list);
	if (xid <= list->xid) { return remain / ED_GC_LIST_PAGE_SIZE; }

	ssize_t tail = ed_align_type((ssize_t)sizeof(pgc->data) - remain, EdPgGcList);
	return gc_list_npages(sizeof(pgc->data) - tail);
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

	EdPgGcList *list = (EdPgGcList *)(pgc->data + pgc->state.tail);
	uint16_t nlists = pgc->state.nlists;
	uint16_t remain = gc_list_remain(pgc, list);

	// Older xid pages can be merged into a new xid.
	if (nlists > 0 && xid <= list->xid) {
		return remain < ED_GC_LIST_PAGE_SIZE ? NULL : list;
	}

	// If the page is full, return NULL.
	if (remain < sizeof(*list)) {
		return NULL;
	}

	uint16_t tail = nlists ?
		pgc->state.tail + ED_GC_LIST_SIZE(list->npages) :
		pgc->state.head;

	// Load the next list in the current gc page.
	pgc->state = (EdPgGcState) {
		.head = pgc->state.head,
		.tail = tail,
		.nlists = nlists + 1,
		.nskip = pgc->state.nskip
	};
	list = (EdPgGcList *)(pgc->data + tail);
	list->xid = xid;
	list->npages = 0;
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
	pgc->state = (EdPgGcState) { 0, 0, 0, 0 };
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
gc_set(EdPgGc **pg, EdPgnoV *no, EdPgGc *new)
{
	assert(new != NULL);
	*pg = new;
	if (*no != new->base.no) {
		*no = new->base.no;
	}
}

static int
pg_cmp(const void *a, const void *b)
{
	EdPgno pa = *(EdPgno *)a;
	EdPgno pb = *(EdPgno *)b;
	if (pa < pb) { return -1; }
	else if(pa > pb) { return 1; }
	return 0;
}

#if 0
static void
gc_check(EdPgGc *gc, EdPgno *p, EdPgno n)
{
	uint16_t head = gc->head;
	uint16_t nskip = gc->nskip;
	while (head <= gc->tail) {
		const EdPgGcList *list = (const EdPgGcList *)(gc->data + head);
		for (EdPgno i = 0; i < n; i++) {
			for (uint16_t j = nskip; j < list->npages; j++) {
				if (p[i] == list->pages[j]) {
					fprintf(stderr, "*** duplicate page %u\n", p[i]);
					ed_backtrace_print(NULL, 4, stderr);
					abort();
				}
			}
		}
		head += (uint16_t)ED_GC_LIST_SIZE(list->npages);
		nskip = 0;
	}
}
#else
# define gc_check(gc, p, n)
#endif

static void
gc_unmap(EdIdx *idx, EdPgGc *gc)
{
	if (gc != idx->gc_head && gc != idx->gc_tail) {
		ed_pg_unmap(gc, 1);
	}
}

int
ed_pg_mark_gc(EdIdx *idx, EdStat *stat)
{
	EdPgGc *gc = ed_pg_load(idx->fd, (EdPg **)&idx->gc_head, idx->hdr->gc_head, true);
	if (gc == MAP_FAILED) { return ED_ERRNO; }

	int rc = ed_stat_mark(stat, gc->base.no);
	if (rc < 0) { return rc; }
	while (gc) {
		uint16_t head = gc->state.head;
		uint16_t nlists = gc->state.nlists;
		uint16_t nskip = gc->state.nskip;
		for (; nlists > 0; nlists--) {
			const EdPgGcList *list = (const EdPgGcList *)(gc->data + head);
			for (uint16_t i = nskip; i < list->npages; i++) {
				rc = ed_stat_mark(stat, list->pages[i]);
				if (rc < 0) { break; }
			}
			head += (uint16_t)ED_GC_LIST_SIZE(list->npages);
			nskip = 0;
		}

		EdPgGc *next = NULL;
		if (gc->next != ED_PG_NONE) {
			next = ed_pg_map(idx->fd, gc->next, 1, true);
			if (next == MAP_FAILED) {
				rc = ED_ERRNO;
				break;
			}
			gc_unmap(idx, gc);
			gc = next;
		}
		else {
			break;
		}
	}
	gc_unmap(idx, gc);
	return rc;
}

int
ed_alloc(EdIdx *idx, EdPg **pg, EdPgno npg, bool need)
{
	if (npg == 0) { return 0; }
	if (npg > 1024) { return ed_esys(EINVAL); }

	EdPgGc *gc = ed_pg_load(idx->fd, (EdPg **)&idx->gc_head,
			idx->hdr->gc_head, true);
	if (gc == MAP_FAILED) { return ED_ERRNO; }

	int rc = 0;
	EdTxnId xid = ed_idx_xmin(idx, 0);

	// Copy the fields so we can just bail on error without corrupting the gc page.
	EdPgGcState state = gc->state;

	// This is temporary staging area for page numbers.
	EdPgno pgno[npg];
	EdPgno npgno = 0; // Page count added to #pgno. This may end up less than npg.
	EdPgno nrecycle = 0; // Number of pages recycled from mapped gc pages.
	EdPgno nmap = 0; // Number of pages mapped from #pgno.

	while (npgno + nrecycle < npg) {
		// If we grabbed all the pages in the list, jump to the next one. If there
		// are no more lists, attempt to load the next one.
		if (state.nlists == 0) {
			// If this is the last gc page do not recycle it.
			if (gc->base.no == idx->hdr->gc_tail) {
				break;
			}

			// Recycle the gc page. This is already mapped so put it directly into the
			// output page array.
			pg[nrecycle++] = (EdPg *)gc;

			// The gc variable can safely be overwritten, but the new value will need
			// to be moved into the index once all #pgno pages have been mapped.
			gc = ed_pg_map(idx->fd, gc->next, 1, true);
			if (gc == MAP_FAILED) {
				rc = ED_ERRNO;
				goto error;
			}

			// Update the fields for the new gc object. We don't need to retain the
			// previous values. Either the an error will occur and we bail out on all
			// operations, or the old gc page gets recycled and these values get set
			// in the new gc page.
			state = gc->state;
			if (state.nlists == 0) {
				break;
			}
		}

		// Load the head list from the gc data segment.
		const EdPgGcList *list = (const EdPgGcList *)(gc->data + state.head);
		if (list->xid > 0 && list->xid >= xid) { break; }

		// Clamp page count to the remaining number of pages needed.
		assert(list->npages >= state.nskip);
		const uint16_t npages = list->npages - state.nskip;
		const uint16_t nuse = npages > npg - npgno - nrecycle ?
			npg - npgno - nrecycle : npages;

		// Copy list pages to the staging array.
		memcpy(pgno + npgno, list->pages + state.nskip, nuse * sizeof(pgno[0]));
		npgno += nuse;

		// If we did not need all the pages in the list, mark the new skip position
		// and exit the loop.
		if (nuse < npages) {
			state.nskip += nuse;
			break;
		}

		// Otherwise reset the skip position and continue with the next list.
		state.head += (uint16_t)ED_GC_LIST_SIZE(list->npages);
		state.nlists--;
		state.nskip = 0;
	}

	// Sort the collected page numbers so sequential pages can be mapped together.
	qsort(pgno, npgno, sizeof(pgno[0]), pg_cmp);
	rc = map_sorted_pages(idx, pgno, pg + nrecycle, npgno, need);
	if (rc < 0) { goto error; }
	nmap = (EdPgno)rc;

	// If we don't have enough pages, we have to map from the tail.
	if (nmap + nrecycle < npg) {
		rc = map_end_pages(idx, pg + (nmap + nrecycle), npg - (nmap + nrecycle));
		if (rc < 0) { goto error; }
	}

	if (state.head > state.tail) { state.tail = state.head; }

	gc->state = state;
	gc_set(&idx->gc_head, &idx->hdr->gc_head, gc);

	return (int)npg;

error:
	// Skip the first recycled page as that is the active gc list.
	for (EdPgno i = nrecycle > 0 ? 1 : 0; i < nmap + nrecycle; i++) {
		ed_pg_unmap(pg[i], 1);
	}
	return rc;
}

int
ed_free(EdIdx *idx, EdTxnId xid, EdPg **pg, EdPgno n)
{
	if (n < 1) { return 0; }

	EdPgno pgno[n];
	for (EdPgno i = 0; i < n; i++) {
		pgno[i] = pg[i]->no;
	}

	int rc = ed_free_pgno(idx, xid, pgno, n);
	if (rc == 0) {
		for (EdPgno i = 0; i < n; i++) {
			ed_pg_unmap(pg[i], 1);
			pg[i] = NULL;
		}
	}
	return rc;
}

int
ed_free_pgno(EdIdx *idx, EdTxnId xid, EdPgno *pg, EdPgno n)
{
	if (n < 1) { return 0; }
#ifndef NDEBUG
	for (EdPgno x = 0; x < n; x++) {
		assert(pg[x] != ED_PG_NONE);
	}
#endif

	EdPgGc *tail = ed_pg_load(idx->fd, (EdPg **)&idx->gc_tail, idx->hdr->gc_tail, true);
	if (tail == MAP_FAILED) { return ED_ERRNO; }
	gc_check(tail, pg, n);

	size_t used_pages = 0, alloc_pages = 0;
	for (;;) {
		// Determine how many pages can be discarded into the current gc page.
		EdPgno avail = gc_list_npages_for(tail, xid);
		EdPgno remain = avail > n ? 0 : n - avail;

		// Allocate all new pages in a single allocation if needed.
		alloc_pages = ED_COUNT_SIZE(remain, ED_GC_LIST_MAX);

		// If we can safely move the list without leaking pages, move them and retry.
		// Currently, we are checking if the gc list is using no more than half the
		// available space. Technically, we can move it any time there is enough
		// space before state.head to copy the entirety of the gc lists.
		if (alloc_pages == 1 && tail->state.head >= sizeof(tail->data)/2) {
			EdPgGcState state = tail->state;
			memcpy(tail->data, tail->data + state.head, sizeof(tail->data) - state.head);
			state.tail -= state.head;
			state.head = 0;
			tail->state = state;
		}
		else {
			break;
		}
	}

	EdPgGc **new = NULL;
	if (alloc_pages > 0) {
		new = alloca(sizeof(*new) * alloc_pages);
		if (new == NULL) { return ED_ERRNO; }
		int rc = ed_alloc(idx, (EdPg **)new, alloc_pages, true);
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
			if (idx->gc_head == NULL) {
				gc_set(&idx->gc_head, &idx->hdr->gc_head, next);
			}
			else if (idx->gc_head != idx->gc_tail) {
				ed_pg_unmap(idx->gc_tail, 1);
			}
			gc_set(&idx->gc_tail, &idx->hdr->gc_tail, next);

			tail = next;
		}

		// Add as many pages as possible to the list page.
		uint16_t npg = gc_list_remain(tail, list) / sizeof(list->pages[0]);
		if ((EdPgno)npg > n) { npg = (uint16_t)n; }
		memcpy(list->pages + list->npages, pg, npg * sizeof(pg[0]));
		list->npages += npg;
		pg += npg;
		n -= npg;
	} while (n > 0);

	// All allocated pages should have been used.
	assert(used_pages == alloc_pages);

	return 0;
}

