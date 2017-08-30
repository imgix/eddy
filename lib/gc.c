#include "eddy-private.h"

_Static_assert(sizeof(EdPgGc) == PAGESIZE,
		"EdPgGc size invalid");
_Static_assert(offsetof(EdPgGc, data) % 8 == 0,
		"EdPgGc data not 8-byte aligned");

#define GC_LIST_PAGE_SIZE (sizeof(((EdPgGcList *)0)->pages[0]))

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
	return xid <= list->xid ?
		pgc->remain / GC_LIST_PAGE_SIZE :
		gc_list_npages(pgc->remain);
}

/**
 * @brief  Calculates the list byte size requirement for a given number of pages
 * @param  npages  Number of pages
 * @return  Size in bytes required to store a list and pages
 */
static size_t
gc_list_size(EdPgno npages)
{
	return offsetof(EdPgGcList, pages) + npages*GC_LIST_PAGE_SIZE;
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

	// If the page is full, return NULL.
	if (pgc->remain < sizeof(EdPgGcList)) {
		return NULL;
	}

	// Load the next list in the current gc page.
	pgc->tail = sizeof(pgc->data) - pgc->remain;
	pgc->remain -= offsetof(EdPgGcList, pages);
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
ed_gc_put(EdPgAlloc *alloc, EdTxnId xid, EdPg **pg, EdPgno n)
{
	if (n < 1) { return 0; }

	EdPgGc *tail = ed_pg_load(alloc->fd, (EdPg **)&alloc->gc_tail, alloc->hdr->gc_tail);

	// Determine how many pages can be discarded into the current gc page.
	EdPgno avail = gc_list_npages_for(tail, xid);
	EdPgno remain = avail > n ? 0 : n - avail;

	// Allocate all new pages in a single allocation if needed.
	size_t used_pages = 0;
	size_t alloc_pages = ED_COUNT_SIZE(remain, ED_PG_GC_LIST_MAX);
	EdPgGc *new[alloc_pages];
	if (alloc_pages > 0) {
		int rc = ed_pg_alloc(alloc, (EdPg **)new, alloc_pages, true);
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
ed_gc_run(EdPgAlloc *alloc, EdTxnId xid, int limit)
{
	EdPgGc *head = ed_pg_load(alloc->fd, (EdPg **)&alloc->gc_head, alloc->hdr->gc_head);
	uint16_t pos = head ? head->head : 0;
	int rc = 0;

	for (; head != NULL && limit > 0; limit--) {
		EdPgGcList *list = (EdPgGcList *)(head->data + pos);
		if (list->xid == 0 || list->xid >= xid) { break; }
		ed_pgno_free(alloc, list->pages, list->npages);
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
				ed_pg_free(alloc, (EdPg **)&head, 1);
				gc_set(&alloc->gc_head, &alloc->hdr->gc_head, NULL);
				gc_set(&alloc->gc_tail, &alloc->hdr->gc_tail, NULL);
			}
			break;
		}

		// Map the next page in the list, and free the old head.
		EdPgGc *next = ed_pg_map(alloc->fd, head->next, 1);
		if (next == MAP_FAILED) { rc = ED_ERRNO; break; }
		pos = next->head;
		ed_pg_free(alloc, (EdPg **)&head, 1);
		head = next;
		gc_set(&alloc->gc_head, &alloc->hdr->gc_head, head);
	}

	if (head) { head->head = pos; }
	return rc;
}

