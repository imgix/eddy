static int
pgcmp(const void *a, const void *b)
{
	if (*(EdPgno *)a < *(EdPgno *)b) { return 1; }
	if (*(EdPgno *)a > *(EdPgno *)b) { return -1; }
	return 0;
}

// Sorts each free slab in a seqentially optimized order.
static void
optimize_free(EdIndex *index, EdPageFree *fs)
{
	if (fs->count == 0) { return; }

#define OPTFREE_DEPTH 16
#define OPTFREE_PAGES (OPTFREE_DEPTH*(ED_PAGE_FREE_COUNT+1))

	// Collect the list of slabs and copy all the free page numbers.
	EdPageFree *slabs[OPTFREE_DEPTH];
	slabs[0] = fs;

	EdPgno pages[OPTFREE_PAGES];
	pages[0] = fs->base.no;
	memcpy(pages+1, fs->pages, fs->count*sizeof(pages[0]));

	EdPgno slcount = 1, pgcount = 1 + fs->count;

	for (EdPgno i = 1; i < OPTFREE_DEPTH; i++) {
		EdPage *p = pgmap(index->fd, slabs[i-1]->pages[0], 1);
		if (p == MAP_FAILED) { goto done; }
		if (ED_PAGE_ISFREE(p->type)) {
			EdPageFree *nf = (EdPageFree *)p;
			memcpy(pages+pgcount, nf->pages, nf->count*sizeof(pages[0]));
			pgcount += nf->count;
			slabs[slcount++] = nf;
		}
		else {
			pgunmap(p, 1);
			break;
		}
	}

	// Sort all pages in descending order.
	qsort(pages, pgcount, sizeof(pages[0]), pgcmp);

	// Map all the new free slab pages.
	EdPageFree *newslabs[OPTFREE_DEPTH];
	EdPgno pgend = pgcount-1;
	for (EdPgno i = 0; i < slcount; i++) {
		newslabs[i] = pgmap(index->fd, pages[pgend], 1);
		if (newslabs[i] == MAP_FAILED) {
			for (; i > 0; i--) { pgunmap(newslabs[i-1], 1); }
			goto done;
		}
		pgend -= slabs[i]->count;
	}

	// Move the sorted pages onto the new slabs.
	pgend = pgcount - 1;
	for (EdPgno i = 0; i < slcount; i++) {
		EdPgno cnt = slabs[i]->count;
		newslabs[i]->base.type = ED_PAGE_TAIL;
		newslabs[i]->count = cnt;
		memcpy(newslabs[i]->pages, pages+pgend-cnt, cnt*sizeof(pages[0]));
		pgsync(newslabs[i], 1, index->flags, 2);
		pgunmap(slabs[i], 1);
		slabs[i] = newslabs[i];
		pgend -= cnt;
	}
	index->free = newslabs[0];
	index->hdr->pgfree = newslabs[0]->base.no;
	index->free_dirty = 0;
	index->index_dirty = 1;

done:
	for (EdPgno i = 1; i < slcount; i++) {
		pgunmap(slabs[i], 1);
	}
}

int
ed_index_optimize(EdIndex *index, bool wait)
{
	int rc = ed_index_lock(index, ED_LOCK_EX, wait);
	if (rc < 0) { return rc; }
	optimize_free(index, index->free);
	ed_index_lock(index, ED_LOCK_UN, wait);
	clean(index);
	return 0;
}
