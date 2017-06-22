#include "eddy-private.h"

_Static_assert(sizeof(EdPgfree) == PAGESIZE,
		"EdPgfree size invalid");

void *
ed_pgmap(int fd, EdPgno no, size_t n)
{
	if (no == ED_PAGE_NONE) {
		errno = EINVAL;
		return MAP_FAILED;
	}
	return mmap(NULL, n*PAGESIZE, PROT_READ|PROT_WRITE, MAP_SHARED,
			fd, no*PAGESIZE);
}

int
ed_pgunmap(void *p, size_t n)
{
	return munmap(p, n*PAGESIZE);
}

int
ed_pgsync(void *p, size_t n, int flags, uint8_t lvl)
{
	switch (lvl) {
	case 0: return 0;
	case 1: if (flags & ED_FNOSYNC) { return 0; }
	}
	return msync(p, n*PAGESIZE, MS_SYNC);
}

void *
ed_pgload(int fd, EdPg **pgp, EdPgno no)
{
	EdPg *pg = *pgp;
	if (pg != NULL) {
		if (pg->no == no) { return pg; }
		ed_pgunmap(pg, 1);
	}
	if (no == ED_PAGE_NONE) {
		*pgp = pg = NULL;
	}
	else {
		pg = ed_pgmap(fd, no, 1);
		*pgp = pg == MAP_FAILED ? NULL : pg;
	}
	return pg;
}

void
ed_pgmark(EdPg *pg, EdPgno *no, uint8_t *dirty)
{
	if (pg == NULL) {
		if (*no != ED_PAGE_NONE) {
			*no = ED_PAGE_NONE;
			*dirty = 1;
		}
	}
	else if (*no != pg->no) {
		*no = pg->no;
		*dirty = 1;
	}
}

