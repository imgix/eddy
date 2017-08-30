#include "eddy-private.h"

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

