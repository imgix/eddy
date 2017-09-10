#include "eddy-private.h"

#define ED_BIT_MASK(b) (1 << ((b) % 8))
#define ED_BIT_SLOT(b) ((b) / 8)
#define ED_BIT_SET(a, b) ((a)[ED_BIT_SLOT(b)] |= ED_BIT_MASK(b))
#define ED_BIT_CLEAR(a, b) ((a)[ED_BIT_SLOT(b)] &= ~ED_BIT_MASK(b))
#define ED_BIT_TEST(a, b) ((a)[ED_BIT_SLOT(b)] & ED_BIT_MASK(b))

static int
push_double_page(EdStat *stat, EdPgno no)
{
	if (stat->nmultused == stat->nmultslots) {
		size_t nmultslots = stat->nmultslots;
		nmultslots = nmultslots ? nmultslots * 2 : 64;
		EdPgno *mult = realloc(stat->mult, sizeof(*mult) * nmultslots);
		if (mult == NULL) { return ED_ERRNO; }
		stat->mult = mult;
		stat->nmultslots = nmultslots;
	}
	stat->mult[stat->nmultused++] = no;
	return 0;
}

int
ed_stat_new(EdStat **statp, EdIdx *idx, uint64_t flags)
{
	struct stat sbuf;
	if (fstat(idx->fd, &sbuf) < 0) { return ED_ERRNO; }

	int rc = ed_lck(&idx->lck, idx->fd, ED_LCK_EX, flags);
	if (rc < 0) { return rc; }

	EdBpt *tree[2] = { NULL, NULL };

	ed_idx_acquire_xid(idx);

	EdPgno tail_start = idx->hdr->tail_start;
	EdPgno tail_count = idx->hdr->tail_count;
	EdPgno no = tail_start + tail_count;
	EdStat *stat = calloc(1, sizeof(*stat) + no/8);

	if (stat == NULL) {
		rc = ED_ERRNO;
	}
	else {
		stat->stat = sbuf;
		stat->header = ED_IDX_PAGES(idx->hdr->nconns);
		stat->tail_start = tail_start;
		stat->tail_count = tail_count;
		stat->no = no;

		size_t hdr = idx->hdr->base.no + ED_IDX_PAGES(idx->nconns);
		for (size_t p = 0; p < hdr; p++) {
			ED_BIT_SET(stat->vec, p);
		}
		for (EdPgno p = idx->hdr->tail_start; p < no; p++) {
			ED_BIT_SET(stat->vec, p);
		}

		EdConn *conn = idx->hdr->conns;
		stat->mark = &stat->npending;
		for (int i = 0; i < idx->nconns; i++, conn++) {
			for (EdPgno n = 0; n < conn->npending; n++) {
				ed_stat_mark(stat, conn->pending[n]);
			}
		}

		stat->mark = &stat->nactive;
		for (EdPgno n = 0; n < idx->hdr->nactive; n++) {
			ed_stat_mark(stat, idx->hdr->active[n]);
		}

		stat->mark = &stat->ngc;
		rc = ed_pg_mark_gc(idx, stat);

		for (size_t i = 0; rc >= 0 && i < ed_len(tree); i++) {
			if (ed_pg_load(idx->fd, (EdPg **)&tree[i], idx->hdr->tree[i]) == MAP_FAILED) {
				rc = ED_ERRNO;
			}
		}
	}

	ed_lck(&idx->lck, idx->fd, ED_LCK_UN, flags);

	stat->mark = &stat->nbpt;
	for (size_t i = 0; rc >= 0 && i < ed_len(tree); i++) {
		if (tree[i]) {
			rc = ed_bpt_mark(idx, stat, tree[i]);
		}
	}

	for (size_t i = 0; i < ed_len(tree); i++) {
		if (tree[i]) { ed_pg_unmap(tree[i], 1); }
	}

	ed_idx_release_xid(idx);

	if (rc < 0) { free(stat); }
	else { *statp = stat; }
	return rc;
}

void
ed_stat_free(EdStat **statp)
{
	EdStat *stat = *statp;
	if (stat == NULL) { return; }
	*statp = NULL;
	free(stat->mult);
	free(stat);
}

int
ed_stat_mark(EdStat *stat, EdPgno no)
{
	(*stat->mark)++;
	if (no > stat->no) { return 0; }
	if (!ED_BIT_TEST(stat->vec, no)) {
		ED_BIT_SET(stat->vec, no);
		return 0;
	}
	return push_double_page(stat, no);
}

bool
ed_stat_leaked(EdStat *stat, EdPgno no)
{
	return !ED_BIT_TEST(stat->vec, no);
}

const EdPgno *
ed_stat_multi_ref(EdStat *stat, size_t *count)
{
	*count = stat->nmultused;
	return stat->mult;
}

void
ed_stat_print(EdStat *stat, FILE *out)
{
	if (out == NULL) { out = stdout; }
	flockfile(out);

	fprintf(out,
		"index:\n"
		"  size: %zu\n"
		"  pages:\n"
		"    total: %zu\n"
		"    header: %zu\n"
		"    active: %zu\n"
		"    pending: %zu\n"
		"    btree: %zu\n"
		"    gc: %zu\n"
		"    tail: %zu\n"
		,
		(size_t)stat->stat.st_size,
		(size_t)stat->no,
		(size_t)stat->header,
		(size_t)stat->nactive,
		(size_t)stat->npending,
		(size_t)stat->nbpt,
		(size_t)stat->ngc,
		(size_t)stat->tail_count);

	fprintf(out, "    leaks: [");
	bool first = true;
	for (EdPgno i = 0; i < stat->no; i++) {
		if (!ED_BIT_TEST(stat->vec, i)) {
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

	fprintf(out, "    multiref: [");
	for (size_t i = 0; i < stat->nmultused; i++) {
		if (i == 0) {
			fprintf(out, "%zu", i);
		}
		else {
			fprintf(out, ", %zu", i);
		}
	}
	fprintf(out, "]\n");

	funlockfile(out);
}

