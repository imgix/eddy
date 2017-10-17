#include "eddy-private.h"

_Static_assert(offsetof(EdStat, vec) % 8 == 0,
		"EdStat vec member must be 8-byte aligned");

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
	struct stat index;
	if (fstat(idx->fd, &index) < 0) { return ED_ERRNO; }

	int rc = ed_lck(&idx->lck, idx->fd, ED_LCK_EX, flags);
	if (rc < 0) { return rc; }

	EdBpt *trees[ED_NDB] = { NULL };
	EdPgno tail_start = idx->hdr->tail_start;
	EdPgno tail_count = idx->hdr->tail_count;
	EdPgno no = tail_start + tail_count;
	EdStat *stat = calloc(1, sizeof(*stat) + no/8);

	if (stat == NULL) {
		rc = ED_ERRNO;
	}
	else {
		stat->index = index;
		stat->index_path = idx->path ? strdup(idx->path) : NULL;
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

		rc = ed_idx_acquire_snapshot(idx, trees);

		if (rc == 0) {
			EdConn *conn = idx->hdr->conns;
			stat->flags = idx->flags;
			stat->seed = idx->seed;
			stat->epoch = idx->epoch;
			stat->xid = conn->xid;
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
		}
	}

	ed_lck(&idx->lck, idx->fd, ED_LCK_UN, flags);

	stat->mark = &stat->nbpt;
	for (size_t i = 0; rc >= 0 && i < ed_len(trees); i++) {
		if (trees[i]) {
			rc = ed_bpt_mark(idx, stat, trees[i]);
		}
	}

	ed_idx_release_snapshot(idx, trees);

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
	free(stat->index_path);
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
ed_stat_has_leaks(const EdStat *stat)
{
	const uint8_t *v = stat->vec, *ve = v + stat->no / 8;
	for (; ve - v >= 8; v += 8) {
		if (*(uint64_t *)v != UINT64_C(-1)) { return true; }
	}
	for (; v < ve; v++) { if (*v != 0xff) { return true; } }
	EdPgno last = stat->no - (stat->no / 8 * 8);
	return last && *v != 0xff >> (8 - last);
}

bool
ed_stat_has_leak(EdStat *stat, EdPgno no)
{
	return no < stat->no && !ED_BIT_TEST(stat->vec, no);
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

	char created_at[64];
	ctime_r(&stat->epoch, created_at);

	fprintf(out,
		"index:\n"
		"  path: %s\n"
		"  inode: %" PRIu64 "\n"
		"  size: %zu\n"
		"  key entry size: %zu\n"
		"  block entry size: %zu\n"
		"  object header size: %zu\n"
		"  page size: %zu\n"
		"  max align: %zu\n"
		"  seed: %" PRIu64 "\n"
		"  created at: %s"
		"  xid: %" PRIu64 "\n"
		"  flags:\n"
		,
		stat->index_path,
		stat->index.st_ino,
		(size_t)stat->index.st_size,
		sizeof(EdEntryKey),
		sizeof(EdEntryBlock),
		sizeof(EdObjectHdr),
		(size_t)PAGESIZE,
		(size_t)ED_MAX_ALIGN,
		stat->seed,
		created_at,
		stat->xid);
	if (stat->flags & ED_FCHECKSUM) { fprintf(out, "  - ED_FCHECKSUM\n"); }
	if (stat->flags & ED_FPAGEALIGN) { fprintf(out, "  - ED_FPAGEALIGN\n"); }
	if (stat->flags & ED_FKEEPOLD) { fprintf(out, "  - ED_FKEEPOLD\n"); }
	fprintf(out,
		"  pages:\n"
		"    total: %zu\n"
		"    header: %zu\n"
		"    btree: %zu\n"
		"    active: %zu\n"
		"    pending: %zu\n"
		"    gc: %zu\n"
		"    tail: %zu\n"
		,
		(size_t)stat->no,
		(size_t)stat->header,
		(size_t)stat->nbpt,
		(size_t)stat->nactive,
		(size_t)stat->npending,
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
}

