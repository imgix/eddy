#include "../lib/eddy-private.h"
#include "mu.h"

static EdIdx idx;
static EdConfig cfg = {
	.index_path = "./test/tmp/test_page",
	.slab_path = "./test/tmp/slab",
	.slab_size = 16*1024*1024,
	.flags = ED_FNOTLCK|ED_FNOSYNC|ED_FCREATE,
};

static void
cleanup(void)
{
	ed_idx_close(&idx);
	unlink(cfg.index_path);
#if ED_MMAP_DEBUG
	mu_assert_int_eq(ed_pg_check(), 0);
#endif
}

static void
copy_pgno(EdPg **pg, EdPgno *pgno, size_t n)
{
	for (size_t i = 0; i < n; i++) {
		pgno[i] = pg[i]->no;
	}
}

static void
test_basic(void)
{
	mu_teardown = cleanup;

	unlink(cfg.index_path);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	EdPg *pages[8];
	EdPgno pgno[ed_len(pages)];

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages)), ed_len(pages));
	copy_pgno(pages, pgno, ed_len(pages));
	mu_assert_int_eq(ed_free(&idx, 0, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages)), ed_len(pages));
	// This is actually guaranteed, but for the moment it does test the alloc/free
	// interaction as expected.
	for (size_t i = 0; i < ed_len(pages); i++) {
		mu_assert_uint_eq(pages[i]->no, pgno[i]);
	}
	mu_assert_int_eq(ed_free(&idx, 0, pages, ed_len(pages)), 0);
}

static void
test_gc(void)
{
	// This REALLY hacks the transaction system...don't do that! :)
	mu_teardown = cleanup;

	unlink(cfg.index_path);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);
	mu_assert_int_ge(idx.conn, 0);

	EdPg *pages[8];
	EdPgno pgno[ed_len(pages)];

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages)), ed_len(pages));
	copy_pgno(pages, pgno, ed_len(pages));

	idx.hdr->xid = 1;
	ed_idx_acquire_xid(&idx);

	mu_assert_int_eq(ed_free(&idx, 1, pages, ed_len(pages)/2), 0);
	mu_assert_int_eq(ed_free(&idx, 2, pages+ed_len(pages)/2, ed_len(pages)/2), 0);

	idx.hdr->xid = 2;
	ed_idx_acquire_xid(&idx);

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages)), ed_len(pages));
	for (size_t i = 0; i < ed_len(pages); i++) {
		for (size_t j = 0; j < ed_len(pages); j++) {
			mu_assert_uint_ne(pages[i]->no, pgno[j]);
		}
	}
	mu_assert_int_eq(ed_free(&idx, 3, pages, ed_len(pages)), 0);
	idx.hdr->xid = 3;

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages)), ed_len(pages));
	for (size_t i = 0; i < ed_len(pages); i++) {
		for (size_t j = ed_len(pages)/2; j < ed_len(pages); j++) {
			mu_assert_uint_ne(pages[i]->no, pgno[j]);
		}
	}
	for (size_t i = 0; i < ed_len(pages)/2; i++) {
		mu_assert_uint_eq(pages[i]->no, pgno[i]);
	}
	mu_assert_int_eq(ed_free(&idx, 4, pages, ed_len(pages)), 0);
}


int
main(void)
{
	mu_init("page");
	mu_run(test_basic);
	mu_run(test_gc);
	return 0;
}

