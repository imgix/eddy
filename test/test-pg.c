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
test_basic(void)
{
	mu_teardown = cleanup;

	unlink(cfg.index_path);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	EdPg *pages[2];
	EdPgFree *free;
	EdIdxTail tail;

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));

	tail.vpos = idx.hdr->tail.vpos;
	mu_assert_int_eq(tail.pos.off, 2);

	ed_free(&idx, pages, ed_len(pages));

	free = ed_alloc_free_list(&idx);
	mu_assert_int_eq(free->count, 2);

	ed_idx_close(&idx);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	mu_assert_int_eq(tail.pos.off, 2);

	free = ed_alloc_free_list(&idx);
	mu_assert_int_eq(free->count, 2);
}

static void
test_gc(void)
{
	mu_teardown = cleanup;

	unlink(cfg.index_path);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	EdPg *pages[8];

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&idx, 1, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&idx, 2, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&idx, 3, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_gc_run(&idx, 3, 2), ed_len(pages) * 2);
	mu_assert_int_eq(ed_gc_run(&idx, 3, 1), 0);
	mu_assert_int_eq(ed_gc_run(&idx, 4, 1), ed_len(pages));
}

static void
test_gc_merge(void)
{
	mu_teardown = cleanup;

	unlink(cfg.index_path);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	EdPg *pages[8];

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&idx, 1, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&idx, 1, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&idx, 2, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_gc_run(&idx, 3, 1), ed_len(pages) * 2);
	mu_assert_int_eq(ed_gc_run(&idx, 2, 1), 0);
	mu_assert_int_eq(ed_gc_run(&idx, 3, 1), ed_len(pages));
}

static void
test_gc_reopen(void)
{
	mu_teardown = cleanup;

	unlink(cfg.index_path);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	EdPg *pages[8];

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&idx, 1, pages, ed_len(pages)), 0);

	ed_idx_close(&idx);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put( &idx, 2, pages, ed_len(pages)), 0);

	ed_idx_close(&idx);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&idx, 3, pages, ed_len(pages)), 0);

	ed_idx_close(&idx);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	mu_assert_int_eq(ed_gc_run(&idx, 3, 2), ed_len(pages) * 2);

	ed_idx_close(&idx);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	mu_assert_int_eq(ed_gc_run(&idx, 3, 1), 0);

	ed_idx_close(&idx);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	mu_assert_int_eq(ed_gc_run(&idx, 4, 1), ed_len(pages));
}

static void
test_gc_large(void)
{
	mu_teardown = cleanup;

	unlink(cfg.index_path);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	EdTxnId xid = 0;

	EdPg *pages[4096];
	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));

	for (size_t i = 0; i < ed_len(pages)/2; i += 16) {
		mu_assert_int_eq(ed_gc_put(&idx, ++xid, pages+i, 16), 0);
	}

	mu_assert_int_eq(ed_gc_run(&idx, 2, 1000), 16);
	mu_assert_int_eq(ed_gc_run(&idx, 100, 1000), 1568);

	for (size_t i = ed_len(pages)/2; i < ed_len(pages); i += 16) {
		mu_assert_int_eq(ed_gc_put(&idx, ++xid, pages+i, 16), 0);
	}

	mu_assert_int_eq(ed_gc_run(&idx, 257, 1000), 2512);
}

static void
test_gc_single(void)
{
	mu_teardown = cleanup;

	unlink(cfg.index_path);
	mu_assert_int_eq(ed_idx_open(&idx, &cfg), 0);

	EdTxnId xid = 0;

	EdPg *pages[1024];
	mu_assert_int_eq(ed_alloc(&idx, pages, ed_len(pages), true), ed_len(pages));

	for (size_t i = 0; i < ed_len(pages); i++) {
		mu_assert_int_eq(ed_gc_put(&idx, ++xid, pages+i, 1), 0);
		mu_assert_int_eq(ed_gc_run(&idx, xid, 2), i > 0);
	}
	mu_assert_int_eq(ed_gc_run(&idx, ++xid, 2), 1);
}

int
main(void)
{
	mu_init("page");
	mu_run(test_basic);
	mu_run(test_gc);
	mu_run(test_gc_merge);
	mu_run(test_gc_reopen);
	mu_run(test_gc_large);
	mu_run(test_gc_single);
	return 0;
}

