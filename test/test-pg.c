#include "../lib/eddy-private.h"
#include "mu.h"

static EdAlloc alloc;
static const char *path = "/tmp/eddy_test_page";

static void
cleanup(void)
{
	ed_alloc_close(&alloc);
	unlink(path);
#if ED_MMAP_DEBUG
	mu_assert_int_eq(ed_pg_check(), 0);
#endif
}

static void
test_basic(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdPg *pages[2];
	EdPgFree *free;
	EdAllocTail tail;

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));

	tail = alloc.hdr->tail;
	mu_assert_int_eq(tail.off, 2);

	ed_free(&alloc, pages, ed_len(pages));

	free = ed_alloc_free_list(&alloc);
	mu_assert_int_eq(free->count, 2);

	ed_alloc_close(&alloc);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	mu_assert_int_eq(tail.off, 2);

	free = ed_alloc_free_list(&alloc);
	mu_assert_int_eq(free->count, 2);
}

static void
test_gc(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdPg *pages[8];

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&alloc, 1, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&alloc, 2, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&alloc, 3, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_gc_run(&alloc, 3, 2), ed_len(pages) * 2);
	mu_assert_int_eq(ed_gc_run(&alloc, 3, 1), 0);
	mu_assert_int_eq(ed_gc_run(&alloc, 4, 1), ed_len(pages));
}

static void
test_gc_merge(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdPg *pages[8];

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&alloc, 1, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&alloc, 1, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&alloc, 2, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_gc_run(&alloc, 3, 1), ed_len(pages) * 2);
	mu_assert_int_eq(ed_gc_run(&alloc, 2, 1), 0);
	mu_assert_int_eq(ed_gc_run(&alloc, 3, 1), ed_len(pages));
}

static void
test_gc_reopen(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdPg *pages[8];

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&alloc, 1, pages, ed_len(pages)), 0);

	ed_alloc_close(&alloc);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put( &alloc, 2, pages, ed_len(pages)), 0);

	ed_alloc_close(&alloc);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&alloc, 3, pages, ed_len(pages)), 0);

	ed_alloc_close(&alloc);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	mu_assert_int_eq(ed_gc_run(&alloc, 3, 2), ed_len(pages) * 2);

	ed_alloc_close(&alloc);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	mu_assert_int_eq(ed_gc_run(&alloc, 3, 1), 0);

	ed_alloc_close(&alloc);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	mu_assert_int_eq(ed_gc_run(&alloc, 4, 1), ed_len(pages));
}

static void
test_gc_large(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdTxnId xid = 0;

	EdPg *pages[4096];
	mu_assert_int_eq(ed_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));

	for (size_t i = 0; i < ed_len(pages)/2; i += 16) {
		mu_assert_int_eq(ed_gc_put(&alloc, ++xid, pages+i, 16), 0);
	}

	mu_assert_int_eq(ed_gc_run(&alloc, 2, 1000), 16);
	mu_assert_int_eq(ed_gc_run(&alloc, 100, 1000), 1568);

	for (size_t i = ed_len(pages)/2; i < ed_len(pages); i += 16) {
		mu_assert_int_eq(ed_gc_put(&alloc, ++xid, pages+i, 16), 0);
	}

	mu_assert_int_eq(ed_gc_run(&alloc, 257, 1000), 2512);
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
	return 0;
}

