#include "../lib/eddy-private.h"
#include "mu.h"

static EdPgAlloc alloc;
static const char *path = "/tmp/eddy_test_page";

static void
cleanup(void)
{
	ed_pg_alloc_close(&alloc);
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
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdPg *pages[2];
	EdPgFree *free;
	EdPgTail tail;

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));

	tail = alloc.hdr->tail;
	mu_assert_int_eq(tail.off, 2);

	ed_pg_free(&alloc, pages, ed_len(pages));

	free = ed_pg_alloc_free_list(&alloc);
	mu_assert_int_eq(free->count, 2);

	ed_pg_alloc_close(&alloc);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	mu_assert_int_eq(tail.off, 2);

	free = ed_pg_alloc_free_list(&alloc);
	mu_assert_int_eq(free->count, 2);
}

static void
test_gc(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdGc gc;
	EdPgno head = ED_PG_NONE, tail = ED_PG_NONE;
	ed_gc_init(&gc, &head, &tail);

	EdPg *pages[8];

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&gc, &alloc, 1, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&gc, &alloc, 2, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&gc, &alloc, 3, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 3, 2), ed_len(pages) * 2);
	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 3, 1), 0);
	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 4, 1), ed_len(pages));

	ed_gc_final(&gc);
}

static void
test_gc_merge(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdGc gc;
	EdPgno head = ED_PG_NONE, tail = ED_PG_NONE;
	ed_gc_init(&gc, &head, &tail);

	EdPg *pages[8];

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&gc, &alloc, 1, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&gc, &alloc, 1, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&gc, &alloc, 2, pages, ed_len(pages)), 0);

	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 3, 1), ed_len(pages) * 2);
	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 2, 1), 0);
	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 3, 1), ed_len(pages));

	ed_gc_final(&gc);
}

static void
test_gc_reopen(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdGc gc;
	EdPgno head = ED_PG_NONE, tail = ED_PG_NONE;
	ed_gc_init(&gc, &head, &tail);

	EdPg *pages[8];

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&gc, &alloc, 1, pages, ed_len(pages)), 0);

	ed_gc_final(&gc);
	ed_pg_alloc_close(&alloc);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);
	ed_gc_init(&gc, &head, &tail);

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&gc, &alloc, 2, pages, ed_len(pages)), 0);

	ed_gc_final(&gc);
	ed_pg_alloc_close(&alloc);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);
	ed_gc_init(&gc, &head, &tail);

	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));
	mu_assert_int_eq(ed_gc_put(&gc, &alloc, 3, pages, ed_len(pages)), 0);

	ed_gc_final(&gc);
	ed_pg_alloc_close(&alloc);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);
	ed_gc_init(&gc, &head, &tail);

	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 3, 2), ed_len(pages) * 2);

	ed_gc_final(&gc);
	ed_pg_alloc_close(&alloc);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);
	ed_gc_init(&gc, &head, &tail);

	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 3, 1), 0);

	ed_gc_final(&gc);
	ed_pg_alloc_close(&alloc);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);
	ed_gc_init(&gc, &head, &tail);

	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 4, 1), ed_len(pages));

	ed_gc_final(&gc);
}

static void
test_gc_large(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, 0, ED_FNOSYNC), 0);

	EdTxnId xid = 0;
	EdGc gc;
	EdPgno head = ED_PG_NONE, tail = ED_PG_NONE;
	ed_gc_init(&gc, &head, &tail);

	EdPg *pages[4096];
	mu_assert_int_eq(ed_pg_alloc(&alloc, pages, ed_len(pages), true), ed_len(pages));

	for (size_t i = 0; i < ed_len(pages)/2; i += 16) {
		mu_assert_int_eq(ed_gc_put(&gc, &alloc, ++xid, pages+i, 16), 0);
	}

	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 2, 1000), 16);
	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 100, 1000), 1568);

	for (size_t i = ed_len(pages)/2; i < ed_len(pages); i += 16) {
		mu_assert_int_eq(ed_gc_put(&gc, &alloc, ++xid, pages+i, 16), 0);
	}

	mu_assert_int_eq(ed_gc_run(&gc, &alloc, 257, 1000), 2512);

	ed_gc_final(&gc);
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

