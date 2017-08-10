#include "eddy-private.h"
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

int
main(void)
{
	mu_init("page");
	mu_run(test_basic);
	return 0;
}

