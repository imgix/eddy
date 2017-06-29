#include "eddy-private.h"
#include "mu.h"

static EdPgalloc alloc;
static const char *path = "/tmp/eddy_test_page";

static void
cleanup(void)
{
	ed_pgalloc_close(&alloc);
	unlink(path);
	mu_assert_int_eq(ed_pgcheck(), 0);
}

static void
test_basic(void)
{
	mu_teardown = cleanup;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, 0), 0);

	EdPg *pages[2];
	EdPgfree *free;
	EdPgtail tail;

	mu_assert_int_eq(ed_pgalloc(&alloc, pages, ed_len(pages), true), ed_len(pages));

	tail = alloc.hdr->tail;
	mu_assert_int_eq(tail.off, 2);

	ed_pgfree(&alloc, pages, ed_len(pages));

	free = ed_pgfree_list(&alloc);
	mu_assert_int_eq(free->count, 2);

	ed_pgalloc_close(&alloc);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, 0), 0);

	mu_assert_int_eq(tail.off, 2);

	free = ed_pgfree_list(&alloc);
	mu_assert_int_eq(free->count, 2);
}

int
main(void)
{
	mu_init("page");
	mu_run(test_basic);
	return 0;
}

