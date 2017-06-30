#include "eddy-private.h"
#include "mu.h"

static EdPgalloc alloc;
static const char *path = "/tmp/eddy_test_btree";

typedef struct {
	EdPgno head;
	uint32_t pad;
	EdPgallocHdr alloc;
} Tree;

typedef struct {
	uint64_t key;
	char name[16];
} Entry;

static void
cleanup(void)
{
	ed_pgalloc_close(&alloc);
	unlink(path);
#if ED_MMAP_DEBUG
	mu_assert_int_eq(ed_pgcheck(), 0);
#endif
}

static void
test_basic(void)
{
	mu_teardown = cleanup;

	Entry *found = NULL;
	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;
	mu_assert_ptr_ne(ed_pgload(alloc.fd, (EdPg **)&bt, t->head), MAP_FAILED);
	mu_assert_ptr_eq(bt, NULL);

	for (int i = 1; i <= 10; i++) {
		Entry ent = { i, "a1" };
		sprintf(ent.name, "a%d", i);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}
	mu_assert_ptr_ne(bt, NULL);
	mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, 1, sizeof(Entry), &srch), 1);

	ed_pgmark(&bt->base, &t->head, &alloc.dirty);
	ed_pgalloc_sync(&alloc);

	found = srch.entry;
	mu_assert_uint_eq(found->key, 1);
	mu_assert_str_eq(found->name, "a1");
	mu_assert_ptr_eq(srch.entry, bt->data);

	ed_pgunmap(bt, 1);
	bt = NULL;
	ed_pgalloc_close(&alloc);

	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	mu_assert_ptr_ne(ed_pgload(alloc.fd, (EdPg **)&bt, t->head), MAP_FAILED);
	mu_assert_ptr_ne(bt, NULL);

	mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, 1, sizeof(Entry), &srch), 1);
	ed_bsearch_final(&srch);

	found = srch.entry;
	mu_assert_uint_eq(found->key, 1);
	mu_assert_str_eq(found->name, "a1");
	ed_pgunmap(bt, 1);
}

static void
test_split(void)
{
	mu_teardown = cleanup;

	//Entry *found = NULL;
	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	for (unsigned seed = 0, i = 0; i < 10000; i++) {
		int k = rand_r(&seed);
		Entry ent = { .key = k };
		sprintf(ent.name, "a%u", k);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	for (unsigned seed = 0, i = 0; i < 10000; i++) {
		int k = rand_r(&seed);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 1);
		ed_bsearch_final(&srch);
	}

	ed_pgunmap(bt, 1);
}

int
main(void)
{
	mu_init("btree");
	mu_run(test_basic);
	mu_run(test_split);
	return 0;
}

