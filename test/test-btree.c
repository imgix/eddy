#include "eddy-private.h"
#include "mu.h"

// Enough entries to get to depth 3.
// The entry size is bloated to get there in fewer ops.
#define LARGE 22000
#define SMALL 10

static EdPgalloc alloc;
static const char *path = "/tmp/eddy_test_btree";

typedef struct {
	EdPgno head;
	uint32_t pad;
	EdPgallocHdr alloc;
} Tree;

typedef struct {
	uint64_t key;
	char name[56];
} Entry;

static void __attribute__((unused))
print_entry(const void *ent, char *buf, size_t len)
{
	const Entry *e = ent;
	snprintf(buf, len, "%2llu:%s", e->key, e->name);
}

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
test_capacity(void)
{
	mu_assert_uint_eq(ed_btree_capacity(sizeof(Entry), 1),         63);
	mu_assert_uint_eq(ed_btree_capacity(sizeof(Entry), 2),      21420);
	mu_assert_uint_eq(ed_btree_capacity(sizeof(Entry), 3),    7282800);
	mu_assert_uint_eq(ed_btree_capacity(sizeof(Entry), 4), 2476152000);
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

	for (int i = 1; i <= SMALL; i++) {
		Entry ent = { i, "a1" };
		snprintf(ent.name, sizeof(ent.name), "a%d", i);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}
	mu_assert_ptr_ne(bt, NULL);
	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);
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
test_repeat(void)
{
	mu_teardown = cleanup;

	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	{
		Entry ent = { .key = 0, .name = "a1" };
		mu_assert_int_ge(ed_btree_search(&bt, alloc.fd, 0, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
	}

	{
		Entry ent = { .key = 20, .name = "a2" };
		mu_assert_int_ge(ed_btree_search(&bt, alloc.fd, 20, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
	}

	for (unsigned i = 0; i < 200; i++) {
		Entry ent = { .key = 10 };
		snprintf(ent.name, sizeof(ent.name), "b%u", i);
		mu_assert_int_ge(ed_btree_search(&bt, alloc.fd, 10, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);
	mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, 0, sizeof(Entry), &srch), 1);
	ed_bsearch_final(&srch);
	mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, 20, sizeof(Entry), &srch), 1);
	ed_bsearch_final(&srch);

	mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, 10, sizeof(Entry), &srch), 1);
	for (unsigned i = 1; i < 200; i++) {
		mu_assert_int_eq(ed_bsearch_next(&srch), 1);
	}
	ed_bsearch_final(&srch);

	ed_pgunmap(bt, 1);
}

static void
test_large(void)
{
	mu_teardown = cleanup;

	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		int k = rand_r(&seed);
		Entry ent = { .key = k };
		snprintf(ent.name, sizeof(ent.name), "a%u", k);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		int k = rand_r(&seed);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 1);
		ed_bsearch_final(&srch);
	}

	ed_pgunmap(bt, 1);
}

static void
test_large_sequential(void)
{
	mu_teardown = cleanup;

	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	for (unsigned i = 0; i < LARGE; i++) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (unsigned i = 0; i < LARGE; i++) {
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 1);
		ed_bsearch_final(&srch);
	}

	ed_pgunmap(bt, 1);
}

static void
test_large_sequential_reverse(void)
{
	mu_teardown = cleanup;

	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	for (unsigned i = LARGE; i > 0; i--) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (unsigned i = LARGE; i > 0; i--) {
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 1);
		ed_bsearch_final(&srch);
	}

	ed_pgunmap(bt, 1);
}

static void
test_split_leaf_middle_left(void)
{
	mu_teardown = cleanup;

	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	size_t n = ed_btree_capacity(sizeof(Entry), 1);
	size_t mid = (n / 2) - 1;
	for (size_t i = 0; i <= n; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, mid, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (size_t i = 0; i <= n; i++) {
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 1);
		ed_bsearch_final(&srch);
	}

	ed_pgunmap(bt, 1);
}

static void
test_split_leaf_middle_right(void)
{
	mu_teardown = cleanup;

	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	size_t n = ed_btree_capacity(sizeof(Entry), 1);
	size_t mid = n / 2;
	for (size_t i = 0; i <= n; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, mid, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (size_t i = 0; i <= n; i++) {
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 1);
		ed_bsearch_final(&srch);
	}

	ed_pgunmap(bt, 1);
}

static void
test_split_middle_branch(void)
{
	mu_teardown = cleanup;

	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	size_t mid = LARGE / 2;
	for (size_t i = 0; i <= LARGE; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, mid, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (size_t i = 0; i <= LARGE; i++) {
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, i, sizeof(Entry), &srch), 1);
		ed_bsearch_final(&srch);
	}

	ed_pgunmap(bt, 1);
}

static void
test_remove_small(void)
{
	mu_teardown = cleanup;

	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		int k = rand_r(&seed);
		Entry ent = { .key = k };
		snprintf(ent.name, sizeof(ent.name), "a%u", k);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		int k = rand_r(&seed);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 1);
		mu_assert_int_eq(ed_bsearch_del(&srch), 1);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		int k = rand_r(&seed);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 0);
		ed_bsearch_final(&srch);
	}

	for (unsigned seed = 1, i = 0; i < SMALL; i++) {
		int k = rand_r(&seed);
		Entry ent = { .key = k };
		snprintf(ent.name, sizeof(ent.name), "a%u", k);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (unsigned seed = 1, i = 0; i < SMALL; i++) {
		int k = rand_r(&seed);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 1);
		ed_bsearch_final(&srch);
	}

	ed_pgunmap(bt, 1);
}

static void
test_remove_large(void)
{
	mu_teardown = cleanup;

	Tree *t = NULL;
	EdBTree *bt = NULL;
	EdBSearch srch;

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree)), 0);

	t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		int k = rand_r(&seed);
		Entry ent = { .key = k };
		snprintf(ent.name, sizeof(ent.name), "a%u", k);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		int k = rand_r(&seed);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 1);
		mu_assert_int_eq(ed_bsearch_del(&srch), 1);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		int k = rand_r(&seed);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 0);
		ed_bsearch_final(&srch);
	}

	for (unsigned seed = 1, i = 0; i < LARGE; i++) {
		int k = rand_r(&seed);
		Entry ent = { .key = k };
		snprintf(ent.name, sizeof(ent.name), "a%u", k);
		mu_assert_int_eq(ed_btree_search(&bt, alloc.fd, k, sizeof(Entry), &srch), 0);
		mu_assert_int_eq(ed_bsearch_ins(&srch, &ent, &alloc), 0);
		ed_bsearch_final(&srch);
	}

	mu_assert_int_eq(ed_btree_verify(bt, alloc.fd, sizeof(Entry), stderr), 0);

	for (unsigned seed = 1, i = 0; i < LARGE; i++) {
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
	mu_run(test_capacity);
	mu_run(test_basic);
	mu_run(test_repeat);
	mu_run(test_large);
	mu_run(test_large_sequential);
	mu_run(test_large_sequential_reverse);
	mu_run(test_split_leaf_middle_left);
	mu_run(test_split_leaf_middle_right);
	mu_run(test_split_middle_branch);
	mu_run(test_remove_small);
	mu_run(test_remove_large);
	return 0;
}

