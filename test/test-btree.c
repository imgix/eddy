#include "../lib/eddy-private.h"
#include "mu.h"

// Enough entries to get to depth 3.
// The entry size is bloated to get there in fewer ops.
#define LARGE 22000
#define SMALL 12
#define MULTI 5000

// These flags aren't terribly safe to use, but they do speed up the tests.
#define FOPEN (ED_FNOTLCK|ED_FNOFLCK)
#define FCLOSE (FOPEN|ED_FNOSYNC)
#define FRESET (FCLOSE|ED_FRESET)

static EdPgAlloc alloc;
static const char *path = "/tmp/eddy_test_btree";

typedef struct {
	EdPgno db1, db2;
} Tree;

typedef struct {
	uint64_t key;
	char name[56];
} Entry;

static int
print_entry(const void *ent, char *buf, size_t len)
{
	const Entry *e = ent;
	return snprintf(buf, len, "%11llu  %s", e->key, e->name);
}

static void
print_tree(EdBpt *bt, int fd)
{
	char *p = getenv("PRINT");
	if (p && strcmp(p, "1") == 0) {
		ed_bpt_print(bt, fd, sizeof(Entry), stdout, print_entry);
	}
}

static int
verify_tree(int fd, EdPgno no, bool tryprint)
{
	char *p = getenv("NOVERIFY");
	if (p && strcmp(p, "1") == 0) { return 0; }

	EdBpt *bt = NULL;
	if (ed_pg_load(fd, (EdPg **)&bt, no) == MAP_FAILED) {
		return ED_ERRNO;
	}
	int rc = ed_bpt_verify(bt, alloc.fd, sizeof(Entry), stderr);
	if (rc == 0 && tryprint) {
		print_tree(bt, alloc.fd);
	}
	ed_pg_unload((EdPg **)&bt);
	return rc;
}

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
test_capacity(void)
{
	mu_assert_uint_eq(ed_bpt_capacity(sizeof(Entry), 1),         63);
	mu_assert_uint_eq(ed_bpt_capacity(sizeof(Entry), 2),      21420);
	mu_assert_uint_eq(ed_bpt_capacity(sizeof(Entry), 3),    7282800);
	mu_assert_uint_eq(ed_bpt_capacity(sizeof(Entry), 4), 2476152000);
}

static void
test_basic(void)
{
	mu_teardown = cleanup;

	Entry *found = NULL;
	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned i = 1; i <= SMALL; i++) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 1, (void **)&found), 1);
	mu_assert_uint_eq(found->key, 1);
	mu_assert_str_eq(found->name, "a1");
	ed_txn_close(&txn, FCLOSE);

	alloc.dirty = 1;
	ed_pg_alloc_close(&alloc);

	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	t = ed_pg_alloc_meta(&alloc);
	type[0].no = &t->db1;

	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);
	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 1, (void **)&found), 1);
	mu_assert_uint_eq(found->key, 1);
	mu_assert_str_eq(found->name, "a1");
	ed_txn_close(&txn, FCLOSE);
}

static void
test_repeat(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	{
		Entry ent = { .key = 0, .name = "a1" };
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_ge(ed_bpt_find(txn, 0, 0, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	{
		Entry ent = { .key = 20, .name = "a2" };
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_ge(ed_bpt_find(txn, 0, 20, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	for (unsigned i = 0; i < 200; i++) {
		Entry ent = { .key = 10 };
		snprintf(ent.name, sizeof(ent.name), "b%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_ge(ed_bpt_find(txn, 0, 10, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 0, NULL), 1);
	ed_txn_close(&txn, FRESET);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 20, NULL), 1);
	ed_txn_close(&txn, FRESET);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 10, NULL), 1);
	for (unsigned i = 1; i < 200; i++) {
		mu_assert_int_eq(ed_bpt_next(txn, 0, NULL), 1);
	}

	Entry *ent;

	ent = NULL;
	mu_assert_int_eq(ed_bpt_next(txn, 0, (void **)&ent), 0);
	mu_assert_ptr_ne(ent, NULL);
	mu_assert_uint_eq(ent->key, 20);

	ent = NULL;
	mu_assert_int_eq(ed_bpt_next(txn, 0, (void **)&ent), 0);
	mu_assert_ptr_ne(ent, NULL);
	mu_assert_uint_eq(ent->key, 0);

	ent = NULL;
	mu_assert_int_eq(ed_bpt_next(txn, 0, (void **)&ent), 0);
	mu_assert_ptr_ne(ent, NULL);
	mu_assert_uint_eq(ent->key, 10); // the key was implicitly dropped by traversing the cursor

	ed_txn_close(&txn, FCLOSE);
}

static void
test_large(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	ed_txn_close(&txn, FCLOSE);
}

static void
test_large_sequential(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned i = 0; i < LARGE; i++) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (unsigned i = 0; i < LARGE; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	ed_txn_close(&txn, FCLOSE);
}

static void
test_large_sequential_reverse(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned i = LARGE; i > 0; i--) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (unsigned i = LARGE; i > 0; i--) {
		Entry *ent;
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	ed_txn_close(&txn, FCLOSE);
}

static void
test_split_leaf_middle_left(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	size_t n = ed_bpt_capacity(sizeof(Entry), 1);
	size_t mid = (n / 2) - 1;
	for (size_t i = 0; i <= n; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (size_t i = 0; i <= n; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%zu", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	ed_txn_close(&txn, FCLOSE);
}

static void
test_split_leaf_middle_right(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	size_t n = ed_bpt_capacity(sizeof(Entry), 1);
	size_t mid = n / 2;
	for (size_t i = 0; i <= n; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (size_t i = 0; i <= n; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%zu", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	ed_txn_close(&txn, FCLOSE);
}

static void
test_split_middle_branch(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	size_t mid = LARGE / 2;
	for (size_t i = 0; i <= LARGE; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (size_t i = 0; i <= LARGE; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%zu", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	ed_txn_close(&txn, FCLOSE);
}

static void
test_remove_small(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		mu_assert_int_eq(ed_bpt_del(txn, 0), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, false), 0);

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, NULL), 0);
		ed_txn_close(&txn, FRESET);
	}

	for (unsigned seed = 1, i = 0; i < SMALL; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "b%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (unsigned seed = 1, i = 0; i < SMALL; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "b%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	ed_txn_close(&txn, FCLOSE);
}

static void
test_remove_large(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;

	EdTxnType type[] = { { &t->db1, sizeof(Entry) } };
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, false), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		mu_assert_int_eq(ed_bpt_del(txn, 0), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, NULL), 0);
		ed_txn_close(&txn, FRESET);
	}

	for (unsigned seed = 1, i = 0; i < LARGE; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);

	for (unsigned seed = 1, i = 0; i < LARGE; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	ed_txn_close(&txn, FCLOSE);
}

static void
test_multi(void)
{
	mu_teardown = cleanup;

	EdLck lock;
	ed_lck_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pg_alloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pg_alloc_meta(&alloc);
	t->db1 = ED_PG_NONE;
	t->db2 = ED_PG_NONE;

	EdTxnType type[] = {
		{ &t->db1, sizeof(Entry) },
		{ &t->db2, sizeof(Entry) },
	};
	EdTxn *txn;
	mu_assert_int_eq(ed_txn_new(&txn, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned seed = 0, i = 0; i < MULTI; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 1);
		if (i % 3 == 0) {
			Entry ent2 = { .key = rand_r(&seed) };
			snprintf(ent2.name, sizeof(ent2.name), "b%u", i);
			mu_assert_int_eq(ed_bpt_find(txn, 1, ent2.key, NULL), 0);
			mu_assert_int_eq(ed_bpt_set(txn, 1, &ent2, false), 1);
		}
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->db1, true), 0);
	mu_assert_int_eq(verify_tree(alloc.fd, t->db2, true), 0);

	for (unsigned seed = 0, i = 0; i < MULTI; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		if (i % 3 == 0) {
			int key2 = rand_r(&seed);
			mu_assert_int_eq(ed_bpt_find(txn, 1, key2, (void **)&ent), 1);
			char name2[64];
			snprintf(name2, sizeof(name2), "b%u", i);
			mu_assert_int_eq(ent->key, key2);
			mu_assert_str_eq(ent->name, name2);
		}
		ed_txn_close(&txn, FRESET);
	}

	ed_txn_close(&txn, FCLOSE);
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
	mu_run(test_multi);
	return 0;
}

