/**
 * This tests the b+tree structure. We cheat a bit here and overwrite some of
 * the internals so that the tests are a bit simpler.
 */
#include "../lib/eddy-private.h"
#include "mu.h"

// Enough entries to get to depth 3.
// The entry size is bloated to get there in fewer ops.
#define LARGE 22000
#define SMALL 162
#define MULTI 5000

// These flags aren't terribly safe to use, but they do speed up the tests.
#define FOPEN (ED_FNOTLCK|ED_FNOSYNC)
#define FCLOSE (FOPEN)
#define FRESET (FCLOSE|ED_FRESET)

static EdIdx idx;
static EdConfig cfg = {
	.index_path = "./test/tmp/test_bpt",
	.slab_path = "./test/tmp/slab",
	.slab_size = 16*1024*1024,
	.flags = FOPEN|ED_FCREATE,
};

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
	int rc = ed_bpt_verify(bt, idx.fd, sizeof(Entry), stderr);
	if (tryprint) {
		print_tree(bt, idx.fd);
	}
	ed_pg_unload((EdPg **)&bt);
	return rc;
}

static void
cleanup(void)
{
	unlink(cfg.index_path);
#if ED_MMAP_DEBUG
	mu_assert_int_eq(ed_pg_check(), 0);
#endif
}

static void
setup(EdTxn **txn)
{
	int rc;
	EdTxn *x;

	int fd = open(cfg.slab_path, O_CREAT|O_RDWR, 0640);
	mu_assert_msg(fd >= 0, "failed to open slab: %s\n", strerror(errno));

	rc = ed_mkfile(fd, cfg.slab_size);
	mu_assert_msg(rc >= 0, "failed to create slab: %s\n", ed_strerror(rc));

	close(fd);

	rc = ed_idx_open(&idx, &cfg);
	mu_assert_msg(rc >= 0, "failed to open index: %s\n", ed_strerror(rc));

	rc = ed_txn_new(&x, &idx);
	mu_assert_msg(rc >= 0, "failed to create transaction: %s\n", ed_strerror(rc));

	// Testing hack. Don't do this!
	x->db[0].entry_size = sizeof(Entry);
	x->db[1].entry_size = sizeof(Entry);

	*txn = x;
}

static void
finish(EdTxn **txn)
{
	ed_txn_close(txn, FCLOSE);
	ed_idx_close(&idx);
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
	unlink(cfg.index_path);

	Entry *found = NULL;
	EdTxn *txn;

	setup(&txn);

	for (unsigned i = 1; i <= SMALL; i += 2) {
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		{
			Entry ent = { .key = i };
			snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
			mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
			mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		}
		mu_assert_uint_eq(txn->db[0].root->tree->xid, txn->xid);
		{
			Entry ent = { .key = i+1 };
			snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
			mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
			mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		}
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 1, (void **)&found), 1);
	mu_assert_uint_eq(found->key, 1);
	mu_assert_str_eq(found->name, "a1");

	finish(&txn);

	setup(&txn);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 1, (void **)&found), 1);
	mu_assert_uint_eq(found->key, 1);
	mu_assert_str_eq(found->name, "a1");
	mu_assert_int_eq(ed_bpt_find(txn, 0, 6, (void **)&found), 1);
	mu_assert_uint_eq(found->key, 6);
	mu_assert_str_eq(found->name, "a6");
	mu_assert_uint_eq(idx.hdr->xid, SMALL/2 + 1);

	finish(&txn);
}

static void
test_repeat(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	{
		Entry ent = { .key = 0, .name = "a1" };
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_ge(ed_bpt_find(txn, 0, 0, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	{
		Entry ent = { .key = 20, .name = "a2" };
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_ge(ed_bpt_find(txn, 0, 20, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	for (unsigned i = 0; i < 50; i++) {
		Entry ent = { .key = 10 };
		snprintf(ent.name, sizeof(ent.name), "b%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_ge(ed_bpt_find(txn, 0, 10, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 0, NULL), 1);
	ed_txn_close(&txn, FRESET);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 20, NULL), 1);
	ed_txn_close(&txn, FRESET);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 10, NULL), 1);
	for (unsigned i = 1; i < 50; i++) {
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

	finish(&txn);
}

static void
test_large(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	for (unsigned seed = 0, i = 0; i < LARGE; i += 2) {
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		{
			Entry ent = { .key = rand_r(&seed) };
			snprintf(ent.name, sizeof(ent.name), "a%u", i);
			mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
			mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		}
		mu_assert_uint_eq(txn->db[0].root->tree->xid, txn->xid);
		{
			Entry ent = { .key = rand_r(&seed) };
			snprintf(ent.name, sizeof(ent.name), "a%u", i+1);
			mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
			mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		}
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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

	finish(&txn);
}

static void
test_large_sequential(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	for (unsigned i = 0; i < LARGE; i++) {
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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

	finish(&txn);
}

static void
test_large_sequential_reverse(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	for (unsigned i = LARGE; i > 0; i--) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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

	finish(&txn);
}

static void
test_split_leaf_middle_left(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	size_t n = ed_bpt_capacity(sizeof(Entry), 1);
	size_t mid = (n / 2) - 1;
	for (size_t i = 0; i <= n; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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

	finish(&txn);
}

static void
test_split_leaf_middle_right(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	size_t n = ed_bpt_capacity(sizeof(Entry), 1);
	size_t mid = n / 2;
	for (size_t i = 0; i <= n; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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

	finish(&txn);
}

static void
test_split_branch_left0(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	// The leaf order is odd, so we'll so this funky business to get a full tree
	int branch_order = ed_branch_order();
	int leaf_order = ed_leaf_order(sizeof(Entry)) - 1;
	int n = branch_order * leaf_order;
	int final = 0;
	for (int i = 0; i < n; i += 2) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}
	for (int i = n-1; i > 0; i -= 2) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "b%llu", ent.key);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}
	for (int i = 0; i < n; i += leaf_order) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "c%llu", ent.key);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 1);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}
	{
		Entry ent = { .key = final };
		snprintf(ent.name, sizeof(ent.name), "d%llu", ent.key);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 1);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	for (int i = 0; i < n; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, i, (void **)&ent), 1);
		char name[64], ch;
		if (i == final) { ch = 'd'; }
		else if (i % leaf_order == 0) { ch = 'c'; }
		else if (i % 2) { ch = 'b'; }
		else { ch = 'a'; }
		snprintf(name, sizeof(name), "%c%d", ch, i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	finish(&txn);
}

static void
test_split_branch_right0(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	// The leaf order is odd, so we'll so this funky business to get a full tree
	int branch_order = ed_branch_order();
	int leaf_order = ed_leaf_order(sizeof(Entry)) - 1;
	int n = branch_order * leaf_order;
	int final = (branch_order/2) * leaf_order;
	for (int i = 0; i < n; i += 2) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}
	for (int i = n-1; i > 0; i -= 2) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "b%llu", ent.key);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}
	for (int i = 0; i < n; i += leaf_order) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "c%llu", ent.key);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 1);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}
	{
		Entry ent = { .key = final };
		snprintf(ent.name, sizeof(ent.name), "d%llu", ent.key);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 1);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	for (int i = 0; i < n; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, i, (void **)&ent), 1);
		char name[64], ch;
		if (i == final) { ch = 'd'; }
		else if (i % leaf_order == 0) { ch = 'c'; }
		else if (i % 2) { ch = 'b'; }
		else { ch = 'a'; }
		snprintf(name, sizeof(name), "%c%d", ch, i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	finish(&txn);
}

static void
test_split_middle_branch(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	size_t mid = LARGE / 2;
	for (size_t i = 0; i <= LARGE; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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

	finish(&txn);
}

static void
test_remove_small(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], false), 0);

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
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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

	finish(&txn);
}

static void
test_remove_large(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], false), 0);

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

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

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

	finish(&txn);
}

static void
test_multi(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	for (unsigned seed = 0, i = 0; i < MULTI; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		if (i % 3 == 0) {
			Entry ent2 = { .key = rand_r(&seed) };
			snprintf(ent2.name, sizeof(ent2.name), "b%u", i);
			mu_assert_int_eq(ed_bpt_find(txn, 1, ent2.key, NULL), 0);
			mu_assert_int_eq(ed_bpt_set(txn, 1, &ent2, false), 0);
		}
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);
	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[1], true), 0);

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

	finish(&txn);
}

static void
test_iter(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;
	uint64_t start, end = 0;

	setup(&txn);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry ent = { .key = rand_r(&seed) };
		if (i == LARGE/3) { start = ent.key; }
		else if (ent.key > end) { end = ent.key; }
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	Entry *ent;

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, start, (void **)&ent), 1);
	mu_assert_uint_eq(ent->key, start);

	uint64_t last = 0;
	int c;
	for (c = 0; ed_bpt_loop(txn, 0) == 0; c++) {
		mu_assert_int_eq(ed_bpt_next(txn, 0, (void **)&ent), 0);
		mu_assert_uint_lt(last, ent->key);
		last = ent->key;
		if (last == end) { last = 0; }
	}
	mu_assert_int_eq(c, LARGE);

	finish(&txn);
}

static void
test_iter_del(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;

	setup(&txn);

	for (unsigned seed = 0, i = 0; i < 200; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	Entry *ent;
	uint64_t keys[4];

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, 1002898451, (void **)&ent), 1);

	keys[0] = txn->db[0].key;
	mu_assert_int_eq(ed_bpt_del(txn, 0), 1);
	keys[1] = txn->db[0].key;
	mu_assert_int_eq(ed_bpt_del(txn, 0), 1);
	keys[2] = txn->db[0].key;
	mu_assert_int_eq(ed_bpt_del(txn, 0), 1);
	keys[3] = txn->db[0].key;
	mu_assert_int_eq(ed_bpt_del(txn, 0), 1);

	mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, keys[0], (void **)&ent), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, keys[1], (void **)&ent), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, keys[2], (void **)&ent), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, keys[3], (void **)&ent), 0);
	ed_txn_close(&txn, FRESET);

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
	mu_assert_int_eq(ed_bpt_first(txn, 0, (void **)&ent), 0);
	keys[0] = ent->key;
	mu_assert_int_eq(ed_bpt_next(txn, 0, (void **)&ent), 0);
	keys[1] = ent->key;
	mu_assert_int_eq(ed_bpt_next(txn, 0, (void **)&ent), 0);
	keys[2] = ent->key;
	mu_assert_int_eq(ed_bpt_next(txn, 0, (void **)&ent), 0);
	keys[3] = ent->key;
	mu_assert_int_eq(ed_bpt_first(txn, 0, NULL), 0);
	mu_assert_int_eq(ed_bpt_del(txn, 0), 1);
	mu_assert_int_eq(ed_bpt_del(txn, 0), 1);
	mu_assert_int_eq(ed_bpt_del(txn, 0), 1);
	mu_assert_int_eq(ed_bpt_del(txn, 0), 1);

	mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, keys[0], (void **)&ent), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, keys[1], (void **)&ent), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, keys[2], (void **)&ent), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, keys[3], (void **)&ent), 0);
	ed_txn_close(&txn, FRESET);

	finish(&txn);
}

static int
compare_key(const void *a, const void *b)
{
	if (*(uint64_t *)a < *(uint64_t *)b) {
		return -1;
	}
	if (*(uint64_t *)a > *(uint64_t *)b) {
		return 1;
	}
	return 0;
}

static void
test_key_range(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;
	setup(&txn);

	uint64_t keys[200], kmin = 0;

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
	for (unsigned seed = 0, i = 0; i < 200; i++) {
		Entry ent = { .key = rand_r(&seed) % 1000000 };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		keys[i] = ent.key;
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
	}
	mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	qsort(keys, ed_len(keys), sizeof(keys[0]), compare_key);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	for (unsigned i = 0; i < ed_len(keys); i++) {
		uint64_t kmax = keys[i];
		mu_assert_int_eq(ed_bpt_find(txn, 0, keys[i], NULL), 1);
		mu_assert_uint_ge(txn->db[0].kmin, kmin);
		mu_assert_uint_le(txn->db[0].kmin, keys[i]);
		mu_assert_uint_eq(txn->db[0].kmax, kmax);
		kmin = kmax;
	}
	ed_txn_close(&txn, FRESET);

	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_first(txn, 0, NULL), 0);
	kmin = 0;
	for (unsigned i = 0; i < ed_len(keys); i++) {
		mu_assert_uint_eq(ed_bpt_loop(txn, 0), 0);
		uint64_t kmax = keys[i];
		mu_assert_uint_ge(txn->db[0].kmin, kmin);
		mu_assert_uint_le(txn->db[0].kmin, keys[i]);
		mu_assert_uint_eq(txn->db[0].kmax, kmax);
		kmin = kmax;
		mu_assert_int_eq(ed_bpt_next(txn, 0, NULL), 0);
	}
	ed_txn_close(&txn, FRESET);

	finish(&txn);
}

int
main(void)
{
	mu_init("b+tree");
	mu_run(test_capacity);
	mu_run(test_basic);
	mu_run(test_repeat);
	mu_run(test_large);
	mu_run(test_large_sequential);
	mu_run(test_large_sequential_reverse);
	mu_run(test_split_leaf_middle_left);
	mu_run(test_split_leaf_middle_right);
	mu_run(test_split_branch_left0);
	mu_run(test_split_branch_right0);
	mu_run(test_split_middle_branch);
	mu_run(test_remove_small);
	mu_run(test_remove_large);
	mu_run(test_multi);
	mu_run(test_iter);
	mu_run(test_iter_del);
	mu_run(test_key_range);
	return 0;
}

