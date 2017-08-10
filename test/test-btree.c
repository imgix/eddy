#include "eddy-private.h"
#include "mu.h"

// Enough entries to get to depth 3.
// The entry size is bloated to get there in fewer ops.
#define LARGE 22000
#define SMALL 12

#define FOPEN (ED_FNOTLOCK|ED_FNOFLOCK)
#define FCLOSE (FOPEN|ED_FNOSYNC)
#define FRESET (FCLOSE|ED_FRESET)

static EdPgAlloc alloc;
static const char *path = "/tmp/eddy_test_btree";

typedef struct {
	EdPgno head;
	uint32_t pad;
	EdPgAllocHdr alloc;
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
print_tree(EdBTree *bt, int fd)
{
	char *p = getenv("PRINT");
	if (p && strcmp(p, "1") == 0) {
		ed_btprint(bt, fd, sizeof(Entry), stdout, print_entry);
	}
}

static int
verify_tree(int fd, EdPgno no, bool tryprint)
{
	char *p = getenv("NOVERIFY");
	if (p && strcmp(p, "1") == 0) { return 0; }

	EdBTree *bt = NULL;
	if (ed_pgload(fd, (EdPg **)&bt, no) == MAP_FAILED) {
		return ED_ERRNO;
	}
	int rc = ed_btverify(bt, alloc.fd, sizeof(Entry), stderr);
	if (rc == 0 && tryprint) {
		print_tree(bt, alloc.fd);
	}
	ed_pgunload((EdPg **)&bt);
	return rc;
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
	mu_assert_uint_eq(ed_btcapacity(sizeof(Entry), 1),         63);
	mu_assert_uint_eq(ed_btcapacity(sizeof(Entry), 2),      21420);
	mu_assert_uint_eq(ed_btcapacity(sizeof(Entry), 3),    7282800);
	mu_assert_uint_eq(ed_btcapacity(sizeof(Entry), 4), 2476152000);
}

static void
test_basic(void)
{
	mu_teardown = cleanup;

	Entry *found = NULL;
	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned i = 1; i <= SMALL; i++) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
	mu_assert_int_eq(ed_btfind(tx, 0, 1, (void **)&found), 1);
	mu_assert_uint_eq(found->key, 1);
	mu_assert_str_eq(found->name, "a1");
	ed_txclose(&tx, FCLOSE);

	alloc.dirty = 1;
	ed_pgalloc_close(&alloc);

	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	t = ed_pgalloc_meta(&alloc);
	type[0].no = &t->head;

	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);
	mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
	mu_assert_int_eq(ed_btfind(tx, 0, 1, (void **)&found), 1);
	mu_assert_uint_eq(found->key, 1);
	mu_assert_str_eq(found->name, "a1");
	ed_txclose(&tx, FCLOSE);
}

static void
test_repeat(void)
{
	mu_teardown = cleanup;

	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	{
		Entry ent = { .key = 0, .name = "a1" };
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_ge(ed_btfind(tx, 0, 0, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	{
		Entry ent = { .key = 20, .name = "a2" };
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_ge(ed_btfind(tx, 0, 20, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	for (unsigned i = 0; i < 200; i++) {
		Entry ent = { .key = 10 };
		snprintf(ent.name, sizeof(ent.name), "b%u", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_ge(ed_btfind(tx, 0, 10, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
	mu_assert_int_eq(ed_btfind(tx, 0, 0, NULL), 1);
	ed_txclose(&tx, FRESET);

	mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
	mu_assert_int_eq(ed_btfind(tx, 0, 20, NULL), 1);
	ed_txclose(&tx, FRESET);

	mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
	mu_assert_int_eq(ed_btfind(tx, 0, 10, NULL), 1);
	for (unsigned i = 1; i < 200; i++) {
		mu_assert_int_eq(ed_btnext(tx, 0, NULL), 1);
	}
	ed_txclose(&tx, FCLOSE);
}

static void
test_large(void)
{
	mu_teardown = cleanup;

	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		ed_txclose(&tx, FRESET);
	}

	ed_txclose(&tx, FCLOSE);
}

static void
test_large_sequential(void)
{
	mu_teardown = cleanup;

	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned i = 0; i < LARGE; i++) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (unsigned i = 0; i < LARGE; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txclose(&tx, FRESET);
	}

	ed_txclose(&tx, FCLOSE);
}

static void
test_large_sequential_reverse(void)
{
	mu_teardown = cleanup;

	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned i = LARGE; i > 0; i--) {
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (unsigned i = LARGE; i > 0; i--) {
		Entry *ent;
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txclose(&tx, FRESET);
	}

	ed_txclose(&tx, FCLOSE);
}

static void
test_split_leaf_middle_left(void)
{
	mu_teardown = cleanup;

	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	size_t n = ed_btcapacity(sizeof(Entry), 1);
	size_t mid = (n / 2) - 1;
	for (size_t i = 0; i <= n; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (size_t i = 0; i <= n; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%zu", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txclose(&tx, FRESET);
	}

	ed_txclose(&tx, FCLOSE);
}

static void
test_split_leaf_middle_right(void)
{
	mu_teardown = cleanup;

	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	size_t n = ed_btcapacity(sizeof(Entry), 1);
	size_t mid = n / 2;
	for (size_t i = 0; i <= n; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (size_t i = 0; i <= n; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%zu", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txclose(&tx, FRESET);
	}

	ed_txclose(&tx, FCLOSE);
}

static void
test_split_middle_branch(void)
{
	mu_teardown = cleanup;

	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	size_t mid = LARGE / 2;
	for (size_t i = 0; i <= LARGE; i++) {
		if (i == mid) { i++; }
		Entry ent = { .key = i };
		snprintf(ent.name, sizeof(ent.name), "a%zu", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	{
		Entry ent = { .key = mid };
		snprintf(ent.name, sizeof(ent.name), "a%zu", mid);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (size_t i = 0; i <= LARGE; i++) {
		Entry *ent;
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, i, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%zu", i);
		mu_assert_int_eq(ent->key, i);
		mu_assert_str_eq(ent->name, name);
		ed_txclose(&tx, FRESET);
	}

	ed_txclose(&tx, FCLOSE);
}

static void
test_remove_small(void)
{
	mu_teardown = cleanup;

	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		mu_assert_int_eq(ed_btdel(tx, 0), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, false), 0);

	for (unsigned seed = 0, i = 0; i < SMALL; i++) {
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, key, NULL), 0);
		ed_txclose(&tx, FRESET);
	}

	for (unsigned seed = 1, i = 0; i < SMALL; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "b%u", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (unsigned seed = 1, i = 0; i < SMALL; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "b%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		ed_txclose(&tx, FRESET);
	}

	ed_txclose(&tx, FCLOSE);
}

static void
test_remove_large(void)
{
	mu_teardown = cleanup;

	EdLock lock;
	ed_lock_init(&lock, 0, PAGESIZE);

	unlink(path);
	mu_assert_int_eq(ed_pgalloc_new(&alloc, path, sizeof(Tree), ED_FNOSYNC), 0);

	Tree *t = ed_pgalloc_meta(&alloc);
	t->head = ED_PAGE_NONE;

	EdTxType type[] = { { &t->head, sizeof(Entry) } };
	EdTx *tx;
	mu_assert_int_eq(ed_txnew(&tx, &alloc, &lock, type, ed_len(type)), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, false), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		mu_assert_int_eq(ed_btdel(tx, 0), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (unsigned seed = 0, i = 0; i < LARGE; i++) {
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, key, NULL), 0);
		ed_txclose(&tx, FRESET);
	}

	for (unsigned seed = 1, i = 0; i < LARGE; i++) {
		Entry ent = { .key = rand_r(&seed) };
		snprintf(ent.name, sizeof(ent.name), "a%u", i);
		mu_assert_int_eq(ed_txopen(tx, false, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_btset(tx, 0, &ent, false), 1);
		mu_assert_int_eq(ed_txcommit(&tx, FRESET), 0);
	}

	mu_assert_int_eq(verify_tree(alloc.fd, t->head, true), 0);

	for (unsigned seed = 1, i = 0; i < LARGE; i++) {
		Entry *ent;
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txopen(tx, true, FOPEN), 0);
		mu_assert_int_eq(ed_btfind(tx, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		ed_txclose(&tx, FRESET);
	}

	ed_txclose(&tx, FCLOSE);
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

