#include "../lib/eddy-private.h"
#include "mu.h"

// These flags aren't terribly safe to use, but they do speed up the tests.
#define FOPEN (ED_FNOTLCK|ED_FNOSYNC)
#define FCLOSE (FOPEN)
#define FRESET (FCLOSE|ED_FRESET)

static EdIdx idx;
static EdConfig cfg = {
	.index_path = "./test/tmp/test_fault",
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
	//unlink(cfg.index_path);
#if ED_MMAP_DEBUG
	mu_assert_int_eq(ed_pg_check(), 0);
#endif
}

static void
setup(EdTxn **txn)
{
	int rc;
	EdTxn *x;

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
build_tree(void)
{
	EdTxn *txn;
	setup(&txn);

	for (unsigned seed = 0, i = 0; i < 500; i += 2) {
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

	finish(&txn);
}

static void
test_commit_begin(void)
{
	unlink(cfg.index_path);

	pid_t pid = fork();
	if (pid < 0) {
		mu_fail("fork failed '%s'\n", strerror(errno));
	}
	if (pid == 0) {
		ed_fault_enable(COMMIT_BEGIN, 100, ED_FAULT_NOPRINT);
		build_tree();
		return;
	}
	while (waitpid(pid, NULL, 0) == -1 && errno == EINTR) {}

	mu_teardown = cleanup;

	Entry *ent;
	EdTxn *txn;
	setup(&txn);

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	unsigned seed = 0, i = 0;
	for (; i < 198; i++) {
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	int key = rand_r(&seed);
	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 0);

	EdStat *stat;
	mu_assert_int_eq(ed_stat_new(&stat, &idx, 0), 0);
	mu_assert(!ed_stat_has_leaks(stat));
	ed_stat_free(&stat);

	finish(&txn);
}

static void
test_active_cleared(void)
{
	unlink(cfg.index_path);

	pid_t pid = fork();
	if (pid < 0) {
		mu_fail("fork failed '%s'\n", strerror(errno));
	}
	if (pid == 0) {
		ed_fault_enable(ACTIVE_CLEARED, 100, ED_FAULT_NOPRINT);
		build_tree();
		return;
	}
	while (waitpid(pid, NULL, 0) == -1 && errno == EINTR) {}

	mu_teardown = cleanup;

	Entry *ent;
	EdTxn *txn;
	setup(&txn);

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	unsigned seed = 0, i = 0;
	for (; i < 198; i++) {
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	int key = rand_r(&seed);
	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 0);

	EdStat *stat;
	mu_assert_int_eq(ed_stat_new(&stat, &idx, 0), 0);
	mu_assert(ed_stat_has_leaks(stat));
	mu_assert_int_eq(ed_idx_repair_leaks(&idx, stat, 0), 0);
	ed_stat_free(&stat);

	mu_assert_int_eq(ed_stat_new(&stat, &idx, 0), 0);
	mu_assert(!ed_stat_has_leaks(stat));
	ed_stat_free(&stat);

	finish(&txn);
}

static void
test_update_tree(void)
{
	unlink(cfg.index_path);

	pid_t pid = fork();
	if (pid < 0) {
		mu_fail("fork failed '%s'\n", strerror(errno));
	}
	if (pid == 0) {
		ed_fault_enable(UPDATE_TREE, 100, ED_FAULT_NOPRINT);
		build_tree();
		return;
	}
	while (waitpid(pid, NULL, 0) == -1 && errno == EINTR) {}

	mu_teardown = cleanup;

	Entry *ent;
	EdTxn *txn;
	setup(&txn);

	mu_assert_int_eq(verify_tree(idx.fd, idx.hdr->tree[0], true), 0);

	unsigned seed = 0, i = 0;
	for (; i < 200; i++) {
		int key = rand_r(&seed);
		mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 1);
		char name[64];
		snprintf(name, sizeof(name), "a%u", i);
		mu_assert_int_eq(ent->key, key);
		mu_assert_str_eq(ent->name, name);
		ed_txn_close(&txn, FRESET);
	}

	int key = rand_r(&seed);
	mu_assert_int_eq(ed_txn_open(txn, ED_FRDONLY|FOPEN), 0);
	mu_assert_int_eq(ed_bpt_find(txn, 0, key, (void **)&ent), 0);

	EdStat *stat;
	mu_assert_int_eq(ed_stat_new(&stat, &idx, 0), 0);
	mu_assert(ed_stat_has_leaks(stat));
	mu_assert_int_eq(ed_idx_repair_leaks(&idx, stat, 0), 0);
	ed_stat_free(&stat);

	mu_assert_int_eq(ed_stat_new(&stat, &idx, 0), 0);
	mu_assert(!ed_stat_has_leaks(stat));
	ed_stat_free(&stat);

	finish(&txn);
}

int
main(void)
{
	mu_init("fault");

	int fd = open(cfg.slab_path, O_CREAT|O_RDWR, 0640);
	mu_assert_msg(fd >= 0, "failed to open slab: %s\n", strerror(errno));

	int rc = ed_mkfile(fd, cfg.slab_size);
	mu_assert_msg(rc >= 0, "failed to create slab: %s\n", ed_strerror(rc));

	close(fd);

	mu_run(test_commit_begin);
	mu_run(test_active_cleared);
	mu_run(test_update_tree);
}

