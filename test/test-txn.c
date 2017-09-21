#include "../lib/eddy-private.h"
#include "mu.h"
#include "rnd.c"

// These flags aren't terribly safe to use, but they do speed up the tests.
#define FOPEN (ED_FNOTLCK|ED_FNOSYNC)
#define FCLOSE (FOPEN)
#define FRESET (FCLOSE|ED_FRESET)

static EdIdx idx;
static EdConfig cfg = {
	.index_path = "./test/tmp/test_txn",
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

static int __attribute__((unused))
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
test_basic(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;
	setup(&txn);

	Entry ent;

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);

	ent.key = 10;
	snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
	mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
	mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
	mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);

	finish(&txn);
}

static void
test_no_key(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;
	setup(&txn);

	Entry ent;

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);

	ent.key = 10;
	snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
	mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), ED_EINDEX_KEY_MATCH);

	ed_txn_close(&txn, FRESET);

	finish(&txn);
}

static void
test_close(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;
	setup(&txn);

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);
	ed_txn_close(&txn, FRESET);

	finish(&txn);
}

static void
test_no_commit(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;
	setup(&txn);

	Entry ent;

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);

	ent.key = 10;
	snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
	mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
	mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);

	ed_txn_close(&txn, FRESET);

	finish(&txn);
}

static void
test_read_snapshot(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;
	setup(&txn);

	Entry ent;

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);

	ent.key = 10;
	snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
	mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
	mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
	mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);

	ent.key = 20;
	snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
	mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
	mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);

	pid_t pid = fork();
	if (pid < 0) {
		mu_fail("fork failed '%s'\n", strerror(errno));
	}
	if (pid == 0) {
		EdTxn *ftxn;
		setup(&ftxn);

		Entry *fent;

		mu_assert_int_eq(ed_txn_open(ftxn, ED_FRDONLY|FOPEN), 0);

		sleep(2);

		mu_assert_int_eq(ed_bpt_find(ftxn, 0, 10, (void **)&fent), 1);
		mu_assert_str_eq(fent->name, "a10");
		mu_assert_int_eq(ed_bpt_find(ftxn, 0, 20, (void **)&fent), 0);
		ed_txn_close(&ftxn, ED_FRESET|FCLOSE);

		mu_assert_int_eq(ed_txn_open(ftxn, ED_FRDONLY|FOPEN), 0);
		mu_assert_int_eq(ed_bpt_find(ftxn, 0, 10, (void **)&fent), 1);
		mu_assert_str_eq(fent->name, "a10");
		mu_assert_int_eq(ed_bpt_find(ftxn, 0, 20, (void **)&fent), 1);
		mu_assert_str_eq(fent->name, "a20");
		ed_txn_close(&ftxn, ED_FRESET|FCLOSE);

		finish(&ftxn);
		mu_assert_int_eq(ed_pg_check(), 0);
	}
	else {
		sleep(1);

		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);

		int status;
		mu_assert_call(waitpid(pid, &status, 0));
		mu_assert_int_eq(WEXITSTATUS(status), 0);
		mu_assert_int_eq(WTERMSIG(status), 0);

		finish(&txn);
	}
}

static void
test_write_sequence(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdTxn *txn;
	setup(&txn);

	Entry ent;

	mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);

	ent.key = 10;
	snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
	mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
	mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
	mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);

	pid_t pid = fork();
	if (pid < 0) {
		mu_fail("fork failed '%s'\n", strerror(errno));
	}
	if (pid == 0) {
		EdTxn *ftxn;
		setup(&ftxn);

		Entry *fent;

		mu_assert_int_eq(ed_txn_open(ftxn, FOPEN), 0);

		mu_assert_int_eq(ed_bpt_find(ftxn, 0, 10, (void **)&fent), 1);
		mu_assert_str_eq(fent->name, "a10");
		mu_assert_int_eq(ed_bpt_find(ftxn, 0, 20, (void **)&fent), 1);
		mu_assert_str_eq(fent->name, "a20");
		ed_txn_close(&ftxn, ED_FRESET|FCLOSE);

		finish(&ftxn);
		mu_assert_int_eq(ed_pg_check(), 0);
	}
	else {
		mu_assert_int_eq(ed_txn_open(txn, FOPEN), 0);

		sleep(1);

		ent.key = 20;
		snprintf(ent.name, sizeof(ent.name), "a%llu", ent.key);
		mu_assert_int_eq(ed_bpt_find(txn, 0, ent.key, NULL), 0);
		mu_assert_int_eq(ed_bpt_set(txn, 0, &ent, false), 0);
		mu_assert_int_eq(ed_txn_commit(&txn, FRESET), 0);

		int status;
		mu_assert_call(waitpid(pid, &status, 0));
		mu_assert_int_eq(WEXITSTATUS(status), 0);
		mu_assert_int_eq(WTERMSIG(status), 0);

		finish(&txn);
	}
}

int
main(void)
{
	mu_init("txn");

	int fd = open(cfg.slab_path, O_CREAT|O_RDWR, 0640);
	mu_assert_msg(fd >= 0, "failed to open slab: %s\n", strerror(errno));

	int rc = ed_mkfile(fd, cfg.slab_size);
	mu_assert_msg(rc >= 0, "failed to create slab: %s\n", ed_strerror(rc));

	close(fd);

	mu_run(test_basic);
	mu_run(test_no_key);
	mu_run(test_close);
	mu_run(test_no_commit);
	mu_run(test_read_snapshot);
	mu_run(test_write_sequence);
}
