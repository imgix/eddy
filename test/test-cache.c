#include "../lib/eddy-private.h"
#include "mu.h"

static EdConfig cfg = {
	.index_path = "./test/tmp/test_cache",
	.slab_path = "./test/tmp/slab",
	.slab_size = 16*1024*1024,
	.flags = ED_FNOSYNC|ED_FCREATE|ED_FALLOCATE|ED_FCHECKSUM|ED_FZERO,
};

static void
cleanup(void)
{
	//unlink(cfg.index_path);
#if ED_MMAP_DEBUG
	mu_assert_int_eq(ed_pg_check(), 0);
#endif
}

static void
test_create(void)
{
	mu_teardown = cleanup;
	unlink(cfg.index_path);

	EdCache *cache = NULL;
	int rc = ed_cache_open(&cache, &cfg);
	mu_assert_msg(rc >= 0, "failed to open cache: %s\n", ed_strerror(rc));

	int count = ('~' - '!') + 1;

	EdObject *obj = NULL;
	EdObjectAttr attr = {
		.object_size = count * 1000,
		.key = "foo",
		.key_size = 3,
		.meta_size = 0,
		.ttl = -1
	};

	mu_assert_int_eq(ed_create(cache, &obj, &attr), 0);
	for (int i = '!' ; i <= '~'; i++) {
		char buf[1000];
		memset(buf, i, sizeof(buf));
		mu_assert_int_eq(ed_write(obj, buf, sizeof(buf)), sizeof(buf));
	}
	ed_close(&obj);

	ed_cache_close(&cache);
}

int
main(void)
{
	mu_init("cache");

	mu_run(test_create);
}

