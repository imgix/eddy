#include "../lib/eddy-private.h"
#include "mu.h"

#define SP_TEST_ROOT PROJECT_SOURCE_DIR "/test"
#define SP_TEST_DIR_ROOT SP_TEST_ROOT "/dir"

#define TEST_JOIN(base, rel, exp) do { \
	char buf[1024]; \
	int rc = ed_path_join(buf, sizeof(buf), \
			base, sizeof(base) - 1, \
			rel, sizeof(rel) - 1); \
	mu_assert_int_gt(rc, 0); \
	mu_assert_str_eq(buf, exp); \
} while (0)

#define TEST_CLEAN(path, exp) do { \
	char buf[4096] = { 0 }; \
	memcpy(buf, path, sizeof path); \
	uint16_t rc = ed_path_clean(buf, sizeof path - 1); \
	mu_assert_uint_gt(rc, 0); \
	mu_assert_str_eq(buf, exp); \
} while (0)


static void
test_join(void)
{
	TEST_JOIN("/some/path/to/", "../up1.txt", "/some/path/to/../up1.txt");
	TEST_JOIN("/some/path/to/", "../../up2.txt", "/some/path/to/../../up2.txt");
	TEST_JOIN("/some/path/to/", "/root.txt", "/root.txt");
	TEST_JOIN("/some/path/to/", "./current.txt", "/some/path/to/./current.txt");
	TEST_JOIN("", "file.txt", "file.txt");
	TEST_JOIN("some", "file.txt", "some/file.txt");
	TEST_JOIN("some/", "../file.txt", "some/../file.txt");
	TEST_JOIN("/", "file.txt", "/file.txt");
	TEST_JOIN("/test", "", "/test");

	char buf[16];
	char base[] = "/some/longer/named/path.txt";
	char rel[] = "../../longothernamedfile.txt";
	ssize_t rc = ed_path_join(buf, sizeof(buf),
			base, sizeof(base) - 1,
			rel, sizeof(rel) - 1);
	mu_assert_int_eq(rc, ed_esys(ENOBUFS));
}

static void
test_clean(void)
{
	TEST_CLEAN("/some/path/../other/file.txt", "/some/other/file.txt");
	TEST_CLEAN("/some/path/../../other/file.txt", "/other/file.txt");
	TEST_CLEAN("/some/path/../../../other/file.txt", "/other/file.txt");
	TEST_CLEAN("../file.txt", "../file.txt");
	TEST_CLEAN("../../file.txt", "../../file.txt");
	TEST_CLEAN("/../file.txt", "/file.txt");
	TEST_CLEAN("/../../file.txt", "/file.txt");
	TEST_CLEAN("/some/./file.txt", "/some/file.txt");
	TEST_CLEAN("/some/././file.txt", "/some/file.txt");
	TEST_CLEAN("//some/file.txt", "/some/file.txt");
	TEST_CLEAN("/some//file.txt", "/some/file.txt");
	TEST_CLEAN("/a/b/c/./../../g", "/a/g");
	TEST_CLEAN(".", ".");
	TEST_CLEAN("/", "/");
	TEST_CLEAN("", ".");
	TEST_CLEAN("//", "/");
}

int
main (void)
{
	mu_init ("path");
	test_clean();
	test_join();
}

