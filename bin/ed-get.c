#include "../lib/eddy-private.h"

#include <getopt.h>
#include <err.h>

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-u] [-m] index key [2>meta] >file\n"
			"\n"
			"about:\n"
			"  Gets the contents of an object in the cache to stdout.\n"
			"\n"
			"options:\n"
			"  -u        immediately unlink the object\n"
			"  -m        write the object metadata to stderr\n"
			,
			name);
}

int
main(int argc, char **argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;
	bool unlink = false, meta = false;
	const char *key = NULL;

	int ch;
	while ((ch = getopt(argc, argv, ":hum")) != -1) {
		switch (ch) {
		case 'u': unlink = true; break;
		case 'm': meta = true; break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) { errx(1, "index file not provided"); }
	cfg.index_path = argv[0];

	if (argc == 1) { errx(1, "key not provided"); }
	key = argv[1];

	argc -= 2;
	argv += 2;

	int rc;

	rc = ed_cache_open(&cache, &cfg);
	if (rc < 0) { errx(1, "failed to open: %s", ed_strerror(rc)); }

	EdObject *obj;
	rc = ed_open(cache, &obj, key, strlen(key));
	if (rc < 0) {
		if (!meta) { warnx("faild to open object: %s", ed_strerror(rc)); }
	}
	else if (rc == 0) {
		if (!meta) { warnx("key not found"); }
	}
	else {
		size_t len;
		const void *data = ed_value(obj, &len);
		write(STDOUT_FILENO, data, len);
		if (meta) {
			data = ed_meta(obj, &len);
			write(STDERR_FILENO, data, len);
		}
		ed_close(&obj);
	}

	/*
	if (unlink) {
		ed_set_ttl(obj, 0);
	}
	*/

	ed_cache_close(&cache);
#if ED_MMAP_DEBUG
	if (ed_pg_check() > 0) { return EXIT_FAILURE; }
#endif
	return rc == 1 ? EXIT_SUCCESS : EXIT_FAILURE;
}

