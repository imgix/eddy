#include "eddy-private.h"

#include <getopt.h>
#include <err.h>

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-u] [-m] [-i PATH] PATH KEY\n"
			"\n"
			"about:\n"
			"  Gets the contents of an object in the cache to stdout.\n"
			"\n"
			"options:\n"
			"  -u        immediately unlink the object\n"
			"  -m        write the object metadata to stderr\n"
			"  -i PATH   path to index file (default is the cache path with \"-index\" suffix)\n"
			,
			name);
}

int
main(int argc, char **argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;
	bool unlink = false, mime = false;
	const char *key = NULL;

	int ch;
	while ((ch = getopt(argc, argv, ":humi:")) != -1) {
		switch (ch) {
		case 'u': unlink = true; break;
		case 'm': mime = true; break;
		case 'i': cfg.index_path = optarg; break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) { errx(1, "cache file not provided"); }
	cfg.cache_path = argv[0];

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
		warnx("faild to create object: %s", ed_strerror(rc));
	}

	/*
	if (unlink) {
		ed_set_ttl(obj, 0);
	}
	*/

	ed_cache_close(&cache);
#if ED_MMAP_DEBUG
	if (ed_pgcheck() > 0) { return EXIT_FAILURE; }
#endif
	return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

