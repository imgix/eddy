#include "../lib/eddy-private.h"

#include <getopt.h>
#include <err.h>

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-n] index\n"
			"\n"
			"about:\n"
			"  Reports on the status of the cache. Outputs information in YAML.\n"
			"\n"
			"options:\n"
			"  -n        don't block trying to read the index\n"
			,
			name);
}

int
main(int argc, char **argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;

	int ch;
	while ((ch = getopt(argc, argv, ":hn")) != -1) {
		switch (ch) {
		case 'n': cfg.flags |= ED_FNOBLOCK; break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) { errx(1, "index file path not provided"); }
	cfg.index_path = argv[0];

	int rc;

	rc = ed_cache_open(&cache, &cfg);
	if (rc < 0) { errx(1, "failed to open: %s", ed_strerror(rc)); }

	rc = ed_cache_stat(cache, stdout, cfg.flags);
	if (rc < 0) { errx(1, "failed to stat: %s", ed_strerror(rc)); }

	ed_cache_close(&cache);
#if ED_MMAP_DEBUG
	if (ed_pg_check() > 0) { return EXIT_FAILURE; }
#endif
	return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

