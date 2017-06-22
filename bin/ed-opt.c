#include "eddy-private.h"

#include <getopt.h>
#include <err.h>

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-i PATH] PATH\n"
			"\n"
			"about:\n"
			"  Performs optimizations on the index.\n"
			"\n"
			"options:\n"
			"  -i PATH   path to index file (default is the cache path with \"-index\" suffix)\n"
			,
			name);
}

int
main(int argc, char **argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;

	int ch;
	while ((ch = getopt(argc, argv, ":hi:")) != -1) {
		switch (ch) {
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

	int rc;

	rc = ed_config_open(&cfg);
	if (rc < 0) { errx(1, "failed to open: %s", ed_strerror(rc)); }

	rc = ed_cache_open(&cache, &cfg, 0);
	if (rc < 0) { errx(1, "failed to open: %s", ed_strerror(rc)); }

	ed_index_optimize(&cache->index, true);

	ed_cache_close(&cache);
	ed_config_close(&cfg);
	return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

