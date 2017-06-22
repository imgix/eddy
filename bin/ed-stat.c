#include "eddy-private.h"

#include <getopt.h>
#include <err.h>

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-e] [-i PATH] PATH\n"
			"\n"
			"about:\n"
			"  Reports on the status of the cache. Outputs information in YAML.\n"
			"\n"
			"options:\n"
			"  -e        perform extended stat (requires briefly locking the index)\n"
			"  -i PATH   path to index file (default is the cache path with \"-index\" suffix)\n"
			,
			name);
}

int
main(int argc, char **argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;
	int flags = 0;

	int ch;
	while ((ch = getopt(argc, argv, ":hei:")) != -1) {
		switch (ch) {
		case 'e': flags |= ED_FSTAT_EXTEND; break;
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

	rc = ed_cache_open(&cache, &cfg);
	if (rc < 0) { errx(1, "failed to open: %s", ed_strerror(rc)); }

	rc = ed_cache_stat(cache, stdout, flags);
	if (rc < 0) { errx(1, "failed to verify: %s", ed_strerror(rc)); }

	ed_cache_close(&cache);
	return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

