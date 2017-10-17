#include "../lib/eddy-private.h"

static const char stat_descr[] =
	"Reports on the status of the cache. Outputs information in YAML.";
static const char stat_usage[] =
	"usage: eddy stat [-n] index\n";
static EdOption stat_opts[] = {
	{"noblock", NULL,   0, 'n', "don't block trying to read the index"},
	{0, 0, 0, 0, 0}
};

static int
stat_run(const EdCommand *cmd, int argc, char *const *argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;

	int ch;
	while ((ch = ed_opt(argc, argv, cmd)) != -1) {
		switch (ch) {
		case 'n': cfg.flags |= ED_FNOBLOCK; break;
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
	return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

