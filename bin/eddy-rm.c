#include "../lib/eddy-private.h"

static const EdUsage rm_usage = {
	"Remove objects from the cache.",
	(const char *[]) {
		"index key [key ...]",
		NULL
	},
	NULL
};
static EdOption rm_opts[] = {
	{0, 0, 0, 0, 0}
};

static int
rm_run(const EdCommand *cmd, int argc, char *const *argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;
	int rc = 1;

	int ch;
	while ((ch = ed_opt(argc, argv, cmd)) != -1) {}
	argc -= optind;
	argv += optind;

	if (argc == 0) { errx(1, "index file path not provided"); }
	if (argc == 1) { errx(1, "key not provided"); }

	cfg.index_path = argv[0];

	rc = ed_cache_open(&cache, &cfg);
	if (rc < 0) { errx(1, "failed to open index '%s': %s", cfg.index_path, ed_strerror(rc)); }

	for (int i = 1; i < argc; i++) {
		rc = ed_update_ttl(cache, argv[i], strlen(argv[i]), 0, false);
		if (rc < 0) {
			warnx("faild to remove object: %s", ed_strerror(rc));
			break;
		}
	}

	ed_cache_close(&cache);
	return rc == 1 ? EXIT_SUCCESS : EXIT_FAILURE;
}

