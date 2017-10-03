#include "../lib/eddy-private.h"
#define DEFAULT_SIZE "4096p"

static int
new_run(const EdCommand *cmd, int argc, char *const *argv)
{
	char *size_arg = DEFAULT_SIZE;
	EdConfig cfg = { .flags = ED_FCREATE|ED_FALLOCATE };

	int ch;
	while ((ch = ed_opt(argc, argv, cmd->opts, &cmd->usage)) != -1) {
		switch (ch) {
		case 'v': cfg.flags |= ED_FVERBOSE; break;
		case 'f': cfg.flags |= ED_FREPLACE; break;
		case 'c': cfg.flags |= ED_FCHECKSUM; break;
		case 'p': cfg.flags |= ED_FPAGEALIGN; break;
		case 's': size_arg = optarg; break;
		case 'S': cfg.slab_path = optarg; break;
		}
	}
	argc -= optind;
	argv += optind;

	if (!ed_parse_size(size_arg, &cfg.slab_size)) {
		errx(1, "-s must be a valid positive number");
	}

	if (argc == 0) { errx(1, "index file path not provided"); }
	cfg.index_path = argv[0];

	EdCache *cache;
	int rc = ed_cache_open(&cache, &cfg);
	if (rc < 0) {
		fprintf(stderr, "failed to open cache: %s\n", ed_strerror(rc));
		return EXIT_FAILURE;
	}
	ed_cache_close(&cache);
	return EXIT_SUCCESS;
}
