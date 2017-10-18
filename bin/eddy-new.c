#include "../lib/eddy-private.h"
#define DEFAULT_SIZE "4096p"

static const EdUsage new_usage = {
	"Creates a new cache index and slab.",
	(const char *[2]) {
		"[-v] [-f] [-c] [-s size] [-b size] [-S slab] index",
		NULL
	},
	"size:\n"
	"  Sizes are expressed as a number with an optional size modifier.\n"
	"  Supported modifiers:\n"
	"    k  kibibytes (1024 bytes)\n"
	"    m  mebibytes (1048576 bytes)\n"
	"    g  gibibytes (1073741824 bytes)\n"
	"    t  tebibytes (1099511627776 bytes)\n"
	"    p  pages (" ED_STR(PAGESIZE) " bytes)\n"
	"    b  blocks (multiple of --block-size)"
};
static EdOption new_opts[] = {
	{"size",       "size", 0, 's', "size of the file (default " DEFAULT_SIZE ")"},
	{"block-size", "size", 0, 'b', "byte size of the blocks in the slab (default 1p)"},
	{"slab",       "path", 0, 'S', "path to slab file (default is the index path with \"-slab\" suffix)"},
	{"seed",       "num",  0, 'D', "use an explicit (0 will create a random seed)"},
	{"verbose",    NULL,   0, 'v', "enable verbose messaging"},
	{"force",      NULL,   0, 'f', "force creation of a new cache file"},
	{"checksum",   NULL,   0, 'c', "track crc32 checksums"},
	{"keep-old",   NULL,   0, 'k', "don't mark replaced objects as expired"},
	{"page-align", NULL,   0, 'p', "force file data to be page aligned"},
	{0, 0, 0, 0, 0}
};

static int
new_run(const EdCommand *cmd, int argc, char *const *argv)
{
	char *size_arg = DEFAULT_SIZE;
	char *end;
	long long val;
	unsigned long long uval;
	EdConfig cfg = { .flags = ED_FCREATE|ED_FALLOCATE };

	int ch;
	while ((ch = ed_opt(argc, argv, cmd)) != -1) {
		switch (ch) {
		case 'v': cfg.flags |= ED_FVERBOSE; break;
		case 'f': cfg.flags |= ED_FREPLACE; break;
		case 'c': cfg.flags |= ED_FCHECKSUM; break;
		case 'k': cfg.flags |= ED_FKEEPOLD; break;
		case 'p': cfg.flags |= ED_FPAGEALIGN; break;
		case 's': size_arg = optarg; break;
		case 'S': cfg.slab_path = optarg; break;
		case 'b':
			if (!ed_parse_size(optarg, &val, PAGESIZE)) {
				errx(1, "%s must be a valid positive number", argv[optind-1]);
			}
			if (val < 16 || val > UINT16_MAX) {
				errx(1, "%s must be be >= 16 and <= %u", argv[optind-1], UINT16_MAX);
			}
			cfg.slab_block_size = (uint16_t)val;
			break;
		case 'D':
			uval = strtoull(optarg, &end, 10);
			if (*end != '\0') {
				errx(1, "%s must be a valid number", argv[optind-1]);
			}
			cfg.seed = uval;
			break;
		}
	}
	argc -= optind;
	argv += optind;

	if (!ed_parse_size(size_arg, &cfg.slab_size, cfg.slab_block_size)) {
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
