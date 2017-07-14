#include "eddy-util.h"

// TODO: round new size to PAGESIZE

#include <getopt.h>
#include <err.h>

#define DEFAULT_SIZE "4096p"

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-v] [-f] [-c] [-s SIZE[k|m|g|t|p]] [-S PATH] PATH\n"
			"\n"
			"about:\n"
			"  Creates a new cache index and slab.\n"
			"\n"
			"options:\n"
			"  -v        enable verbose messaging\n"
			"  -f        force creation of a new cache file\n"
			"  -c        track crc32 checksums\n"
			"  -s SIZE   size of the file (default " DEFAULT_SIZE ")\n"
			"  -S PATH   path to slab file (default is the index path with \"-slab\" suffix)\n"
			"\n"
			"Sizes are expressed as numbers with optional size modifiers.\n"
			"Supported size modifiers are:\n"
			"  k  kibibytes (%lld bytes)\n"
			"  m  mebibytes (%lld bytes)\n"
			"  g  gibibytes (%lld bytes)\n"
			"  t  tebibytes (%lld bytes)\n"
			"  p  pages (%d bytes)\n"
			,
			name, ED_KiB, ED_MiB, ED_GiB, ED_TiB, PAGESIZE);
}

int
main(int argc, char **argv)
{
	char *size_arg = DEFAULT_SIZE;
	EdConfig cfg = { .flags = ED_FCREATE|ED_FALLOCATE };

	int ch;
	while ((ch = getopt(argc, argv, ":hvfcs:S:b:")) != -1) {
		switch (ch) {
		case 'v': cfg.flags |= ED_FVERBOSE; break;
		case 'f': cfg.flags |= ED_FREPLACE; break;
		case 'c': cfg.flags |= ED_FCHECKSUM; break;
		case 's': size_arg = optarg; break;
		case 'S': cfg.slab_path = optarg; break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
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
	int ec = ed_cache_open(&cache, &cfg);
	if (ec < 0) {
		fprintf(stderr, "failed to open cache: %s\n", ed_strerror(ec));
		exit(1);
	}
	ed_cache_close(&cache);

#if ED_MMAP_DEBUG
	if (ed_pgcheck() > 0) { return EXIT_FAILURE; }
#endif
	return EXIT_SUCCESS;
}

