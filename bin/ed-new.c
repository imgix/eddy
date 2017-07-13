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
			"usage: %s [-r] [-f] [-c] [-i PATH] [-s SIZE[k|m|g|t|p]] PATH\n"
			"\n"
			"about:\n"
			"  Creates a new cache slab and index.\n"
			"\n"
			"options:\n"
			"  -r        rebuild the index file if invalid\n"
			"  -f        force creation of a new cache file\n"
			"  -c        track crc32 checksums\n"
			"  -i PATH   path to index file (default is the cache path with \"-index\" suffix)\n"
			"  -s SIZE   size of the file (default " DEFAULT_SIZE ")\n"
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
	int open_flags = O_CREAT|O_EXCL|O_RDWR|O_CLOEXEC;
	EdConfig cfg = { .flags = 0 };
	long long size;

	int ch;
	while ((ch = getopt(argc, argv, ":hrfci:s:b:")) != -1) {
		switch (ch) {
		case 'r': cfg.flags |= ED_FREBUILD; break;
		case 'f':
			open_flags &= ~O_EXCL;
			cfg.flags |= ED_FCREATE;
			break;
		case 'c': cfg.flags |= ED_FCHECKSUM; break;
		case 'i': cfg.index_path = optarg; break;
		case 's': size_arg = optarg; break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
		}
	}
	argc -= optind;
	argv += optind;

	if (!ed_parse_size(size_arg, &size)) {
		errx(1, "-s must be a valid positive number");
	}

	int fd, ec;
	struct stat stat;
	EdCache *cache;

	if (argc == 0) { errx(1, "cache file not provided"); }
	cfg.cache_path = argv[0];

	fd = open(cfg.cache_path, open_flags, 0600);
	if (fd < 0) {
		if ((cfg.flags & ED_FREBUILD) && errno == EEXIST) { goto open_cache; }
		err(1, "failed to open file");
	}

	if (fstat(fd, &stat) < 0) { err(1, "failed to stat file"); }

	if (ED_IS_FILE(stat.st_mode)) {
		fprintf(stderr, "allocating %lld bytes...", size);
		if (ed_mkfile(fd, (off_t)size) < 0) { err(1, "error\nfailed to allocate file"); }
		fprintf(stderr, "ok\n");
	}
	else if (!ED_IS_DEVICE(stat.st_mode)) {
		errx(1, "unsupported file mode");
	}
	close(fd);

open_cache:
	ec = ed_cache_open(&cache, &cfg);
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

