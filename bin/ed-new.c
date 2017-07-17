#include "eddy-private.h"
#include "parse_size.h"

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
			"usage: %s [-v] [-f] [-c] [-r] [-s SIZE[k|m|g|t|p]] [-S PATH] PATH\n"
			"\n"
			"about:\n"
			"  Creates a new cache index and slab.\n"
			"\n"
			"options:\n"
			"  -v        enable verbose messaging\n"
			"  -f        force creation of a new cache file\n"
			"  -c        track crc32 checksums\n"
			"  -r        create a RAM disk for the slab\n"
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

static ssize_t
run_cmd(const char *cmd, char *line, size_t len)
{
	FILE *p = popen(cmd, "r");
	if (p == NULL) { return ED_ERRNO; }

	int rc = 0;
	if (fgets(line, len-1, p) == NULL && ferror(p)) {
		rc = ED_ERRNO;
		goto done;
	}
	line[len-1] = '\0';

	size_t end = strcspn(line, " \t\r\n");
	line[end] = '\0';
	return end;

done:
	pclose(p);
	return rc;
}

static int
create_ram_disk(long long size, char path[static 1024])
{
#if __APPLE__
	char cmd[256];
	snprintf(cmd, sizeof(cmd), "/usr/bin/hdiutil attach -nomount ram://%lld", size/512);
	ssize_t len = run_cmd(cmd, path, 1024);
	if (len < 0) { return (int)len; }
	if (len == 0) { return ed_esys(EINVAL); }
	return 0;
#else
	(void)size;
	(void)path;
	return ed_esys(ENOTSUP);
#endif
}

static int
destroy_ram_disk(char *path)
{
#if __APPLE__
	char cmd[256];
	snprintf(cmd, sizeof(cmd), "/usr/bin/hdiutil detach %s", path);
	char buf[1024];
	return run_cmd(cmd, buf, sizeof(buf));
#else
	(void)path;
	return ed_esys(ENOTSUP);
#endif
}

int
main(int argc, char **argv)
{
	char *size_arg = DEFAULT_SIZE;
	EdConfig cfg = { .flags = ED_FCREATE|ED_FALLOCATE };
	bool ram = false;

	int ch;
	while ((ch = getopt(argc, argv, ":hrvfcs:S:b:")) != -1) {
		switch (ch) {
		case 'v': cfg.flags |= ED_FVERBOSE; break;
		case 'f': cfg.flags |= ED_FREPLACE; break;
		case 'c': cfg.flags |= ED_FCHECKSUM; break;
		case 's': size_arg = optarg; break;
		case 'r':
			if (cfg.slab_path) {
				errx(1, "RAM disk cannot be set when specifying a slab path");
			}
			ram = true;
			break;
		case 'S':
			if (ram) { errx(1, "slab path cannot be set when creating a RAM disk"); }
			cfg.slab_path = optarg;
			break;
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

	char slab_path[1024];
	if (ram) {
		ed_verbose(cfg.flags, "creating RAM disk...");
		int rc = create_ram_disk(cfg.slab_size, slab_path);
		if (rc < 0) {
			ed_verbose(cfg.flags, "failed: %s\n", ed_strerror(rc));
			exit(1);
		}
		ed_verbose(cfg.flags, "ok (%s)\n", slab_path);
		cfg.slab_path = slab_path;
	}

	EdCache *cache;
	int ec = ed_cache_open(&cache, &cfg);
	if (ec < 0) {
		fprintf(stderr, "failed to open cache: %s\n", ed_strerror(ec));
		if (ram) {
			destroy_ram_disk(slab_path);
		}
		ed_verbose(cfg.flags, "removing RAM disk (%s)...", slab_path);
		int rc = destroy_ram_disk(slab_path);
		if (rc < 0) {
			ed_verbose(cfg.flags, "failed: %s\n", ed_strerror(rc));
		}
		else {
			ed_verbose(cfg.flags, "ok\n");
		}
		exit(1);
	}
	ed_cache_close(&cache);

#if ED_MMAP_DEBUG
	if (ed_pgcheck() > 0) { return EXIT_FAILURE; }
#endif
	return EXIT_SUCCESS;
}

