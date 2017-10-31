#include "../lib/eddy-private.h"
#define DEFAULT_SIZE "4096p"

#include <pwd.h>

#if __linux__
# define WITH_RAM 1
#endif

static const EdUsage new_usage = {
	"Creates a new cache index and slab.",
	(const char *[]) {
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
#if WITH_RAM
	{"ram",        NULL,   0, 'R', "create the slab as a RAM-backed device"},
#endif
	{0, 0, 0, 0, 0}
};

#if WITH_RAM
static void
new_ram(const EdConfig *cfg, uid_t uid, gid_t gid)
{
	const char *path;
	char pbuf[4096];

	if (cfg->slab_path) {
		path = cfg->slab_path;
	}
	else {
		size_t len = strnlen(cfg->index_path, sizeof(pbuf));
		if (len >= sizeof(pbuf) - sizeof("-slab")) { errx(1, "index path too long"); }
		memcpy(pbuf, cfg->index_path, len);
		memcpy(pbuf+len, "-slab", sizeof("-slab"));
		path = pbuf;
	}

	int rc = mknod(path, S_IFBLK|0660, makedev(1, 1));
	if (rc < 0) { err(1, "mknod failed"); }
	rc = chmod(path, 0660);
	if (rc < 0) { err(1, "chmod failed"); }

	int fd = open(path, O_DIRECT|O_WRONLY);
	if (fd < 0) { err(1, "open failed"); }

	char zbuf[4096];
	memset(zbuf, 0, sizeof(zbuf));
	for (long long n = cfg->slab_size; n > 0; n -= sizeof(zbuf)) {
		size_t len = n >= (long long)sizeof(zbuf) ? sizeof(zbuf) : (size_t)n;
		ssize_t n = write(fd, zbuf, len);
		if (n < 0) { err(1, "write failed"); }
	}
	close(fd);

	rc = chown(path, uid, gid);
	if (rc < 0) { err(1, "chown failed"); }
}
#endif

static int
new_run(const EdCommand *cmd, int argc, char *const *argv)
{
	uid_t uid = getuid();
	gid_t gid = getgid();
	if (uid == 0) {
		const char *user = getenv("SUDO_USER");
		if (user) {
			struct passwd *pwd = getpwnam(user);
			if (pwd == NULL) { err(1, "user name '%s' failed", user); }
			uid = pwd->pw_uid;
			gid = pwd->pw_gid;
		}
	}

	char *size_arg = DEFAULT_SIZE;
	char *end;
	long long val;
	unsigned long long uval;
	EdConfig cfg = { .flags = ED_FCREATE|ED_FALLOCATE, .slab_block_size = 4096 };
#if WITH_RAM
	bool ram = false;
#endif

	int ch;
	while ((ch = ed_opt(argc, argv, cmd)) != -1) {
		switch (ch) {
		case 'v': cfg.flags |= ED_FVERBOSE; break;
		case 'f': cfg.flags |= ED_FREPLACE; break;
		case 'c': cfg.flags |= ED_FCHECKSUM; break;
		case 'k': cfg.flags |= ED_FKEEPOLD; break;
		case 'p': cfg.flags |= ED_FPAGEALIGN; break;
#if WITH_RAM
		case 'R': ram = true; break;
#endif
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
		errx(1, "size must be a valid positive number");
	}

	if (argc == 0) { errx(1, "index file path not provided"); }
	cfg.index_path = argv[0];

#if WITH_RAM
	if (ram) {
		new_ram(&cfg, uid, gid);
	}
#endif

	setgid(gid);
	setuid(uid);

	EdCache *cache;
	int rc = ed_cache_open(&cache, &cfg);
	if (rc < 0) {
		fprintf(stderr, "failed to open cache: %s\n", ed_strerror(rc));
		return EXIT_FAILURE;
	}
	ed_cache_close(&cache);
	return EXIT_SUCCESS;
}
