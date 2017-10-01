#include "../lib/eddy-private.h"
#include "input.h"

#include <getopt.h>
#include <err.h>

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-e ttl] [-m meta] index key {file | <file}\n"
			"\n"
			"about:\n"
			"  Sets the contents of an object in the cache from stdin or a file.\n"
			"\n"
			"options:\n"
			"  -e ttl    set the time-to-live in seconds\n"
			"  -m meta   set the object meta data from the contents of a file\n"
			"  -z        zero entry region before writing\n"
			,
			name);
}

int
main(int argc, char **argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;
	EdInput meta = ed_input_make();
	EdInput data = ed_input_make();
	char *end;
	int rc;
	EdObjectAttr attr = { .ttl = -1 };

	int ch;
	while ((ch = getopt(argc, argv, ":hze:m:")) != -1) {
		switch (ch) {
		case 'z': cfg.flags |= ED_FZERO; break;
		case 'e':
			attr.ttl = strtol(optarg, &end, 10);
			if (*end != '\0') { errx(1, "invalid number: -%c", optopt); }
			break;
		case 'm':
			rc = ed_input_fread(&meta, optarg, UINT16_MAX);
			if (rc < 0) { errc(1, ed_ecode(rc), "failed to read MIME file"); }
			break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) { errx(1, "index file path not provided"); }
	cfg.index_path = argv[0];

	if (argc == 1) { errx(1, "key not provided"); }
	attr.key = argv[1];
	attr.keylen = strlen(attr.key);

	argc -= 2;
	argv += 2;

	rc = ed_input_fread(&data, argc ? argv[0] : NULL, UINT32_MAX);
	if (rc < 0) { errc(1, ed_ecode(rc), "failed to read object file"); }

	rc = ed_cache_open(&cache, &cfg);
	if (rc < 0) { errx(1, "failed to open index '%s': %s", cfg.index_path, ed_strerror(rc)); }

	attr.meta = meta.data;
	attr.metalen = meta.length;
	attr.datalen = data.length;

	EdObject *obj;
	rc = ed_create(cache, &obj, &attr);
	if (rc < 0) {
		warnx("faild to create object: %s", ed_strerror(rc));
	}

	ed_write(obj, data.data, data.length);
	ed_close(&obj);

	ed_input_final(&data);
	ed_input_final(&meta);
	ed_cache_close(&cache);
#if ED_MMAP_DEBUG
	if (ed_pg_check() > 0) { return EXIT_FAILURE; }
#endif
	return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

