#include "../lib/eddy-private.h"

static int
set_run(const EdCommand *cmd, int argc, char *const *argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;
	EdInput meta = ed_input_make();
	EdInput data = ed_input_make();
	char *end;
	int rc;
	EdObjectAttr attr = { .ttl = -1 };
	bool update = false;

	int ch;
	while ((ch = ed_opt(argc, argv, cmd->opts, &cmd->usage)) != -1) {
		switch (ch) {
		case 'u': update = true; break;
		case 'e':
			attr.ttl = strtol(optarg, &end, 10);
			if (*end != '\0') { errx(1, "invalid number: -%c", optopt); }
			break;
		case 'm':
			rc = ed_input_fread(&meta, optarg, UINT16_MAX);
			if (rc < 0) { errc(1, ed_ecode(rc), "failed to read MIME file"); }
			break;
		}
	}
	argc -= optind;
	argv += optind;

	if (update) {
		errx(1, "-u option not implemented");
	}

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
	return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

