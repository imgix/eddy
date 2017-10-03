#include "../lib/eddy-private.h"

static int
get_run(const EdCommand *cmd, int argc, char *const *argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;
	bool unlink = false, meta = false, info = false;

	int ch;
	while ((ch = ed_opt(argc, argv, cmd->opts, &cmd->usage)) != -1) {
		switch (ch) {
		case 'u': unlink = true; break;
		case 'm': meta = true; break;
		case 'i': info = true; break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) { errx(1, "index file not provided"); }
	cfg.index_path = argv[0];

	int rc;

	rc = ed_cache_open(&cache, &cfg);
	if (rc < 0) { errx(1, "failed to open: %s", ed_strerror(rc)); }

	EdObject *obj;
	if (info) {
		for (int i = 1; i < argc; i++) {
			const char *key = argv[i];
			rc = ed_open(cache, &obj, key, strlen(key));
			printf("---\nkey: %s\n", key);
			if (rc < 0) {
				printf("error: %s\n", ed_strerror(rc));
			}
			else if (rc == 0) {
				printf("error: key not found\n");
			}
			else {
				EdTimeTTL ttl = ed_ttl(obj, -1);
				time_t expiry_timestamp = ed_expiry(obj);
				time_t created_timestamp = ed_created_at(obj);
				char expiry[32] = {0}, created[32] = {0};
				ctime_r(&created_timestamp, created);
				if (expiry_timestamp >= 0) {
					ctime_r(&expiry_timestamp, expiry);
				}

				printf(
						"ttl: %ld\n"
						"expiry: %s"
						"expiry timestamp: %ld\n"
						"created: %s"
						"created timestamp: %ld\n"
						"meta length: %u\n"
						"data length: %u\n"
						"key hash: %" PRIu64 "\n"
						,
						ttl,
						expiry_timestamp < 0 ? "~\n" : expiry,
						expiry_timestamp,
						created,
						created_timestamp,
						obj->metalen,
						obj->datalen,
						obj->hdr->keyhash);

				if (cache->idx.flags & ED_FCHECKSUM) {
					printf("meta crc: %u\ndata crc: %u\n",
							obj->hdr->metacrc, obj->hdr->datacrc);
				}
				printf("tag: %u\nversion: %u\n",
						obj->hdr->tag, obj->hdr->version);
				ed_close(&obj);
			}
		}
	}
	else {
		if (argc == 1) { errx(1, "key not provided"); }
		const char *key = argv[1];

		rc = ed_open(cache, &obj, key, strlen(key));
		if (rc < 0) {
			if (!meta) { warnx("faild to open object: %s", ed_strerror(rc)); }
		}
		else if (rc == 0) {
			if (!meta) { warnx("key not found"); }
		}
		else {
			size_t len;
			const void *data = ed_value(obj, &len);
			write(STDOUT_FILENO, data, len);
			if (meta) {
				data = ed_meta(obj, &len);
				write(STDERR_FILENO, data, len);
			}
			ed_close(&obj);
		}
	}

	if (unlink) {
		warnx("-u not yet supported");
	}

	ed_cache_close(&cache);
	return rc == 1 ? EXIT_SUCCESS : EXIT_FAILURE;
}
