#include "eddy-private.h"

#include <getopt.h>
#include <err.h>
#include <pthread.h>

static long pages = 4, sets = 3, lose = 0, usec = 0;

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-t THREADS] [-p PAGES] [-s SETS] [-l PAGES] [-w USEC] PATH\n"
			"\n"
			"about:\n"
			"  Fiddles with the index page allocator. Generally its best not to run this.\n"
			"\n"
			"options:\n"
			"  -t THREADS  number of threads to run (default 1)\n"
			"  -p PAGES    number of pages to allocate and free per set (default %ld, max %u)\n"
			"  -s SETS     number of sets to perform (default %ld)\n"
			"  -l PAGES    number of pages to \"lose\" (default %ld)\n"
			"  -w USEC     hold on to the pages for some microseconds (default %ld)\n"
			,
			name,
			pages, UINT16_MAX,
			sets,
			lose,
			usec);
}

static void *
task(void *data)
{
	EdCache *cache = data;
	EdPg *p[pages];
	int rc;

	for (int x = 0; x < sets; x++) {
		rc = ed_pg_alloc(&cache->index.alloc, p, pages, false);
		if (rc < 0) {
			warnx("failed to allocate page: %s", ed_strerror(rc));
			break;
		}
		else if (rc < pages) {
			ed_idx_lock(&cache->index, ED_LCK_EX, true);
			int nrc = ed_pg_alloc(&cache->index.alloc, p+rc, pages-rc, true);
			if (nrc < 0) { warnx("failed to allocate page: %s", ed_strerror(nrc)); }
			else { rc += nrc; }
			ed_idx_lock(&cache->index, ED_LCK_UN, true);
		}
		if (lose > 0) { 
			if (rc < lose) {
				lose -= rc;
				rc = 0;
			}
			else {
				rc -= lose;
				lose = 0;
			}
		}
		if (usec > 0) { usleep(usec); }
		if (rc > 0) {
			ed_idx_lock(&cache->index, ED_LCK_EX, true);
			ed_pg_free(&cache->index.alloc, p, rc);
			ed_idx_lock(&cache->index, ED_LCK_UN, true);
		}
	}
	return NULL;
}

int
main(int argc, char **argv)
{
	EdConfig cfg = ed_config_make();
	EdCache *cache = NULL;
	char *end;
	long threads = 1;
	int rc;

	int ch;
	while ((ch = getopt(argc, argv, ":ht:p:s:l:w:")) != -1) {
		switch (ch) {
		case 't':
			threads = strtol(optarg, &end, 10);
			if (threads < 0 || *end != '\0') { errx(1, "invalid number: -t"); }
			break;
		case 'p':
			pages = strtol(optarg, &end, 10);
			if (*end != '\0') { errx(1, "invalid number: -%c", optopt); }
			if (pages < 0 || pages > UINT16_MAX) { errx(1, "invalid range: -%c", optopt); }
			break;
		case 's':
			sets = strtol(optarg, &end, 10);
			if (*end != '\0') { errx(1, "invalid number: -%c", optopt); }
			if (sets < 0) { errx(1, "invalid range: -%c", optopt); }
			break;
		case 'l':
			lose = strtol(optarg, &end, 10);
			if (*end != '\0') { errx(1, "invalid number: -%c", optopt); }
			if (lose < 0) { errx(1, "invalid range: -%c", optopt); }
			break;
		case 'w':
			usec = strtol(optarg, &end, 10);
			if (*end != '\0') { errx(1, "invalid number: -%c", optopt); }
			if (lose < 0) { errx(1, "invalid range: -%c", optopt); }
			break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) { errx(1, "index file not provided"); }
	cfg.index_path = argv[0];

	rc = ed_cache_open(&cache, &cfg);
	if (rc < 0) { errx(1, "failed to open: %s", ed_strerror(rc)); }

	if (threads > 1) {
		pthread_t t[threads];
		for (long i = 0; i < threads; i++) {
			if (pthread_create(&t[i], NULL, task, cache) < 0) {
				errx(1, "failed to create thread");
			}
		}
		for (long i = 0; i < threads; i++) {
			pthread_join(t[i], NULL);
		}
	}
	else {
		task(cache);
	}

	ed_cache_close(&cache);

#if ED_MMAP_DEBUG
	if (ed_pg_check() > 0) { return EXIT_FAILURE; }
#endif
	return EXIT_SUCCESS;
}

