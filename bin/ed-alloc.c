#include "eddy-private.h"

#include <getopt.h>
#include <err.h>
#include <pthread.h>

#define DEFAULT_PAGES 4
#define DEFAULT_SETS 3
#define DEFAULT_LOSE 0

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-t THREADS] [-p PAGES] [-s SETS] [-l PAGES] [-i PATH] PATH\n"
			"\n"
			"about:\n"
			"  Fiddles with the index page allocator. Generally its best not to run this.\n"
			"\n"
			"options:\n"
			"  -t THREADS  number of threads to run (default 1)\n"
			"  -p PAGES    number of pages to allocate and free per set (default %d, max %d)\n"
			"  -s SETS     number of sets to perform (default %d)\n"
			"  -l PAGES    number of pages to \"lose\" (default %d)\n"
			"  -i PATH     path to index file (default is the cache path with \"-index\" suffix)\n"
			,
			name,
			DEFAULT_PAGES, UINT16_MAX,
			DEFAULT_SETS,
			DEFAULT_LOSE);
}

static long pages = DEFAULT_PAGES, sets = DEFAULT_SETS, lose = DEFAULT_LOSE;

static void *
task(void *data)
{
	EdCache *cache = data;
	EdPg *p[pages];
	int rc;

	for (int x = 0; x < sets; x++) {
		ed_index_lock(&cache->index, ED_LOCK_EX, true);
		rc = ed_pgalloc(&cache->index.alloc, p, pages);
		ed_index_lock(&cache->index, ED_LOCK_UN, true);
		if (rc < 0) {
			warnx("failed to allocate page: %s", ed_strerror(rc));
			break;
		}
		ed_index_lock(&cache->index, ED_LOCK_EX, true);
		ed_pgfree(&cache->index.alloc, p, rc);
		ed_index_lock(&cache->index, ED_LOCK_UN, true);
		/*
		long n = pages - lose;
		if (n > 0) {
			ed_page_free(&cache->index, p, n);
			lose = 0;
		}
		else {
			lose -= pages;
		}
		*/
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
	while ((ch = getopt(argc, argv, ":ht:p:s:l:i:")) != -1) {
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
		case 'i': cfg.index_path = optarg; break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) { errx(1, "cache file not provided"); }
	cfg.cache_path = argv[0];

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
	if (ed_pgcheck() > 0) { return EXIT_FAILURE; }
#endif
	return EXIT_SUCCESS;
}

