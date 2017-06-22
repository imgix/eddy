#include "eddy-mime.h"

#include <string.h>
#include <getopt.h>
#include <err.h>

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-p] [-d PATH] [PATH...]\n"
			"       %s -l\n"
			"\n"
			"about:\n"
			"  Checks the MIME types of a file or standard input.\n"
			"\n"
			"options:\n"
			"  -p        include parent mime types\n"
			"  -l        list mime types with magic matches and exit\n"
			"  -d PATH   path to mime.cache file\n"
			,
			name, name);
}

static void
print_mime(const EdMime *db, bool parents, const char *name, const char *mime)
{
	if (name) { printf("%s = ", name); }
	printf("%s", mime ? mime : "(unknown)");
	if (parents && mime) {
		const char *par[8];
		size_t n = ed_mime_parents(db, mime, par, 8);
		for (size_t i = 0; i < n; i++) {
			printf(" %s", par[i]);
		}
	}
	printf("\n");
}

static void
print_name(const char *name, void *data)
{
	(void)data;
	printf("%s\n", name);
}

int
main(int argc, char **argv)
{
	EdMime *db = NULL;
	int flags = ED_FMIME_MLOCK;
	bool parents = false;
	bool list = false;

	int ch;
	while ((ch = getopt(argc, argv, ":hpld:")) != -1) {
		switch (ch) {
		case 'p': parents = true; break;
		case 'l': list = true; break;
		case 'd':
			if (ed_mime_open(&db, optarg, flags) < 0) {
				err(1, "failed to open %s", optarg);
			}
			break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
		}
	}
	argc -= optind;
	argv += optind;

	if (db == NULL) {
		int rc = ed_mime_open(&db, "/usr/local/share/mime/mime.cache", flags);
		if (rc < 0) {
			rc = ed_mime_open(&db, "/usr/share/mime/mime.cache", flags);
			if (rc < 0) { errx(1, "failed to open mime.cache"); }
		}
	}

	if (list) {
		ed_mime_list(db, print_name, NULL);
		return 0;
	}

	if (argc == 0) {
		size_t max = ed_mime_max_extent(db);
		uint8_t data[max];
		ssize_t n = fread(data, 1, max, stdin);
		if (n < 0) { err(1, "failed to read stdin"); }
		if (n > 0) { print_mime(db, parents, NULL, ed_mime_type(db, data, n, true)); }
		return 0;
	}

	for (int i = 0; i < argc; i++) {
		print_mime(db, parents, argv[i], ed_mime_file_type(db, argv[i], true));
	}
}

