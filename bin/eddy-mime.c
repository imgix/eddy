#include "../lib/eddy-private.h"
#include "../lib/eddy-mime.h"

static void
mime_print(const EdMime *db, bool parents, const char *name, const char *mime);

static void
mime_print_name(const char *name, void *data);

static const char mime_descr[] =
	"Checks the MIME types of a file or standard input.";
static const char mime_usage[] =
	"usage: eddy mime [-p] [-d db] {file [file ...] | <file}\n"
	"       eddy mime -l\n";
static EdOption mime_opts[] = {
	{"db",      "pgno", 0, 'd', "path to mime.cache database file"},
	{"parents", NULL,   0, 'p', "include parent mime types"},
	{"list",    NULL,   0, 'l', "list all mime types with magic matches and exit"},
	{0, 0, 0, 0, 0}
};

static int
mime_run(const EdCommand *cmd, int argc, char *const *argv)
{
	EdMime *db = NULL;
	int flags = ED_FMIME_MLOCK;
	bool parents = false;
	bool list = false;
	const char *path = NULL;

	int ch;
	while ((ch = ed_opt(argc, argv, cmd)) != -1) {
		switch (ch) {
		case 'p': parents = true; break;
		case 'l': list = true; break;
		case 'd': path = optarg; break;
		}
	}
	argc -= optind;
	argv += optind;

	int rc = ed_mime_open(&db, path, flags);
	if (rc < 0) { errx(1, "failed to open mime.cache"); }

	if (list) {
		ed_mime_list(db, mime_print_name, NULL);
		return 0;
	}

	if (argc == 0) {
		size_t max = ed_mime_max_extent(db);
		uint8_t data[max];
		ssize_t n = fread(data, 1, max, stdin);
		if (n < 0) { err(1, "failed to read stdin"); }
		if (n > 0) { mime_print(db, parents, NULL, ed_mime_type(db, data, n, true)); }
		return 0;
	}

	for (int i = 0; i < argc; i++) {
		mime_print(db, parents, argv[i], ed_mime_file_type(db, argv[i], true));
	}

	return EXIT_SUCCESS;
}

void
mime_print(const EdMime *db, bool parents, const char *name, const char *mime)
{
	if (name) { printf("%s: ", name); }
	if (mime == NULL) {
		printf("~\n");
	}
	else if (parents) {
		static const char indent[2] = "  ";
		int len = name ? 2 : 0;
		if (name) { printf("\n"); }
		printf("%.*smime: %s\n%.*sparents:\n", len, indent, mime, len, indent);
		const char *par[8];
		size_t n = ed_mime_parents(db, mime, par, 8);
		for (size_t i = 0; i < n; i++) {
			printf("%.*s- %s\n", len, indent, par[i]);
		}
	}
	else {
		printf("%s\n", mime);
	}
}

void
mime_print_name(const char *name, void *data)
{
	(void)data;
	printf("%s\n", name);
}

