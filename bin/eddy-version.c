#include "../lib/eddy-private.h"

static const char version_descr[] =
	"Show version information.";
static const char version_usage[] =
	"usage: eddy version [-b] [-y] index\n";
static EdOption version_opts[] = {
	{"yaml",  NULL,   0, 'y', "output in YAML format"},
	{"build", NULL,   0, 'b', "only output the build version"},
	{0, 0, 0, 0, 0}
};

static int
version_run(const EdCommand *cmd, int argc, char *const *argv)
{
	bool yaml = false;

	int ch;
	while ((ch = ed_opt(argc, argv, cmd->opts, &cmd->usage)) != -1) {
		switch (ch) {
		case 'y': yaml = true; break;
		case 'b':
			printf(ED_STR(VERSION_BUILD) "\n");
			return 0;
		}
	}

	if (yaml) {
		printf(
				"major: " ED_STR(VERSION_MAJOR) "\n"
				"minor: " ED_STR(VERSION_MINOR) "\n"
				"build: " ED_STR(VERSION_BUILD) "\n"
				"features:\n");
#if ED_MIME
		printf("- mime\n");
#endif
#if ED_MIMEDB
		printf("- mimedb\n");
#endif
#if ED_DUMP
		printf("- dump\n");
#endif
#if ED_MMAP_DEBUG
		printf("- mmapdbg\n");
#endif
#if ED_FAULT
		printf("- fault\n");
#endif
	}
	else {
		printf("eddy v" ED_STR(VERSION_MAJOR) "." ED_STR(VERSION_MINOR)
				" - " ED_STR(BUILD) "@%.7s", ED_STR(VERSION_BUILD) "\n");
#if ED_MIME
		printf(" +mime");
#endif
#if ED_MIMEDB
		printf(" +mimedb");
#endif
#if ED_DUMP
		printf(" +dump");
#endif
#if ED_MMAP_DEBUG
		printf(" +mmapdbg");
#endif
#if ED_FAULT
		printf(" +fault");
#endif
		printf("\n");
	}
		fflush(stdout);

	return 0;
}

