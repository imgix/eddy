#include "../lib/eddy-private.h"

static const char version_descr[] =
	"Reports on the status of the cache. Outputs information in YAML.";
static const char version_usage[] =
	"usage: eddy version [-b] index\n";
static EdOption version_opts[] = {
	{"build", NULL,   0, 'b', "only output the build version"},
	{0, 0, 0, 0, 0}
};

static int
version_run(const EdCommand *cmd, int argc, char *const *argv)
{
	int ch;
	while ((ch = ed_opt(argc, argv, cmd->opts, &cmd->usage)) != -1) {
		switch (ch) {
		case 'b':
			printf(ED_STR(VERSION_BUILD) "\n");
			return 0;
		}
	}

	printf("eddy v" ED_STR(VERSION_MAJOR) "." ED_STR(VERSION_MINOR)
			" - " ED_STR(BUILD) "@" ED_STR(VERSION_BUILD));

#if ED_MIME
	printf(" +mime");
#endif
#if ED_MIMEDB
	printf(" +mimedb");
#endif
	printf("\n");
	fflush(stdout);

	return 0;
}

