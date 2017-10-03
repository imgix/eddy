#include <ctype.h>
#include <getopt.h>
#include <err.h>

#ifndef ED_OPT_MAX
# define ED_OPT_MAX 32
#endif

typedef struct {
	const char *description;
	const char *usage;
} EdUsage;

typedef struct {
	char *name;
	char *var;
	int *flag;
	int val;
	char *usage;
} EdOption;

typedef struct EdCommand {
	const char *name;
	EdOption *opts;
	int (*run)(const struct EdCommand *cmd, int argc, char *const *argv);
	EdUsage usage;
} EdCommand;

static const EdOption *optcur = NULL;

static void
ed_usage_opts(const EdOption *opts)
{
	int maxname = 0, maxvar = 0;
	for (const EdOption *o = opts; o->name; o++) {
		int len = (int)strlen(o->name);
		if (len > maxname) { maxname = len; }
		if (o->var) {
			len = (int)strlen(o->var);
			if (len > maxvar) { maxvar = len; }
		}
	}
	for (const EdOption *o = opts; o->name; o++) {
		fprintf(stderr, "  ");
		if (o->flag == NULL && isprint(o->val)) { fprintf(stderr, "-%c,", o->val); }
		else { fprintf(stderr, "   "); }
		fprintf(stderr, "--%-*s %-*s    %s\n",
				maxname, o->name, maxvar, o->var ? o->var : "", o->usage);
	}
}

static void
ed_usage(const EdUsage *usage, const EdOption *opts)
{
	if (usage) {
		if (usage->usage) { fprintf(stderr, "%s\n", usage->usage); }
		if (usage->description) { fprintf(stderr, "about:\n  %s\n\n", usage->description); }
	}
	if (opts) {
		fprintf(stderr, "options:\n");
		ed_usage_opts(opts);
	}
}

static int
ed_opt(int argc, char *const *argv, const EdOption *o, const EdUsage *usage)
{
	static struct option copy[ED_OPT_MAX];
	static const EdOption *set = NULL;
	static char optshort[ED_OPT_MAX*2 + 2];
	static int count = 0, help = -1, helpch = 0;

	if (set != o) {
		char *p = optshort;
		*p++ = ':';

		for (count = 0, help = -1, helpch = 'h', set = o; o->name; count++, o++) {
			if (count == ED_OPT_MAX) { errx(1, "too many options"); }
			copy[count].name = o->name;
			copy[count].has_arg = o->var ? required_argument : no_argument;
			copy[count].flag = o->flag;
			copy[count].val = o->val;
			if (o->flag == NULL && isprint(o->val)) {
				*p++ = o->val;
				if (o->var) { *p++ = ':'; }
			}
			if (strcmp(o->name, "help") == 0) {
				help = count;
				helpch = o->val;
			}
			else if (o->val == 'h') {
				helpch = 0;
			}
		}

		if (help < 0 && count < ED_OPT_MAX) {
			copy[count].name = "help";
			copy[count].has_arg = no_argument;
			copy[count].flag = 0;
			copy[count].val = helpch;
			*p++ = copy[count].val;
			help = count++;
		}

		*p++ = '\0';
		memset(&copy[count], 0, sizeof(copy[count]));
	}

	int idx = 0;
	int ch = getopt_long(argc, argv, optshort, copy, &idx);
	if (ch == helpch && (ch || idx == help)) {
		ed_usage(usage, set);
		exit(0);
	}
	optcur = ch ? NULL : &set[idx];
	switch (ch) {
	case '?': errx(1, "invalid option: %s", argv[optind-1]);
	case ':': errx(1, "missing argument for option: %s", argv[optind-1]);
	}
	return ch;
}

static int
ed_cmd(int argc, char *const *argv, const EdCommand *cmd)
{
	if (!optind || optreset) {
		optreset = 0;
		optind = 1;
	}
	if (optind >= argc || !argv[optind]) {
		errx(1, "missing command name");
	}

	const char *name = argv[optind];

	int max = 0;
	for (const EdCommand *c = cmd; c->name; c++) {
		if (strcmp(name, c->name) == 0) {
			optind++;
			return c->run(c, argc, argv);
		}
		int len = strlen(c->name);
		if (len > max) { max = len; }
	}

	if (strcmp(name, "help") && strcmp(name, "--help") && strcmp(name, "-h")) {
		errx(1, "unknown command name: %s", name);
	}

	const char *prog = strrchr(argv[0], '/');
	prog = prog ? prog + 1 : argv[0];
	fprintf(stderr,
			"usage: %s command [args ...]\n"
			"       %s command --help\n"
			"\n"
			"commands:\n", prog, prog);
	for (const EdCommand *c = cmd; c->name; c++) {
		fprintf(stderr, "  %-*s    %s\n", max, c->name, c->usage.description);
	}
	exit(0);
}
