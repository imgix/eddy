#include "../lib/eddy-private.h"
#include <ctype.h>
#include <getopt.h>
#include <err.h>

#if __linux__
#define errc(eval, code, ...) do { \
	errno = (code); \
	err(eval, __VA_ARGS__); \
} while (0)
#endif

/**
 * @defgroup  options  Command runner and option parser
 * @{
 */

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
ed_usage(const EdOption *opts, const EdUsage *usage)
{
	if (usage) {
		if (usage->usage) { fprintf(stderr, "%s\n", usage->usage); }
		if (usage->description) { fprintf(stderr, "about:\n  %s\n", usage->description); }
	}

	if (opts->name) {
		fprintf(stderr, "\noptions:\n");

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
}

static int
ed_opt_check(int ch, char *const *argv)
{
	switch (ch) {
	case '?': errx(1, "invalid option: %s", argv[optind-1]);
	case ':': errx(1, "missing argument for option: %s", argv[optind-1]);
	}
	return ch;
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
		ed_usage(set, usage);
		exit(0);
	}
	optcur = ch ? NULL : &set[idx];
	return ed_opt_check(ch, argv);
}

static void
ed_help(int argc, char *const *argv, const EdCommand *cmds)
{
	const char *prog = strrchr(argv[0], '/');
	prog = prog ? prog + 1 : argv[0];

	argc -= optind;
	argv += optind;

	if (argc > 0) {
		for (const EdCommand *c = cmds; c->name; c++) {
			if (strcmp(argv[0], c->name) == 0) {
				ed_usage(c->opts, &c->usage);
				exit(0);
			}
		}
		errx(1, "unknown command name: %s", argv[0]);
	}
	else {
		int max = 0;
		for (const EdCommand *c = cmds; c->name; c++) {
			int len = strlen(c->name);
			if (len > max) { max = len; }
		}
		fprintf(stderr,
				"usage: %s command [args ...]\n"
				"       %s help command\n"
				"\n"
				"commands:\n", prog, prog);
		for (const EdCommand *c = cmds; c->name; c++) {
			fprintf(stderr, "  %-*s    %s\n", max, c->name, c->usage.description);
		}
	}
}

static int
ed_cmd(int argc, char *const *argv, const EdCommand *cmds)
{
	if (!optind) {
		optind = 1;
	}
	if (optind >= argc || !argv[optind]) {
		errx(1, "missing command name");
	}

	const char *name = argv[optind];

	for (const EdCommand *c = cmds; c->name; c++) {
		if (strcmp(name, c->name) == 0) {
			optind++;
			return c->run(c, argc, argv);
		}
	}

	if (strcmp(name, "help") && strcmp(name, "--help") && strcmp(name, "-h")) {
		errx(1, "unknown command name: %s", name);
	}

	optind++;
	ed_help(argc, argv, cmds);
	exit(0);
}
/** @} */

/**
 * @defgroup  input  File/stdin input interface
 * @{
 */
typedef struct {
	uint8_t *data;
	size_t length;
	bool mapped;
} EdInput;

#define ed_input_make() ((EdInput){ NULL, 0, false })

static int
ed_input_new(EdInput *in, size_t size)
{
	uint8_t *m = mmap(NULL, size,
			PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
	if (m == MAP_FAILED) { return ED_ERRNO; }
	in->data = m;
	in->length = size;
	in->mapped = true;
	return 0;
}

static int
ed_input_read(EdInput *in, int fd, off_t max)
{
	struct stat sbuf;
	if (fstat(fd, &sbuf) < 0) { return ED_ERRNO; }

	if (S_ISREG (sbuf.st_mode)) {
		if (sbuf.st_size > max) { return ed_esys(EFBIG); }
		void *m = mmap(NULL, sbuf.st_size, PROT_READ, MAP_SHARED, fd, 0);
		if (m == MAP_FAILED) { return ED_ERRNO; }
		in->data = m;
		in->length = sbuf.st_size;
		in->mapped = true;
		return 0;
	}

	size_t len = 0, cap = 0, next = 4096;
	uint8_t *p = NULL;
	int rc;
	do {
		if (len == cap) {
			uint8_t *new = realloc(p, next);
			if (new == NULL) { goto done_errno; }
			p = new;
			cap = next;
			next *= 2;
		}
		ssize_t n = read(fd, p + len, cap - len);
		if (n < 0) { goto done_errno; }
		if (n == 0) { break; }
		len += n;
		if (max > 0 && len > (size_t)max) {
			rc = ed_esys(EFBIG);
			goto done_rc;
		}
	} while (1);
	in->data = p;
	in->length = len;
	in->mapped = false;
	return 0;

done_errno:
	rc = ED_ERRNO;
done_rc:
	free(p);
	return rc;
}

static int
ed_input_fread(EdInput *in, const char *path, off_t max)
{
	if (path == NULL || strcmp(path, "-") == 0) {
		return ed_input_read(in, STDIN_FILENO, max);
	}
	int fd = open(path, O_RDONLY);
	if (fd < 0) { return ED_ERRNO; }
	int rc = ed_input_read(in, fd, max);
	close(fd);
	return rc;
}

static void
ed_input_final(EdInput *in)
{
	if (in->mapped) { munmap(in->data, in->length); }
	else { free(in->data); }
	in->data = NULL;
	in->length = 0;
	in->mapped = false;
}
/** @} */

/**
 * @defgroup  parse_size  Parse byte size information
 * @{
 */
#define ED_KiB 1024ll
#define ED_MiB (ED_KiB*ED_KiB)
#define ED_GiB (ED_KiB*ED_MiB)
#define ED_TiB (ED_KiB*ED_GiB)

static bool
ed_parse_size(const char *val, long long *out, size_t block)
{
	char *end;
	long long size = strtoll(val, &end, 10);
	if (size < 0) { return false; }
	switch(*end) {
	case 'k': case 'K': size *= ED_KiB; end++; break;
	case 'm': case 'M': size *= ED_MiB; end++; break;
	case 'g': case 'G': size *= ED_GiB; end++; break;
	case 't': case 'T': size *= ED_TiB; end++; break;
	case 'p': case 'P': size *= PAGESIZE; end++; break;
	case 'b': case 'B': size *= block; end++; break;
	}
	if (*end != '\0') { return false; }
	*out = size;
	return true;
}
/** @} */

#include "eddy-version.c"
#include "eddy-new.c"
#include "eddy-get.c"
#include "eddy-set.c"
#include "eddy-update.c"
#include "eddy-ls.c"
#include "eddy-stat.c"
#if ED_DUMP
# include "eddy-dump.c"
#endif
#if ED_MIME
# include "eddy-mime.c"
#endif

static const EdCommand commands[] = {
	{"new",     new_opts,     new_run,     {new_descr,     new_usage}},
	{"get",     get_opts,     get_run,     {get_descr,     get_usage}},
	{"set",     set_opts,     set_run,     {set_descr,     set_usage}},
	{"update",  update_opts,  update_run,  {update_descr,  update_usage}},
	{"ls",      ls_opts,      ls_run,      {ls_descr,      ls_usage}},
	{"stat",    stat_opts,    stat_run,    {stat_descr,    stat_usage}},
	{"version", version_opts, version_run, {version_descr, version_usage}},
#if ED_DUMP
	{"dump",    dump_opts,    dump_run,    {dump_descr,    dump_usage}},
#endif
#if ED_MIME
	{"mime",    mime_opts,    mime_run,    {mime_descr,    mime_usage}},
#endif
	{0, 0, 0, {0, 0}},
};

int
main(int argc, char *const *argv)
{
	int rc = ed_cmd(argc, argv, commands);
#if ED_MMAP_DEBUG
	if (ed_pg_check() > 0) { return EXIT_FAILURE; }
#endif
	return rc;
}

