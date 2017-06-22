#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <err.h>
#include <errno.h>

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s COUNT PROG [...ARGS]\n"
			"\n"
			"about:\n"
			"  Executes multiple copies of a program in parallel.\n"
			,
			name);
}

int
main(int argc, char **argv)
{
	if (argc == 1) { errx(1, "count required"); }

	if (strcmp(argv[1], "-h") == 0) {
		usage(argv[0]);
		return 0;
	}

	int ec = 0;
	long i, n;
	char *end;

	n = strtol(argv[1], &end, 10);
	if (*end != '\0' || n < 1) { errx(1, "invalid number"); }

	argc -= 2;
	argv += 2;
	if (argc == 0) { errx(1, "command required"); }

	for (i = 0; i < n; i++) {
		pid_t p = fork();
		if (p == -1) {
			ec = errno;
			break;
		}
		if (p == 0 && execvp(argv[0], argv) < 0) {
			err(1, "exec failed");
		}
	}

	for (; i > 0; i--) {
		wait(NULL);
	}

	if (ec > 0) { errc(1, ec, "fork failed"); }
}

