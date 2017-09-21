/*
 * mu.h
 * https://github.com/kalamay/mu
 *
 * Copyright (c) 2017, Jeremy Larkin
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef MU_INCLUDED
#define MU_INCLUDED

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <errno.h>

struct mu_counts {
	uintptr_t asserts, failures;
};

static const char *mu_name = "test";
static int mu_register, mu_main = -1;
static bool mu_fork = true, mu_tty;
static const char *mu_skip, *mu_run;
static struct mu_counts mu_counts_start, *mu_counts = &mu_counts_start;

static void mu_noop(void) {}
static void (*mu_teardown)(void) = mu_noop;

static void
mu_count_assert (void)
{
	__sync_fetch_and_add (&mu_counts->asserts, 1);
}

static void
mu_count_failure (void)
{
	mu_count_assert ();
	__sync_fetch_and_add (&mu_counts->failures, 1);
}

#define MU_CAT2(n, v) n##v
#define MU_CAT(n, v) MU_CAT2(n, v)
#define MU_TMP(n) MU_CAT(mu_tmp_##n, __LINE__)
#define MU_STR2(n) #n
#define MU_STR(n) MU_STR2(n)

#define mu_fail(...) do { \
	mu_count_failure (); \
	fprintf (stderr, __FILE__ ":" MU_STR(__LINE__) " " __VA_ARGS__); \
	exit (0); \
} while (0);

#define mu_assert_msg(exp, ...) do { \
	if (!(exp)) { \
		mu_fail(__VA_ARGS__); \
	} \
	mu_count_assert (); \
} while (0);

#define mu_assert_call(exp) \
	mu_assert_msg ((exp) >= 0, "'%s' failed (%s)\n", #exp, strerror (errno));

#define mu_assert(exp) mu_assert_msg(exp, "'%s' failed\n", #exp)

#define mu_assert_int(a, OP, b) do { \
	intmax_t MU_TMP(A) = (a); \
	intmax_t MU_TMP(B) = (b); \
	mu_assert_msg(MU_TMP(A) OP MU_TMP(B), \
	    "'%s' failed: %s=%" PRIdMAX ", %s=%" PRIdMAX "\n", \
		#a#OP#b, #a, MU_TMP(A), #b, MU_TMP(B)); \
} while (0)
#define mu_assert_int_eq(a, b) mu_assert_int(a, ==, b)
#define mu_assert_int_ne(a, b) mu_assert_int(a, !=, b)
#define mu_assert_int_lt(a, b) mu_assert_int(a, <,  b)
#define mu_assert_int_le(a, b) mu_assert_int(a, <=, b)
#define mu_assert_int_gt(a, b) mu_assert_int(a, >,  b)
#define mu_assert_int_ge(a, b) mu_assert_int(a, >=, b)

#define mu_assert_uint(a, OP, b) do { \
	uintmax_t MU_TMP(A) = (a); \
	uintmax_t MU_TMP(B) = (b); \
	mu_assert_msg(MU_TMP(A) OP MU_TMP(B), \
	    "'%s' failed: %s=%" PRIuMAX ", %s=%" PRIuMAX "\n", \
		#a#OP#b, #a, MU_TMP(A), #b, MU_TMP(B)); \
} while (0)
#define mu_assert_uint_eq(a, b) mu_assert_uint(a, ==, b)
#define mu_assert_uint_ne(a, b) mu_assert_uint(a, !=, b)
#define mu_assert_uint_lt(a, b) mu_assert_uint(a, <,  b)
#define mu_assert_uint_le(a, b) mu_assert_uint(a, <=, b)
#define mu_assert_uint_gt(a, b) mu_assert_uint(a, >,  b)
#define mu_assert_uint_ge(a, b) mu_assert_uint(a, >=, b)

#define mu_assert_str(a, OP, b) do { \
	const char *MU_TMP(A) = (const char *)(a); \
	const char *MU_TMP(B) = (const char *)(b); \
	mu_assert_msg (MU_TMP(A) == MU_TMP(B) || \
	    (MU_TMP(A) && MU_TMP(B) && 0 OP strcmp (MU_TMP(A), MU_TMP(B))), \
	    "'%s' failed: %s=\"%s\", %s=\"%s\"\n", \
		#a#OP#b, #a, MU_TMP(A), #b, MU_TMP(B)); \
} while (0)
#define mu_assert_str_eq(a, b) mu_assert_str(a, ==, b)
#define mu_assert_str_ne(a, b) mu_assert_str(a, !=, b)
#define mu_assert_str_lt(a, b) mu_assert_str(a, <,  b)
#define mu_assert_str_le(a, b) mu_assert_str(a, <=, b)
#define mu_assert_str_gt(a, b) mu_assert_str(a, >,  b)
#define mu_assert_str_ge(a, b) mu_assert_str(a, >=, b)

#define mu_assert_ptr(a, OP, b) do { \
	const void *MU_TMP(A) = (a); \
	const void *MU_TMP(B) = (b); \
	mu_assert_msg(MU_TMP(A) OP MU_TMP(B), \
	    "'%s' failed: %s=%p, %s=%p\n", #a#OP#b, #a, MU_TMP(A), #b, MU_TMP(B)); \
} while (0)
#define mu_assert_ptr_eq(a, b) mu_assert_ptr(a, ==, b)
#define mu_assert_ptr_ne(a, b) mu_assert_ptr(a, !=, b)
	
#define mu_set(T, var, new) do { \
	T old; \
	do { \
		old = var; \
	} while (!__sync_bool_compare_and_swap (&var, old, new)); \
} while (0)

static int
mu_final (void)
{
	static const char *passed[2] = { "passed", "\x1B[1;32mpassed\x1B[0m" };
	static const char *failed[2] = { "failed", "\x1B[1;31mfailed\x1B[0m" };

	__sync_synchronize ();
	uintptr_t asserts = mu_counts->asserts, fails = mu_counts->failures;
	const char *name = mu_name;
	mu_set (uintptr_t, mu_counts->asserts, 0);
	mu_set (uintptr_t, mu_counts->failures, 0);
	int rc;
	if (fails == 0) {
#if !defined(MU_SKIP_SUMMARY) && !defined(MU_SKIP_PASS_SUMMARY)
		fprintf (stderr, "%8s: %s %" PRIuPTR " assertion%s\n",
				name,
				passed[mu_tty],
				asserts,
				asserts == 1 ? "" : "s");
#else
		(void)name;
		(void)asserts;
#endif
		rc = EXIT_SUCCESS;
	}
	else {
#if !defined(MU_SKIP_SUMMARY) && !defined(MU_SKIP_FAIL_SUMMARY)
		fprintf (stderr, "%8s: %s %" PRIuPTR " of %" PRIuPTR " assertion%s\n",
				name,
				failed[mu_tty],
				fails,
				asserts,
				asserts == 1 ? "" : "s");
#else
		(void)name;
		(void)asserts;
#endif
		rc = EXIT_FAILURE;
	}
	fflush (stderr);
	fflush (stdout);
	return rc;
}

static bool
mu_ismain(void)
{
	return mu_main == getpid();
}

static void
mu_exit (void)
{
	if (mu_ismain()) { _exit (mu_final ()); }
}

static void
mu_setup (void)
{
	if (__sync_bool_compare_and_swap (&mu_register, 0, 1)) {
		mu_main = getpid();
		mu_counts = mmap (NULL, 4096,
				PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
		if (mu_counts == MAP_FAILED) {
			fprintf (stderr, "failed mmap: %s\n", strerror (errno));
			exit (1);
		}
		if (getenv ("MU_NOFORK") != NULL) { mu_fork = false; }
		mu_skip = getenv ("MU_SKIP");
		mu_run = getenv ("MU_RUN");
		mu_tty = isatty (STDERR_FILENO);
		memcpy (mu_counts, &mu_counts_start, sizeof (mu_counts_start));
		atexit (mu_exit);
	}
}

static void
mu_init (const char *name)
{
	mu_setup ();
	mu_set (const char *, mu_name, name);
	mu_set (uintptr_t, mu_counts->asserts, 0);
	mu_set (uintptr_t, mu_counts->failures, 0);
}

#define mu_run(fn) mu__run(__FILE__, __LINE__, #fn, fn)

static bool
mu__match (const char *list, const char *name)
{
	char *m = strstr (list, name);
	size_t n = strlen (name);
	return m && (m == list || m[-1] == ':') && (m[n] == '\0' || m[n] == ':');
}

static void __attribute__ ((unused))
mu__run (const char *file, int line, const char *fname, void (*fn) (void))
{
	mu_setup ();

	if (mu_skip != NULL && mu__match (mu_skip, fname)) { return; }
	if (mu_run != NULL && !mu__match (mu_run, fname)) { return; }
	if (!mu_fork) {
		fn ();
		mu_teardown ();
		mu_teardown = mu_noop;
	}
	else {
		int stat = 0, exitstat = 0, termsig = 0;
		pid_t pid = fork ();
		if (pid < 0) {
			fprintf (stderr, "%s:%d: %s failed fork '%s'\n",
					file, line, fname, strerror (errno));
			exit (1);
		}
		if (pid == 0) {
			fn ();
			mu_teardown ();
			mu_teardown = mu_noop;
			exit (0);
		}
		else {
			do {
				pid_t p = waitpid (pid, &stat, 0);
				if (p >= 0) { break; }
				if (p < 0 && errno != EINTR) {
					fprintf (stderr, "%s:%d: %s failed waitpid '%s'\n",
							file, line, fname, strerror (errno));
					exit (1);
				}
			} while (1);
		}
		if (WIFEXITED (stat) && (exitstat = WEXITSTATUS (stat))) {
			mu_count_failure ();
			fprintf (stderr, "%s:%d: %s non-zero exit (%d)\n",
					file, line, fname, exitstat);
		}
		if (WIFSIGNALED (stat) && (termsig = WTERMSIG (stat))) {
			mu_count_failure ();
			fprintf (stderr, "%s:%d: %s recieved signal (%d)\n",
					file, line, fname, termsig);
		}
	}
}

#endif

