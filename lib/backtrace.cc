extern "C" {
#include "eddy-private.h"
}

#include <cxxabi.h>
#include <execinfo.h>
#include <dlfcn.h>

#define MAX_SYMBOLS 32

#if __APPLE__ || __linux__
# define HAS_SOURCE_LINE 1
# define MAX_IMAGES 7
#else
# define HAS_SOURCE_LINE 0
#endif

typedef struct EdImage EdImage;
typedef struct EdSymbol EdSymbol;

struct EdImage {
	EdSymbol *syms[MAX_SYMBOLS];
	int nsyms;
#if HAS_SOURCE_LINE
	bool has_debug;
#endif
};

struct EdSymbol {
	void *frame;
	Dl_info info;
	char *name;
#if HAS_SOURCE_LINE
	EdImage *image;
	char *source;
#endif
};

struct EdBacktrace {
	void *frames[MAX_SYMBOLS];
	EdSymbol syms[MAX_SYMBOLS];
	int nframes;
	int nsyms;
#if HAS_SOURCE_LINE
	EdImage images[MAX_IMAGES];
	int nimages;
	bool has_source;
#endif
	bool has_symbols;
};

#if HAS_SOURCE_LINE

static bool
has_debug(EdSymbol *sym)
{
#if __APPLE__
	char buf[4096];
	size_t len = strnlen(sym->info.dli_fname, sizeof(buf) - 5);
	if (len > sizeof(buf) - 6) { return false; }
	memcpy(buf, sym->info.dli_fname, len);
	memcpy(buf+len, ".dSYM", 6);
	return access(buf, R_OK) == 0;
#else
	return true;
#endif
}

static void
set_image(EdBacktrace *bt, EdSymbol *sym)
{
	int i = 0;
	for (; i < bt->nimages; i++) {
		if (sym->info.dli_fbase == bt->images[i].syms[0]->info.dli_fbase) {
			goto done;
		}
	}
	if (bt->nimages == MAX_IMAGES) {
		return;
	}
	bt->nimages++;
done:
	sym->image = &bt->images[i];
	bt->images[i].has_debug = has_debug(sym);
	bt->images[i].syms[bt->images[i].nsyms++] = sym;
}

static void
collect_source(EdBacktrace *bt)
{
	if (bt->has_source) { return; }
	bt->has_source = true;

	if (bt->nsyms < 1) { return; }

	FILE *procs[MAX_IMAGES];
	int nprocs = 0;

	for (int i = 0; i < bt->nimages; i++) {
		EdImage *image = &bt->images[i];
		if (!image->has_debug) { continue; }

		char buf[16384];
		int len = -1;

#if __APPLE__
		len = snprintf(buf, sizeof(buf),
				"atos -o %s -l 0x%" PRIxPTR,
				image->syms[0]->info.dli_fname,
				(uintptr_t)image->syms[0]->info.dli_fbase);
#endif

		if (len < 0 || len > (int)sizeof(buf)) { goto done; }

		for (int i = 0; i < image->nsyms; i++) {
			int n = snprintf(buf+len, sizeof(buf) - len, " %p",
				(void *)((uint8_t *)image->syms[i]->frame-1));
			if (n < 0 || n > (int)(sizeof(buf) - len)) { goto done; }
			len += n;
		}
		FILE *p = popen(buf, "r");
		if (p == NULL) { goto done; }
		procs[nprocs++] = p;
	}

	for (int i = 0, pi = 0; i < bt->nimages && pi < nprocs; i++, pi++) {
		EdImage *image = &bt->images[i];
		if (!image->has_debug) { continue; }
		FILE *p = procs[pi];
		for (int i = 0; i < image->nsyms; i++) {
			char buf[4096], *start;
			if (fgets(buf, sizeof(buf), p) == NULL) { break; }
			size_t len = strnlen(buf, sizeof(buf));
			if (len >= sizeof(buf)) { continue; }
#if __APPLE__
			if (buf[len-2] != ')' || (start = strrchr(buf, '(')) == NULL) { continue; }
			start++;
			len = len - (start - buf) - 2;
#else
			start = buf;
			len--;
#endif
			image->syms[i]->source = strndup(start, len);
		}
	}

done:
	for (int i = 0; i < nprocs; i++) {
		pclose(procs[i]);
	}
}

#endif

static void
collect_symbols(EdBacktrace *bt)
{
	if (bt->has_symbols) { return; }
	bt->has_symbols = true;

	bt->nsyms = 0;
	for (int i = 0; i < bt->nframes; i++) {
		EdSymbol *sym = &bt->syms[bt->nsyms];
		int rc = dladdr(bt->frames[i], &sym->info);

		if (rc > 0) {
			sym->name = abi::__cxa_demangle(sym->info.dli_sname, nullptr, nullptr, &rc);
#if HAS_SOURCE_LINE
			set_image(bt, sym);
#endif
		}
		else {
			sym->name = NULL;
			memset(&sym->info, 0, sizeof(sym->info));
		}
		sym->frame = bt->frames[i];
		bt->nsyms++;
	}
}

int
ed_backtrace_new(EdBacktrace **btp)
{
	EdBacktrace *bt = (EdBacktrace *)calloc(1, sizeof(*bt));
	if (bt == NULL) { return ED_ERRNO; }
	if ((bt->nframes = backtrace(bt->frames, MAX_SYMBOLS)) < 0) {
		free(bt);
		return ED_ERRNO;
	}
	*btp = bt;
	return 0;
}

void
ed_backtrace_free(EdBacktrace **btp)
{
	EdBacktrace *bt = *btp;
	if (bt != NULL) {
		*btp = NULL;
		for (int i = 0; i < bt->nsyms; i++) {
			free(bt->syms[i].name);
#if HAS_SOURCE_LINE
			free(bt->syms[i].source);
#endif
		}
		free(bt);
	}
}

void
ed_backtrace_print(EdBacktrace *bt, int skip, FILE *out)
{
	bool isnew = false;
	if (bt == NULL) {
		if (ed_backtrace_new(&bt) < 0) { return; }
		isnew = true;
	}

	collect_symbols(bt);
#if HAS_SOURCE_LINE
	collect_source(bt);
#endif

	for (int i = skip; i < bt->nsyms; i++) {
		EdSymbol *sym = &bt->syms[i];
		const char *fname = "???";
		if (sym->info.dli_fname) {
			fname = strrchr(sym->info.dli_fname, '/');
			fname = fname ? fname+1 : sym->info.dli_fname;
		}
		ssize_t diff = (uint8_t *)sym->frame - (uint8_t *)sym->info.dli_saddr;
		const char *name = sym->name ? sym->name : sym->info.dli_sname;
		fprintf(out, "%-3d %-36s0x%016zx %s + %zd",
				i - skip,
				fname,
				(size_t)sym->info.dli_saddr + diff,
				name ? name : "?",
				diff);
#if HAS_SOURCE_LINE
		if (sym->source) { fprintf(out, " (%s)", sym->source); }
#endif
		fputc('\n', out);
	}
	if (isnew) { ed_backtrace_free(&bt); }
}

int
ed_backtrace_index(EdBacktrace *bt, const char *name)
{
	collect_symbols(bt);

	for (int i = 0; i < bt->nsyms; i++) {
		if (strcmp(bt->syms[i].info.dli_sname, name) == 0) {
			return i;
		}
	}
	return -1;
}

