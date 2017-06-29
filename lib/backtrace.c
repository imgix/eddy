#include "eddy-private.h"

#include <execinfo.h>
#include <dlfcn.h>

#define MAX_SYMBOLS 32
#define MAX_IMAGES 7

typedef struct EdImage EdImage;
typedef struct EdSymbol EdSymbol;

struct EdImage {
	EdSymbol *syms[MAX_SYMBOLS];
	int nsyms;
	bool has_debug;
};

struct EdSymbol {
	void *frame;
	Dl_info info;
	EdImage *image;
	char *source;
};

struct EdBacktrace {
	void *frames[MAX_SYMBOLS];
	EdSymbol syms[MAX_SYMBOLS];
	EdImage images[MAX_IMAGES];
	int nframes, nsyms, nimages;
};

_Static_assert(sizeof(EdBacktrace) < 4096, "EdBacktrace too big");

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
collect_symbols(EdBacktrace *bt)
{
	bt->nsyms = 0;
	for (int i = 0; i < bt->nframes; i++) {
		EdSymbol *sym = &bt->syms[bt->nsyms];
		if (dladdr(bt->frames[i], &sym->info) > 0) {
			set_image(bt, sym);
		}
		sym->frame = bt->frames[i];
		bt->nsyms++;
	}
}

int
ed_backtrace_new(EdBacktrace **btp)
{
	EdBacktrace *bt = calloc(1, sizeof(*bt));
	if (bt == NULL) { return ED_ERRNO; }
	if ((bt->nframes = backtrace(bt->frames, MAX_SYMBOLS)) < 0) {
		free(bt);
		return ED_ERRNO;
	}
	bt->nsyms = -1;
	*btp = bt;
	return 0;
}

void
ed_backtrace_free(EdBacktrace **btp)
{
	EdBacktrace *bt = *btp;
	if (bt != NULL) {
		*btp = NULL;
		free(bt);
	}
}

void
ed_backtrace_print(EdBacktrace *bt, int skip, FILE *out)
{
	FILE *procs[bt->nimages];
	int nprocs = 0;

	if (bt->nsyms < 0) { collect_symbols(bt); }
	if (bt->nsyms < 1) { goto fallback; }

#if __APPLE__
	for (int i = 0; i < bt->nimages; i++) {
		EdImage *image = &bt->images[i];
		if (!image->has_debug) { continue; }

		char buf[16384];
		int len;

		len = snprintf(buf, sizeof(buf),
				"atos -o %s -l 0x%" PRIxPTR,
				image->syms[0]->info.dli_fname,
				(uintptr_t)image->syms[0]->info.dli_fbase);

		if (len < 0 || len > (int)sizeof(buf)) { goto fallback; }

		for (int i = 0; i < image->nsyms; i++) {
			int n = snprintf(buf+len, sizeof(buf) - len, " %p",
				(void *)((uint8_t *)image->syms[i]->frame-1));
			if (n < 0 || n > (int)(sizeof(buf) - len)) { goto fallback; }
			len += n;
		}
		FILE *p = popen(buf, "r");
		if (p == NULL) { goto fallback; }
		procs[nprocs++] = p;
	}

	for (int i = 0, pi = 0; i < bt->nimages && pi < nprocs; i++, pi++) {
		EdImage *image = &bt->images[i];
		if (!image->has_debug) { continue; }
		FILE *p = procs[pi];
		for (int i = 0; i < image->nsyms; i++) {
			char buf[4096];
			if (fgets(buf, sizeof(buf), p) == NULL) { break; }
			size_t len = strnlen(buf, sizeof(buf));
			if (len < sizeof(buf) && buf[len-2] == ')') {
				char *end = strrchr(buf, '(');
				if (end) {
					image->syms[i]->source = strndup(end, len - (end - buf) - 1);
				}
			}
		}
	}
#endif

fallback:
	for (int i = 0; i < nprocs; i++) {
		pclose(procs[i]);
	}
	for (int i = skip; i < bt->nsyms; i++) {
		EdSymbol *sym = &bt->syms[i];
		const char *fname = "???";
		if (sym->info.dli_fname) {
			fname = strrchr(sym->info.dli_fname, '/');
			fname = fname ? fname+1 : sym->info.dli_fname;
		}
		ssize_t diff = (uint8_t *)sym->frame - (uint8_t *)sym->info.dli_saddr;
		fprintf(out, "%-3d %-36s0x%016zx %s + %zd %s\n",
				i - skip,
				fname,
				(size_t)sym->info.dli_saddr + diff,
				sym->info.dli_sname ? sym->info.dli_sname : "?",
				diff,
				sym->source ? sym->source : "");
	}
}

int
ed_backtrace_index(EdBacktrace *bt, const char *name)
{
	if (bt->nsyms < 0) { collect_symbols(bt); }
	for (int i = 0; i < bt->nsyms; i++) {
		if (strcmp(bt->syms[i].info.dli_sname, name) == 0) {
			return i;
		}
	}
	return -1;
}

