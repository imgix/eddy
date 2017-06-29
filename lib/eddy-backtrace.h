#ifndef INCLUDED_EDDY_BACKTRACE_H
#define INCLUDED_EDDY_BACKTRACE_H

#include <vector>
#include <unordered_map>
#include <dlfcn.h>

extern "C" {
#include "eddy-private.h"
}

#if __APPLE__ || __linux__
# define ED_BACKTRACE_SOURCE 1
#else
# define ED_BACKTRACE_SOURCE 0
#endif

struct EdBacktrace {
	struct Symbol {
		const void *frame;
		const void *addr;
		char *name;
		const char *fname;
		char *source;

		Symbol(const void *frm)
			: frame(frm), addr(nullptr), name(nullptr), fname(nullptr), source(nullptr) {}
		~Symbol();

		void Print(int idx, FILE *out);
		void SetInfo(Dl_info &info);
		void SetSource(const char *line, size_t len);
	};
	std::vector<Symbol> syms;

	struct Image {
		std::vector<Symbol *> syms;
		const void *base;
		const char *path;
		FILE *proc;
		bool has_debug;

		Image() : syms() {}
		Image(Dl_info &info);
		~Image();

		void Add(Symbol *);
		bool Open(void);
		void Close(void);
		void Apply(void);
	};
	std::unordered_map<const char *, Image> images;

	bool has_symbols;
	bool has_source;

	EdBacktrace() : syms(), images(), has_symbols(false), has_source(false) {}
	EdBacktrace(void **frames, int nframes);

	int Load();
	void Load(void **frames, int nframes);

	void Print(int skip, FILE *out);
	int Find(const char *name);

private:
	void CollectSymbols();
	void CollectSource();
};

#endif

