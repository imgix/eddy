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
		uintptr_t ip, offset;
		char *name;
		char *source = nullptr;
		const char *fname = nullptr;

		Symbol(uintptr_t instr, uintptr_t instroff, char *sym) noexcept :
			ip(instr), offset(instroff), name(sym) {}
		~Symbol();

		Symbol(Symbol &&other) noexcept :
			ip(other.ip),
			offset(other.offset),
			name(other.name),
			source(other.source),
			fname(other.fname)
		{
			other.name = nullptr;
			other.source = nullptr;
			other.fname = nullptr;
		}

		Symbol &operator=(Symbol &&other) noexcept
		{
			ip = other.ip;
			offset = other.offset;
			name = other.name;
			source = other.source;
			fname = other.fname;
			other.name = nullptr;
			other.source = nullptr;
			other.fname = nullptr;
			return *this;
		}

		Symbol(const Symbol &other) = delete;
		Symbol &operator=(const Symbol &other) = delete;

		void Print(int idx, FILE *out);
		void SetSource(const char *line, size_t len);
	};

	struct Image {
		std::vector<Symbol *> syms;
		uintptr_t base = 0;
		const char *path = nullptr;
		FILE *proc = nullptr;
		bool has_debug = false;

		Image() : syms() {}
		Image(Dl_info &info);
		~Image();

		Image(Image &&other) noexcept :
			syms(std::move(other.syms)),
			base(other.base),
			path(other.path),
			proc(other.proc),
			has_debug(other.has_debug)
		{
			other.proc = nullptr;
		}

		Image &operator=(Image &&other) noexcept
		{
			syms = std::move(other.syms);
			base = other.base;
			path = other.path;
			proc = other.proc;
			has_debug = other.has_debug;
			other.proc = nullptr;
			return *this;
		}

		Image(const Image &other) = delete;
		Image &operator=(const Image &other) = delete;

		void Add(Symbol *);
		bool Open(void);
		void Close(void);
		void Apply(void);
	};

	std::vector<Symbol> syms;
	std::unordered_map<const char *, Image> images;
	bool has_images;
	bool has_source;

	EdBacktrace() : syms(), images(), has_images(false), has_source(false) {}

	int Load();
	void Print(int skip, FILE *out);
	int Find(const char *name);

private:
	void CollectSource();
};

#endif

