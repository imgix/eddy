#include "eddy-backtrace.h"

#include <cxxabi.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <inttypes.h>
#include <unistd.h>

#define UNW_LOCAL_ONLY
#include <libunwind.h>

int
ed_backtrace_new(EdBacktrace **btp)
{
	EdBacktrace *bt = new (std::nothrow) EdBacktrace();
	if (bt == nullptr) { return ED_ERRNO; }
	int rc = bt->Load();
	if (rc < 0) {
		delete bt;
		return rc;
	}
	*btp = bt;
	return 0;
}

void
ed_backtrace_free(EdBacktrace **btp)
{
	EdBacktrace *bt = *btp;
	if (bt != nullptr) {
		*btp = nullptr;
		delete bt;
	}
}

void
ed_backtrace_print(EdBacktrace *bt, int skip, FILE *out)
{
	bool isnew = false;
	if (bt == nullptr) {
		if (ed_backtrace_new(&bt) < 0) { return; }
		if (bt == nullptr) { return; }
		isnew = true;
	}
	bt->Print(skip, out);
	if (isnew) { ed_backtrace_free(&bt); }
}

int
ed_backtrace_index(EdBacktrace *bt, const char *name)
{
	return bt->Find(name);
}

int EdBacktrace::Load()
{
	unw_cursor_t cursor;
	unw_context_t context;

	unw_getcontext(&context);
	unw_init_local(&cursor, &context);

	while (unw_step(&cursor) > 0) {
		unw_word_t offset, ip;
		if (unw_get_reg(&cursor, UNW_REG_IP, &ip) != 0) { break; }

		char sym[256], *name;
		if (unw_get_proc_name(&cursor, sym, sizeof(sym), &offset) == 0) {
			int status;
			name = abi::__cxa_demangle(sym, nullptr, nullptr, &status);
			if (status != 0) { name = strdup(sym); }
			syms.emplace_back(ip, offset, name);
		}
	}

	return 0;
}

void EdBacktrace::Print(int skip, FILE *out)
{
	if (out == nullptr) { out = stderr; }

	CollectSource();
	int i = 0;
	for (auto it = syms.begin() + skip, end = syms.end(); it != end; ++it) {
		it->Print(i++, out);
	}
}

int EdBacktrace::Find(const char *name)
{
	int i = 0;
	for (auto &sym : syms) {
		if (sym.name && strcmp(name, sym.name) == 0) {
			return i;
		}
		i++;
	}
	return -1;
}


void EdBacktrace::CollectSource(void)
{
	if (has_source) { return; }
	has_source = true;

	for (auto &sym : syms) {
		Dl_info info;
		if (dladdr((void *)sym.ip, &info) > 0) {
			const char *fname = info.dli_fname;
			auto result = images.emplace(fname, info);
			result.first->second.Add(&sym);
			fname = strstr(fname, "/./");
			sym.fname = fname ? fname + 1 : info.dli_fname;
		}
	}

#if ED_BACKTRACE_SOURCE
	// This allows the process to run in parallel
	for (auto &img : images) { img.second.Open(); }
	for (auto &img : images) { img.second.Apply(); }
	for (auto &img : images) { img.second.Close(); }
#endif
}

EdBacktrace::Symbol::~Symbol()
{
	free(name);
	free(source);
}

void EdBacktrace::Symbol::SetSource(const char *line, size_t len)
{
	free(source);
	source = nullptr;

	if (*line == '?') { return; }

	const char *start = line;
	if (line[len-1] == '\n') { len--; }
	if (line[len-1] == ')' && (start = strrchr(line, '('))) {
		start++;
		len = len - (start - line) - 1;
	}

	const char *tail = strrchr(start, '/');
	if (tail) {
		len -= tail - start + 1;
		start = tail + 1;
	}
	source = strndup(start, len);
}

void EdBacktrace::Symbol::Print(int idx, FILE *out)
{
	const char *nm = name ? name : "?";
	fprintf(out, "%-3d %-36s0x%016" PRIxPTR " %s + %" PRIuPTR, idx, fname, ip + offset, nm, offset);
	if (source) { fprintf(out, " (%s)", source); }
	fputc('\n', out);
}

EdBacktrace::Image::Image(Dl_info &info)
{
	Image();

	base = (uintptr_t)info.dli_fbase;
	path = info.dli_fname;
	proc = nullptr;
#if ED_BACKTRACE_SOURCE
# if __APPLE__
	char buf[4096];
	size_t len = strnlen(info.dli_fname, sizeof(buf) - 5);
	if (len < sizeof(buf) - 5) {
		memcpy(buf, info.dli_fname, len);
		memcpy(buf+len, ".dSYM", 6);
		has_debug = access(buf, R_OK) == 0;
	}
# else
	has_debug = strstr(path, "/libc.so") == nullptr;
# endif
#else
	has_debug = false;
#endif
}

EdBacktrace::Image::~Image()
{
	Close();
}

void EdBacktrace::Image::Add(Symbol *sym)
{
	syms.emplace_back(sym);
}

bool EdBacktrace::Image::Open()
{
#if ED_BACKTRACE_SOURCE
	if (!has_debug) { return false; }

	Close();

	char buf[4096];
	int len = -1;

#if __APPLE__
	len = snprintf(buf, sizeof(buf), "atos -o %s -l 0x%" PRIxPTR, path, base);
#else
	len = snprintf(buf, sizeof(buf), "addr2line -e %s", path);
#endif

	if (len < 0 || len > (int)sizeof(buf)) { return false; }

	for (const auto &sym : syms) {
		int n = snprintf(buf+len, sizeof(buf) - len, " %" PRIxPTR, sym->ip - 1);
		if (n < 0 || n > (int)(sizeof(buf) - len)) { return false; }
		len += n;
	}

	proc = popen(buf, "r");
	return proc != nullptr;
#else
	return false;
#endif
}

void EdBacktrace::Image::Close()
{
	if (proc) {
		pclose(proc);
		proc = nullptr;
	}
}

void EdBacktrace::Image::Apply(void)
{
	if (proc == nullptr) { return; }
	for (auto &sym : syms) {
		char buf[4096];
		if (fgets(buf, sizeof(buf), proc)) {
			size_t len = strnlen(buf, sizeof(buf));
			if (len < sizeof(buf) - 1) {
				buf[len] = '\0';
				sym->SetSource(buf, len);
			}
		}
	}
}

