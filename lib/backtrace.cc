#include "eddy-backtrace.h"

#include <cxxabi.h>
#include <execinfo.h>

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

EdBacktrace::EdBacktrace(void **frames, int nframes)
{
	EdBacktrace();
	Load(frames, nframes);
}

int EdBacktrace::Load()
{
	void *frames[64];
	int nframes = backtrace(frames, ed_len(frames));
	if (nframes < 0) { return ED_ERRNO; }
	Load(frames, nframes);
	return nframes;
}

void EdBacktrace::Load(void **frames, int nframes)
{
	syms.clear();
	syms.reserve(nframes);
	for (int i = 0; i < nframes; i++) {
		syms.emplace_back(frames[i]);
	}
}

void EdBacktrace::Print(int skip, FILE *out)
{
	if (out == nullptr) { out = stderr; }

	CollectSymbols();
	CollectSource();
	int i = 0;
	for (auto it = syms.begin() + skip, end = syms.end(); it != end; ++it) {
		it->Print(i++, out);
	}
}

int EdBacktrace::Find(const char *name)
{
	CollectSymbols();
	int i = 0;
	for (auto &sym : syms) {
		if (strcmp(name, sym.name) == 0) {
			return i;
		}
		i++;
	}
	return -1;
}

void EdBacktrace::CollectSymbols(void)
{
	if (has_symbols) { return; }
	has_symbols = true;
	
	for (auto &sym : syms) {
		Dl_info info;
		int rc = dladdr(sym.frame, &info);
		if (rc > 0) {
			sym.SetInfo(info);
			auto result = images.emplace(info.dli_fname, info);
			result.first->second.Add(&sym);
		}
	}
}

void EdBacktrace::CollectSource(void)
{
	if (has_source) { return; }
	has_source = true;

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

void EdBacktrace::Symbol::SetInfo(Dl_info &info)
{
	addr = info.dli_saddr;
	free(name);
	int rc;
	name = abi::__cxa_demangle(info.dli_sname, nullptr, nullptr, &rc);
	if (rc != 0) { name = strdup(info.dli_sname); }
	if (info.dli_fname) {
		fname = strrchr(info.dli_fname, '/');
		fname = fname ? fname+1 : info.dli_fname;
	}
	else {
		fname = "???";
	}
}

void EdBacktrace::Symbol::SetSource(const char *line, size_t len)
{
	free(source);
	source = nullptr;

	const char *start = line;
	if (line[len-1] == '\n') { len--; }
	if (line[len-1] == ')' && (start = strrchr(line, '('))) {
		start++;
		len = len - (start - line) - 1;
	}
	source = strndup(start, len);
}

void EdBacktrace::Symbol::Print(int idx, FILE *out)
{
	size_t diff = (uint8_t *)frame - (uint8_t *)addr;
	const char *nm = name ? name : "?";
	fprintf(out, "%-3d %-36s0x%016zx %s + %zu", idx, fname, (size_t)addr + diff, nm, diff);
	if (source) { fprintf(out, " (%s)", source); }
	fputc('\n', out);
}

EdBacktrace::Image::Image(Dl_info &info)
{
	Image();

	base = info.dli_fbase;
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
	has_debug = true;
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

	char buf[16384];
	int len = -1;

#if __APPLE__
	len = snprintf(buf, sizeof(buf), "atos -o %s -l 0x%" PRIxPTR, path, (uintptr_t)base);
#endif

	if (len < 0 || len > (int)sizeof(buf)) { return false; }

	for (const auto &sym : syms) {
		int n = snprintf(buf+len, sizeof(buf) - len, " %p",
			(void *)((uint8_t *)sym->frame-1));
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
			if (len < sizeof(buf)) { sym->SetSource(buf, len); }
		}
	}
}

