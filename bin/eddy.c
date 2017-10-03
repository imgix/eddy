#include "../lib/eddy-private.h"
#include "opt.h"
#include "size.h"
#include "input.h"

#define STR2(v) #v
#define STR(v) STR2(v)

#include "eddy-new.c"
#include "eddy-get.c"
#include "eddy-set.c"
#include "eddy-stat.c"
#include "eddy-dump.c"
#if ED_MIME
# include "eddy-mime.c"
#endif

static const char new_descr[] =
	"Creates a new cache index and slab.";
static const char new_usage[] =
	"usage: eddy new [-v] [-f] [-c] [-s size[k|m|g|t|p]] [-S slab] index\n"
	"\n"
	"Sizes are expressed as numbers with optional size modifiers.\n"
	"Supported size modifiers are:\n"
	"  k  kibibytes (1024 bytes)\n"
	"  m  mebibytes (1048576 bytes)\n"
	"  g  gibibytes (1073741824 bytes)\n"
	"  t  tebibytes (1099511627776 bytes)\n"
	"  p  pages (" STR(PAGESIZE) " bytes)\n";
static EdOption new_opts[] = {
	{"size",       "size", 0, 's', "size of the file (default " DEFAULT_SIZE ")"},
	{"slab",       "path", 0, 'S', "path to slab file (default is the index path with \"-slab\" suffix)"},
	{"verbose",    NULL,   0, 'v', "enable verbose messaging"},
	{"force",      NULL,   0, 'f', "force creation of a new cache file"},
	{"checksum",   NULL,   0, 'c', "track crc32 checksums"},
	{"page-align", NULL,   0, 'p', "force file data to be page aligned"},
	{0, 0, 0, 0, 0}
};

static const char get_descr[] =
	"Sets the contents of an object in the cache from stdin or a file.";
static const char get_usage[] =
	"usage: eddy get [-u] [-m] index key [2>meta] >file\n"
	"       eddy get [-u] -i index key [key ...]\n";
static EdOption get_opts[] = {
	{"unlink",  NULL,  0, 'u', "immediately unlink the object"},
	{"meta",    NULL,  0, 'm', "write the object metadata to stderr"},
	{"info",    NULL,  0, 'i', "only print header information"},
	{0, 0, 0, 0, 0}
};

static const char set_descr[] =
	"Sets the contents of an object in the cache from stdin or a file.";
static const char set_usage[] =
	"usage: eddy set [-e ttl] [-m meta] index key {file | <file}\n"
	"       eddy set [-e ttl] -u index key\n";
static EdOption set_opts[] = {
	{"ttl",     "ttl",  0, 'e', "set the time-to-live in seconds"},
	{"meta",    "file", 0, 'm', "set the object meta data from the contents of a file"},
	{"update",  NULL,   0, 'u', "update fields in an existing entry"},
	{0, 0, 0, 0, 0}
};

static const char stat_descr[] =
	"Reports on the status of the cache. Outputs information in YAML.";
static const char stat_usage[] =
	"usage: eddy stat [-n] index\n";
static EdOption stat_opts[] = {
	{"noblock", NULL,   0, 'n', "don't block trying to read the index"},
	{0, 0, 0, 0, 0}
};

static const char dump_descr[] =
	"Prints information about pages in the index. Outputs information in YAML.";
static const char dump_usage[] =
	"usage: eddy dump [-rx] index page1 [page2 ...]\n"
	"       eddy dump [-rx] [-i pgno] [-s pgno] <raw\n"
	"       eddy dump {-k | -b}\n";
static EdOption dump_opts[] = {
	{"include", "pgno", 0, 'i', "include the page number in the output"},
	{"skip",    "pgno", 0, 's', "skip the page number in the output"},
	{"raw",     NULL,   0, 'r', "output the raw page(s)"},
	{"hex",     NULL,   0, 'x', "include a hex dump of the page"},
	{"keys",    NULL,   0, 'k', "print the key b+tree"},
	{"blocks",  NULL,   0, 'b', "print the slab block b+tree"},
	{0, 0, 0, 0, 0}
};

#if ED_MIME
static const char mime_descr[] =
	"Checks the MIME types of a file or standard input.";
static const char mime_usage[] =
	"usage: eddy mime [-p] [-d db] {file [file ...] | <file}\n"
	"       eddy mime -l\n";
static EdOption mime_opts[] = {
	{"db",      "pgno", 0, 'd', "path to mime.cache database file"},
	{"parents", NULL,   0, 'p', "include parent mime types"},
	{"list",    NULL,   0, 'l', "list all mime types with magic matches and exit"},
	{0, 0, 0, 0, 0}
};
#endif

static const EdCommand commands[] = {
	{"new",  new_opts,  new_run,  {new_descr,  new_usage}},
	{"get",  get_opts,  get_run,  {get_descr,  get_usage}},
	{"set",  set_opts,  set_run,  {set_descr,  set_usage}},
	{"stat", stat_opts, stat_run, {stat_descr, stat_usage}},
	{"dump", dump_opts, dump_run, {dump_descr, dump_usage}},
#if ED_MIME
	{"mime", mime_opts, mime_run, {mime_descr, mime_usage}},
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

