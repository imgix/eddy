#include "../lib/eddy-private.h"
#include "input.h"

#include <ctype.h>
#include <getopt.h>
#include <err.h>

static int hex = 0;
static bool raw = false;
static EdPgno skip[64], nskip = 0;
static EdPgno include[64], ninclude = 0;

static void
usage(const char *prog)
{
	const char *name = strrchr(prog, '/');
	name = name ? name + 1 : prog;
	fprintf(stderr,
			"usage: %s [-rx] index page1 [page2 ...]\n"
			"       %s [-rx] [-i pgno] [-s pgno] <raw\n"
			"\n"
			"about:\n"
			"  Prints information about pages in the index.\n"
			"\n"
			"options:\n"
			"  -i pgno   include the page number in the output\n"
			"  -s pgno   skip the page number in the output\n"
			"  -r        output the raw page(s)\n"
			"  -x        include a hex dump of the page\n"
			,
			name, name);
}

static bool
has_pgno(EdPgno no, const EdPgno *list, EdPgno n)
{
	for (EdPgno i = 0; i < n; i++) {
		if (list[i] == no) { return true; }
	}
	return false;
}

static bool
use_pgno(EdPgno no)
{
	if (nskip > 0 && has_pgno(no, skip, nskip)) { return false; }
	if (ninclude > 0 && !has_pgno(no, include, ninclude)) { return false; }
	return true;
}

static void
print_page_array(EdPgno *pages, EdPgno n)
{
	printf("[");
	for (EdPgno i = 0; i < n; i++) {
		if (i > 0) { printf(", "); }
		printf("%u", pages[i]);
	}
	printf("]\n");
}

static void
dump_index(EdPgIdx *idx)
{
	printf("magic: %.4s\n", idx->magic);
	printf("endian: %c\n", idx->endian);
	printf("mark: 0x%02x\n", idx->mark);
	printf("version: %u\n", idx->version);
	printf("seed: %" PRIu64 "\n", idx->seed);
	printf("epoch: %ld\n", idx->epoch);
	printf("flags:\n");
	if (idx->flags & ED_FCHECKSUM) { printf("- ED_FCHECKSUM\n"); }
	if (idx->flags & ED_FNOPAGEALIGN) { printf("- ED_FNOPAGEALIGN\n"); }
	printf("size_page: %u\n", idx->size_page);
	printf("slab_block_size: %u\n", idx->slab_block_size);
	printf("nconns: %u\n", idx->nconns);
	printf("tail_start: %u\n", idx->tail_start);
	printf("tail_count: %u\n", idx->tail_count);
	printf("gc_head: %u\n", idx->gc_head);
	printf("gc_tail: %u\n", idx->gc_tail);
	printf("tree: "); print_page_array(idx->tree, ed_len(idx->tree));
	printf("xid: %" PRIu64 "\n", idx->xid);
	printf("pos: %" PRIu64 "\n", idx->pos);
	printf("slab_block_count: %" PRIu64 "s\n", idx->slab_block_count);
	printf("slab_ino: %" PRIu64 "s\n", idx->slab_ino);
	printf("slab_path: %s\n", idx->slab_path);
	printf("active: "); print_page_array(idx->active, idx->nactive);
	printf("conns:\n");

	uint8_t *end = (uint8_t *)idx + PAGESIZE;
	EdConn *c = idx->conns;
	for (int i = 0; i < idx->nconns; i++, c++) {
		if ((uint8_t *)c + sizeof(*c) > end) {
			printf("- ...\n");
			break;
		}
		printf("- pid: %d\n", c->pid);
		if (c->active == 0) {
			printf("  active: -1\n");
		}
		else {
			EdTimeUnix t = ed_time_to_unix(idx->epoch, c->active);
			printf("  active: %ld\n", t);
			char buf[32];
			printf("  date: %s\n", ctime_r(&t, buf));
		}
		printf("  xid: %" PRIu64 "\n", c->xid);
		printf("  pending: "); print_page_array(c->pending, c->npending);
	}
}

static void
dump_branch(EdBpt *b)
{
	printf("xid: %" PRIu64 "\n", b->xid);
	printf("nkeys: %u\n", b->nkeys);
	printf("data:\n");

	uint8_t *ptr = b->data, *key = b->data + 8;
	for (uint32_t i = 0; i < b->nkeys; i++, key += 12, ptr += 12) {
		printf("- %u\n"
		       "- %" PRIu64 "\n",
				ed_fetch32(ptr), ed_fetch64(key));
	}
	printf("- %u\n", ed_fetch32(ptr));
}

static void
dump_leaf(EdBpt *l)
{
	printf("xid: %" PRIu64 "\n", l->xid);
	if (l->next == ED_PG_NONE) {
		printf("next: ~\n");
	}
	else {
		printf("next: %u\n", l->next);
	}
	printf("nkeys: %u\n", l->nkeys);
}

static void
dump_gc(EdPgGc *gc)
{
	printf("head: %u\n", gc->state.head);
	printf("tail: %u\n", gc->state.tail);
	printf("nlists: %u\n", gc->state.nlists);
	printf("nskip: %u\n", gc->state.nskip);
	if (gc->next == ED_PG_NONE) {
		printf("next: ~\n");
	}
	else {
		printf("next: %u\n", gc->next);
	}
	printf("lists:\n");

	uint16_t head = gc->state.head;
	uint16_t nskip = gc->state.nskip;
	while (head <= gc->state.tail) {
		EdPgGcList *list = (EdPgGcList *)(gc->data + head);
		printf("- xid: %" PRIu64 "\n", list->xid);
		printf("- npages: %u\n", list->npages);
		printf("- pages: ");
		print_page_array(list->pages + nskip, list->npages - nskip);
		head += (uint16_t)ED_GC_LIST_SIZE(list->npages);
		nskip = 0;
	}
}

static void
dump_page(EdPgno no, EdPg *pg)
{
	if (!use_pgno(no)) { return; }

	if (raw) {
		if (pg != NULL) {
			fwrite(pg, 1, PAGESIZE, stdout);
		}
		return;
	}

	printf("---\npage: %u\ntype: ", no);

	if (pg == NULL) {
		printf("invalid\n");
		return;
	}

	uint32_t t = pg->no == no ? pg->type : 0;
	switch (t) {
	case ED_PG_INDEX:
		printf("index\n");
		if (hex < 2) { dump_index((EdPgIdx *)pg); }
		break;
	case ED_PG_BRANCH:
		printf("branch\n");
		if (hex < 2) { dump_branch((EdBpt *)pg); }
		break;
	case ED_PG_LEAF:
		printf("leaf\n");
		if (hex < 2) { dump_leaf((EdBpt *)pg); }
		break;
	case ED_PG_GC:
		printf("gc\n");
		if (hex < 2) { dump_gc((EdPgGc *)pg); }
		break;
	default:
		printf("unknown\n");
		break;
	}

	if (hex > 0) {
		printf("hex: |\n");
#define ROWSIZE 32
		size_t b = (size_t)no * PAGESIZE;
		uint8_t *p = (uint8_t *)pg, *pe = p + PAGESIZE;
		for (; p < pe; p += ROWSIZE, b += ROWSIZE) {
			printf("  %08zx:", b);
			uint8_t *re = p + ROWSIZE;
			for (uint32_t *r = (uint32_t *)p; r < (uint32_t *)re; r++) {
				printf(" %08x", *r);
			}
			printf("  ");
			for (; p < re; p++) {
				putc(isprint(*p) ? *p : '.', stdout);
			}
			putc('\n', stdout);
		}
#undef ROWSIZE
	}
}

static EdPgno
parse_pgno(const char *arg)
{
	char *end;
	long no = strtol(arg, &end, 10);
	if (*end != '\0' || no < 0 || no > (long)ED_PG_MAX) {
		errx(1, "invalid page number: %s", arg);
	}
	return (EdPgno)no;
}

int
main(int argc, char **argv)
{
	EdInput in;
	int rc, ch;

	while ((ch = getopt(argc, argv, ":hrxi:s:")) != -1) {
		switch (ch) {
		case 'i':
			if (ninclude == ed_len(include)) {
				errx(1, "only %zu include options supported", ed_len(include));
			}
			include[ninclude] = parse_pgno(optarg);
			ninclude++;
			break;
		case 's':
			if (nskip == ed_len(skip)) {
				errx(1, "only %zu skip options supported", ed_len(skip));
			}
			skip[nskip] = parse_pgno(optarg);
			nskip++;
			break;
		case 'r': raw = true; break;
		case 'x': hex++; break;
		case 'h': usage(argv[0]); return 0;
		case '?': errx(1, "invalid option: -%c", optopt);
		case ':': errx(1, "missing argument for option: -%c", optopt);
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) {
		rc = ed_input_read(&in, STDIN_FILENO, PAGESIZE * ED_PG_MAX);
		if (rc < 0) { errx(1, "failed to read input: %s", ed_strerror(rc)); }
		uint8_t *p = in.data, *pe = p + in.length;
		for (; p < pe; p += PAGESIZE) {
			EdPg *pg = (EdPg *)p;
			dump_page(pg->no, pg);
		}
	}
	else {
		if (argc == 0) { errx(1, "index file path not provided"); }
		if (argc == 1) { errx(1, "page number(s) not provided"); }

		argc--;
		argv++;

		EdIdx idx;
		EdConfig cfg = { .index_path = argv[0] };

		struct {
			EdPgno no;
			EdPg *pg;
		} pages[argc];

		for (int i = 0; i < argc; i++) {
			pages[i].no = parse_pgno(argv[i]);
			pages[i].pg = NULL;
		}

		rc = ed_input_new(&in, argc * PAGESIZE);
		if (rc < 0) { errx(1, "mmap failed: %s", ed_strerror(rc)); }

		rc = ed_idx_open(&idx, &cfg);
		if (rc < 0) { errx(1, "failed to open: %s", ed_strerror(rc)); }

		rc = ed_idx_lock(&idx, ED_LCK_EX);
		if (rc < 0) {
			warnx("failed to lock: %s", ed_strerror(rc));
		}
		else {
			EdPgno npages = idx.hdr->tail_start + idx.hdr->tail_count;
			for (int i = 0; i < argc; i++) {
				if (pages[i].no >= npages) { continue; }
				EdPg *pg = ed_pg_map(idx.fd, pages[i].no, 1);
				if (pg != MAP_FAILED) {
					pages[i].pg = (EdPg *)(in.data + i*PAGESIZE);
					memcpy(in.data + i*PAGESIZE, pg, PAGESIZE);
					ed_pg_unmap(pg, 1);
				}
			}
			ed_idx_lock(&idx, ED_LCK_UN);
		}

		ed_idx_close(&idx);

		if (rc >= 0) {
			for (int i = 0; i < argc; i++) {
				dump_page(pages[i].no, pages[i].pg);
			}
		}
	}

	ed_input_final(&in);
	return rc < 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}

