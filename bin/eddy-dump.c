#include "../lib/eddy-private.h"

static int dump_hex = 0;
static bool dump_raw = false;
static EdPgno dump_skip[64], dump_nskip = 0;
static EdPgno dump_include[64], dump_ninclude = 0;
static EdTimeUnix dump_epoch;

static EdPgno dump_parse_pgno(const char *arg);
static int dump_read_raw(void);
static int dump_trees(int argc, char *const *argv, bool key, bool block);
static int dump_pages(int argc, char *const *argv);

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

static int
dump_run(const EdCommand *cmd, int argc, char *const *argv)
{
	int rc, ch;
	bool key = false, block = false;

	while ((ch = ed_opt(argc, argv, cmd->opts, &cmd->usage)) != -1) {
		switch (ch) {
		case 'i':
			if (dump_ninclude == ed_len(dump_include)) {
				errx(1, "only %zu include options supported", ed_len(dump_include));
			}
			dump_include[dump_ninclude] = dump_parse_pgno(optarg);
			dump_ninclude++;
			break;
		case 's':
			if (dump_nskip == ed_len(dump_skip)) {
				errx(1, "only %zu skip options supported", ed_len(dump_skip));
			}
			dump_skip[dump_nskip] = dump_parse_pgno(optarg);
			dump_nskip++;
			break;
		case 'r': dump_raw = true; break;
		case 'k': key = true; break;
		case 'b': block = true; break;
		case 'x': dump_hex++; break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0) {
		rc = dump_read_raw();
	}
	else if (key || block) {
		rc = dump_trees(argc, argv, key, block);
	}
	else {
		rc = dump_pages(argc, argv);
	}

	return rc < 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}

static bool
dump_has_pgno(EdPgno no, const EdPgno *list, EdPgno n)
{
	for (EdPgno i = 0; i < n; i++) {
		if (list[i] == no) { return true; }
	}
	return false;
}

static bool
dump_use_pgno(EdPgno no)
{
	if (dump_nskip > 0 && dump_has_pgno(no, dump_skip, dump_nskip)) { return false; }
	if (dump_ninclude > 0 && !dump_has_pgno(no, dump_include, dump_ninclude)) { return false; }
	return true;
}

static void
dump_page_array(EdPgno *pages, EdPgno n)
{
	printf("[");
	for (EdPgno i = 0; i < n; i++) {
		if (i > 0) { printf(", "); }
		if (pages[i] == ED_PG_NONE) {
			putc('~', stdout);
		}
		else {
			printf("%u", pages[i]);
		}
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
	if (idx->flags & ED_FPAGEALIGN) { printf("- ED_FPAGEALIGN\n"); }
	printf("size_page: %u\n", idx->size_page);
	printf("slab_block_size: %u\n", idx->slab_block_size);
	printf("nconns: %u\n", idx->nconns);
	printf("tail_start: %u\n", idx->tail_start);
	printf("tail_count: %u\n", idx->tail_count);
	printf("gc_head: %u\n", idx->gc_head);
	printf("gc_tail: %u\n", idx->gc_tail);
	printf("tree: "); dump_page_array(idx->tree, ed_len(idx->tree));
	printf("xid: %" PRIu64 "\n", idx->xid);
	printf("pos: %" PRIu64 "\n", idx->pos);
	printf("slab_block_count: %" PRIu64 "\n", idx->slab_block_count);
	printf("slab_ino: %" PRIu64 "\n", idx->slab_ino);
	printf("slab_path: %s\n", idx->slab_path);
	printf("active: "); dump_page_array(idx->active, idx->nactive);
	printf("conns:\n");

	uint8_t *end = (uint8_t *)idx + PAGESIZE;
	EdConn *c = idx->conns;
	for (int i = 0; i < idx->nconns; i++, c++) {
		if ((uint8_t *)c + sizeof(*c) > end) {
			printf("- ~\n");
			break;
		}
		if (c->pid <= 0 && c->npending == 0) { continue; }
		printf("- pid: %d\n", c->pid);
		if (c->active == 0) {
			printf("  active: -1\n");
		}
		else {
			EdTimeUnix t = ed_time_to_unix(idx->epoch, c->active);
			printf("  active: %ld\n", t);
			char buf[32];
			printf("  date: %s", ctime_r(&t, buf));
		}
		printf("  xid: %" PRIu64 "\n", c->xid);
		printf("  pending: "); dump_page_array(c->pending, c->npending);
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

	if (gc->state.nlists > 0) {
		uint8_t *end = (uint8_t *)gc + PAGESIZE;
		uint16_t head = gc->state.head;
		uint16_t nskip = gc->state.nskip;

		printf("lists:\n");
		for (uint16_t i = 0; i < gc->state.nlists; i++) {
			EdPgGcList *list = (EdPgGcList *)(gc->data + head);
			if ((uint8_t *)list >= end) {
				printf("- ~\n");
				break;
			}
			printf("- xid: %" PRIu64 "\n", list->xid);
			printf("  npages: %u\n", list->npages);
			printf("  pages: ");
			dump_page_array(list->pages + nskip, list->npages - nskip);
			head += (uint16_t)ED_GC_LIST_SIZE(list->npages);
			nskip = 0;
		}
	}
}

static void
dump_page(EdPgno no, EdPg *pg)
{
	if (!dump_use_pgno(no)) { return; }

	if (dump_raw) {
		if (pg != NULL) {
			fwrite(pg, 1, PAGESIZE, stdout);
		}
		return;
	}

	printf("---\npage: %u\ntype: ", no);

	if (pg == NULL) {
		printf("unallocated\n");
		return;
	}

	uint32_t t = pg->no == no ? pg->type : 0;
	switch (t) {
	case ED_PG_INDEX:
		printf("index\n");
		if (dump_hex < 2) { dump_index((EdPgIdx *)pg); }
		break;
	case ED_PG_BRANCH:
		printf("branch\n");
		if (dump_hex < 2) { dump_branch((EdBpt *)pg); }
		break;
	case ED_PG_LEAF:
		printf("leaf\n");
		if (dump_hex < 2) { dump_leaf((EdBpt *)pg); }
		break;
	case ED_PG_GC:
		printf("gc\n");
		if (dump_hex < 2) { dump_gc((EdPgGc *)pg); }
		break;
	default:
		printf("unused\n");
		break;
	}

	if (dump_hex > 0) {
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

EdPgno
dump_parse_pgno(const char *arg)
{
	char *end;
	long no = strtol(arg, &end, 10);
	if (*end != '\0' || no < 0 || no > (long)ED_PG_MAX) {
		errx(1, "invalid page number: %s", arg);
	}
	return (EdPgno)no;
}

int
dump_read_raw(void)
{
	EdInput in;
	int rc = ed_input_read(&in, STDIN_FILENO, PAGESIZE * ED_PG_MAX);
	if (rc < 0) { errx(1, "failed to read input: %s", ed_strerror(rc)); }
	uint8_t *p = in.data, *pe = p + in.length;
	for (; p < pe; p += PAGESIZE) {
		EdPg *pg = (EdPg *)p;
		dump_page(pg->no, pg);
	}
	ed_input_final(&in);
	return 0;
}

int
dump_pages(int argc, char *const *argv)
{
	if (argc == 0) { errx(1, "index file path not provided"); }
	if (argc == 1) { errx(1, "page number(s) not provided"); }

	EdConfig cfg = { .index_path = argv[0] };
	EdInput in;
	EdIdx idx;
	int rc;

	argc--;
	argv++;

	struct {
		EdPgno no;
		EdPg *pg;
	} pages[argc];

	for (int i = 0; i < argc; i++) {
		pages[i].no = dump_parse_pgno(argv[i]);
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
			EdPg *pg = ed_pg_map(idx.fd, pages[i].no, 1, true);
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

	ed_input_final(&in);
	return rc;
}

static int
dump_key(const void *ent, char *buf, size_t len)
{
	const EdEntryKey *k = ent;
	return snprintf(buf, len, "%016" PRIx64 "@%" PRIx64 "#%" PRIx32,
			k->hash, k->no, k->count);
}

static int
dump_block(const void *ent, char *buf, size_t len)
{
	const EdEntryBlock *b = ent;
	return snprintf(buf, len, "@%" PRIx64 "#%" PRIx32, b->no, b->count);
}

int
dump_trees(int argc, char *const *argv, bool key, bool block)
{
	if (argc == 0) { errx(1, "index file path not provided"); }

	EdConfig cfg = { .index_path = argv[0] };
	EdIdx idx;
	EdTxn *txn = NULL;
	int rc;

	rc = ed_idx_open(&idx, &cfg);
	if (rc < 0) { errx(1, "failed to open: %s", ed_strerror(rc)); }
	dump_epoch = idx.epoch;

	rc = ed_txn_new(&txn, &idx);
	if (rc < 0) {
		warnx("failed to create transaction: %s", ed_strerror(rc));
		goto done;
	}

	rc = ed_txn_open(txn, ED_FRDONLY);
	if (rc < 0) {
		warnx("failed to open transaction: %s", ed_strerror(rc));
		goto done;
	}

	if (key) {
		EdBpt *bt = NULL;
		if (ed_pg_load(idx.fd, (EdPg **)&bt, idx.hdr->tree[ED_DB_KEYS], true) == MAP_FAILED) {
			rc = ED_ERRNO;
			goto done;
		}
		ed_bpt_print(bt, idx.fd, sizeof(EdEntryKey), stdout, dump_key);
		ed_pg_unload((EdPg **)&bt);
	}
	else if (block) {
		EdBpt *bt = NULL;
		if (ed_pg_load(idx.fd, (EdPg **)&bt, idx.hdr->tree[ED_DB_BLOCKS], true) == MAP_FAILED) {
			rc = ED_ERRNO;
			goto done;
		}
		ed_bpt_print(bt, idx.fd, sizeof(EdEntryBlock), stdout, dump_block);
		ed_pg_unload((EdPg **)&bt);
	}

done:
	ed_txn_close(&txn, 0);
	ed_idx_close(&idx);
	return rc;
}

