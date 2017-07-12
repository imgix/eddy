#include "eddy-private.h"
#include "eddy-butil.h"

static void
default_print(const void *ent, char *buf, size_t len)
{
	snprintf(buf, len, "%-11llu", ed_fetch64(ent));
}

static void
print_branch(EdBTree *branch, FILE *out)
{
	fprintf(out, "branch p%u, nkeys=%u\n", branch->base.no, branch->nkeys);
	fprintf(out, "0   %4zu", BRANCH_PTR_SIZE);
	for (uint32_t i = 0; i < branch->nkeys; i++) {
		fprintf(out, "          %4zu   %4zu",
				i*BRANCH_ENTRY_SIZE + BRANCH_ENTRY_SIZE,
				i*BRANCH_ENTRY_SIZE + BRANCH_ENTRY_SIZE + BRANCH_PTR_SIZE);
	}
	fprintf(out, "\n+------+");
	for (uint32_t i = 0; i < branch->nkeys; i++) {
		fprintf(out, "-------------+------+");
	}
	uint8_t *p = branch->data;
	fprintf(out, "\n| p%-3u |", ed_fetch32(p));
	p += BRANCH_PTR_SIZE;
	for (uint32_t i = 0; i < branch->nkeys; i++, p += BRANCH_ENTRY_SIZE) {
		fprintf(out, " %-11llu |", ed_fetch64(p));
		fprintf(out, " p%-3u |", ed_fetch32(p+BRANCH_KEY_SIZE));
	}
	fprintf(out, "\n+------+");
	for (uint32_t i = 0; i < branch->nkeys; i++) {
		fprintf(out, "-------------+------+");
	}
	fprintf(out, "\n");
}

static void
print_leaf(EdBTree *leaf, size_t esize, FILE *out, EdBTreePrint print)
{
	fprintf(out, "%s p%u, nkeys=%u\n",
			leaf->base.type == ED_PGLEAF ? "leaf" : "overflow",
			leaf->base.no, leaf->nkeys);
	uint8_t *p = leaf->data;
	for (uint32_t x = 0; x < leaf->nkeys; x += 12) {
		uint32_t n = leaf->nkeys - x;
		if (n > 12) { n = 12; }

		if (x == 0) {
			fprintf(out, "+");
			for (uint32_t i = 0; i < n; i++) {
				fprintf(out, "-------------+");
			}
			fprintf(out, "\n");
		}
		fprintf(out, "|");
		for (uint32_t i = 0; i < n; i++, p += esize) {
			char buf[12];
			print(p, buf, 11);
			buf[11] = '\0';
			fprintf(out, " %-11s |", buf);
		}
		fprintf(out, "\n+");
		for (uint32_t i = 0; i < n; i++) {
			fprintf(out, "-------------+");
		}
		fprintf(out, "\n");
	}
}

static int
verify_overflow(EdBTree *o, size_t esize, FILE *out, uint64_t expect)
{
	uint8_t *p = o->data;
	for (uint32_t i = 0; i < o->nkeys; i++, p += esize) {
		uint64_t key = ed_fetch64(p);
		if (key != expect) {
			if (out != NULL) {
				fprintf(out, "overflow key incorrect: %llu, %llu\n", key, expect);
				print_leaf(o, esize, out, default_print);
			}
			return -1;
		}
	}
	return 0;
}

static int
verify_leaf(EdBTree *l, int fd, size_t esize, FILE *out, uint64_t min, uint64_t max)
{
	if (l->nkeys == 0) {
		if (out != NULL) { fprintf(out, "leaf has no keys\n"); }
		return -1;
	}

	uint8_t *p = l->data;
	uint64_t last;
	for (uint32_t i = 0; i < l->nkeys; i++, p += esize) {
		uint64_t key = ed_fetch64(p);
		if (key < min || key > max) {
			if (out != NULL) {
				fprintf(out, "leaf key out of range: %llu, %llu...%llu\n", key, min, max);
				print_leaf(l, esize, out, default_print);
			}
			return -1;
		}
		if (i > 0 && key < last) {
			if (out != NULL) {
				fprintf(out, "leaf key out of order: %llu\n", key);
				print_leaf(l, esize, out, default_print);
			}
			return -1;
		}
		last = key;
	}

	EdPgno ptr = l->right;
	while (ptr != ED_PAGE_NONE) {
		EdBTree *next = ed_pgmap(fd, ptr, 1);
		int rc = 0;
		if (next->base.type == ED_PGOVERFLOW) {
			rc = verify_overflow(next, esize, out, last);
			ptr = next->right;
		}
		else {
			ptr = ED_PAGE_NONE;
		}
		ed_pgunmap(next, 1);
		if (rc < 0) { return -1; }
	}
	return 0;
}

int
verify_node(EdBTree *t, int fd, size_t esize, FILE *out, uint64_t min, uint64_t max)
{
	if (t->base.type == ED_PGLEAF) {
		return verify_leaf(t, fd, esize, out, min, max);
	}

	uint8_t *p = t->data;
	uint64_t nmin = min, nmax;
	EdBTree *chld;
	int rc;

	for (uint16_t i = 0; i < t->nkeys; i++, p += BRANCH_ENTRY_SIZE) {
		nmax = ed_fetch64(p + BRANCH_PTR_SIZE);
		if (nmax < min || nmax > max) {
			if (out != NULL) {
				fprintf(out, "branch key out of range: %llu, %llu...%llu\n", nmax, min, max);
				print_branch(t, out);
			}
			return -1;
		}

		chld = ed_pgmap(fd, ed_fetch32(p), 1);
		if (chld == MAP_FAILED) { return ED_ERRNO; }
		rc = verify_node(chld, fd, esize, out, nmin, nmax - 1);
		ed_pgunmap(chld, 1);
		if (rc < 0) { return rc; }
		nmin = nmax;
	}

	chld = ed_pgmap(fd, ed_fetch32(p), 1);
	if (chld == MAP_FAILED) { return ED_ERRNO; }
	rc = verify_node(chld, fd, esize, out, nmin, max);
	ed_pgunmap(chld, 1);
	if (rc < 0) { return rc; }

	return 0;
}

void
ed_btree_print_node(EdBTree *t, size_t esize, FILE *out, EdBTreePrint print)
{
	if (out == NULL) { out = stdout; }
	if (print == NULL) { print = default_print; }
	if (t->base.type == ED_PGBRANCH) {
		print_branch(t, out);
	}
	else {
		print_leaf(t, esize, out, print);
	}
}

void
ed_btree_print(EdBTree *t, int fd, size_t esize, FILE *out, EdBTreePrint print)
{
	if (out == NULL) { out = stdout; }
	if (print == NULL) { print = default_print; }

	EdPgno ptr;
	EdBTree *nt;

	switch (t->base.type) {
	case ED_PGOVERFLOW:
	case ED_PGLEAF:
		print_leaf(t, esize, out, print);
		ptr = t->right;
		while (ptr != ED_PAGE_NONE) {
			nt = ed_pgmap(fd, ptr, 1);
			if (nt->base.type == ED_PGOVERFLOW) {
				print_leaf(nt, esize, out, print);
				ptr = nt->right;
			}
			else {
				ptr = ED_PAGE_NONE;
			}
			ed_pgunmap(nt, 1);
		}
		break;
	case ED_PGBRANCH:
		print_branch(t, out);

		ptr = ed_fetch32(t->data);
		uint8_t *p = t->data + BRANCH_PTR_SIZE;

		nt = ed_pgmap(fd, ptr, 1);
		if (nt == MAP_FAILED) { return; }
		ed_btree_print(nt, fd, esize, out, print);
		ed_pgunmap(nt, 1);

		for (uint16_t i = 0; i < t->nkeys; i++, p += BRANCH_ENTRY_SIZE) {
			ptr = ed_fetch32(p + BRANCH_KEY_SIZE);
			nt = ed_pgmap(fd, ptr, 1);
			if (nt == MAP_FAILED) { return; }
			ed_btree_print(nt, fd, esize, out, print);
			ed_pgunmap(nt, 1);
		}
		break;
	}
}

int
ed_btree_verify(EdBTree *t, int fd, size_t esize, FILE *out)
{
	return verify_node(t, fd, esize, out, 0, UINT64_MAX);
}

