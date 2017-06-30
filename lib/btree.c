#include "eddy-private.h"

/*
 * For branch nodes, the layout of the data segment looks like:
 *
 * 0      4         12     16         24
 * +------+----------+------+----------+-----+------------+------+
 * | P[0] | Key64[0] | P[1] | Key64[1] | ... | Key64[N-1] | P[N] |
 * +------+----------+------+----------+-----+------------+------+
 *
 * The Pgno values are 32-bit numbers for the page numbers for the child page.
 * These values are guaranteed to have 4-byte alignment and do not need special
 * handling to read. The Key values are 64-bit and may not be 8-byte aligned.
 * These values need to be acquired using the `ed_fetch64` to accomodate
 * unaligned reads.
 *
 * Leaf nodes use the data segment as an array of entries. Each entry *must*
 * start with a 64-bit key.
 */

_Static_assert(sizeof(EdBTree) == PAGESIZE,
		"EdBTree size invalid");

#define BRANCH_KEY_SIZE 8
#define BRANCH_PTR_SIZE (sizeof(EdPgno))
#define BRANCH_ENTRY_SIZE (BRANCH_PTR_SIZE + BRANCH_KEY_SIZE)
#define BRANCH_FIRST(n) ((EdPgno *)(n)->data)
#define BRANCH_NEXT(pg) ((EdPgno *)((uint8_t *)(pg) + BRANCH_ENTRY_SIZE))

#define BRANCH_ORDER \
	(((sizeof(((EdBTree *)0)->data) - BRANCH_PTR_SIZE) / BRANCH_ENTRY_SIZE) + 1)

#define LEAF_ORDER(ent) \
	(sizeof(((EdBTree *)0)->data) / (ent))

#define IS_BRANCH(n) ((n)->base.type == ED_PGBRANCH)
#define IS_LEAF(n) ((n)->base.type == ED_PGLEAF)
#define IS_BRANCH_FULL(n) ((n)->nkeys == (BRANCH_ORDER-1))
#define IS_LEAF_FULL(n, ent) ((n)->nkeys == LEAF_ORDER(ent))
#define IS_FULL(n, ent) (IS_BRANCH(n) ? IS_BRANCH_FULL(n) : IS_LEAF_FULL(n, ent))

void
ed_btree_init(EdBTree *bt)
{
	bt->base.type = ED_PGLEAF;
	bt->nkeys = 0;
	bt->right = ED_PAGE_NONE;
}

static EdPgno *
search_branch(EdBTree *node, uint64_t key)
{
	EdPgno *ptr = (EdPgno *)node->data;
	uint8_t *bkey = node->data + BRANCH_PTR_SIZE;
	for (uint32_t i = 0, n = node->nkeys; i < n; i++, bkey += BRANCH_ENTRY_SIZE) {
		uint64_t cmp = ed_fetch64(bkey);
		if (key < cmp) { break; }
		ptr = BRANCH_NEXT(ptr);
	}
	return ptr;
}

int
ed_btree_search(EdBTree **root, int fd, uint64_t key, size_t esize, EdBSearch *srch)
{
	int rc = 0;
	uint32_t i = 0, n = 0;
	uint8_t *data = NULL;
	EdBTree *node = *root, *parent = node;

	memset(srch, 0, sizeof(*srch));

	srch->root = root;
	srch->key = key;
	srch->entry_size = esize;
	srch->fd = fd;
	if (node) {
		srch->nodes[0].tree = node;
		srch->nnodes = 1;
	}
	else {
		// The NULL node is an empty tree.
		srch->nsplits = 1;
		goto done;
	}
	
	// The root node needs two pages when splitting.
	srch->nsplits = IS_FULL(node, esize);

	// Search down the branches of the tree.
	while (IS_BRANCH(node)) {
		if (srch->nnodes == ed_len(srch->nodes)) {
			rc = ED_EINDEX_DEPTH;
			goto done;
		}
		if (IS_BRANCH_FULL(node)) { srch->nsplits++; }
		EdPgno *ptr = search_branch(parent, key);
		node = ed_pgmap(fd, *ptr, 1);
		if (node == MAP_FAILED) { rc = ED_ERRNO; goto done; }
		srch->nodes[srch->nnodes++] = (EdBNode){ node, parent, 0,
			((uint8_t *)ptr - parent->data) / BRANCH_ENTRY_SIZE };
		parent = node;
	}

	srch->nsplits += IS_LEAF_FULL(node, esize);

	// Search the leaf node.
	data = node->data;
	for (i = 0, n = node->nkeys; i < n; i++, data += esize) {
		uint64_t cmp = ed_fetch64(data);
		if (key == cmp) {
			rc = 1;
			break;
		}
		else if (key < cmp) {
			break;
		}
	}

done:
	if (rc < 0) { ed_bsearch_final(srch); }
	else {
		srch->entry = data;
		srch->entry_index = i;
	}
	srch->match = rc;
	return rc;
}

int
ed_bsearch_next(EdBSearch *srch)
{
	if (srch->match != 1) { return srch->match; }

	int rc = 0;
	EdBNode *node = &srch->nodes[srch->nnodes-1];
	EdBTree *leaf = node->tree;
	uint8_t *p = srch->entry;
	uint32_t i = srch->entry_index;
	if (i == leaf->nkeys-1) {
		EdPgno right = leaf->right;
		if (right == 0) { goto done; }
		if (srch->nnodes == ed_len(srch->nodes)) {
			rc = ED_EINDEX_DEPTH;
			goto done;
		}
		if ((leaf = ed_pgmap(srch->fd, right, 1)) == MAP_FAILED) {
			rc = ED_ERRNO;
			goto done;
		}
		srch->nodes[srch->nnodes++] = (EdBNode){ leaf, node->parent, 0, 0 };
		p = leaf->data;
		i = 0;
	}
	else {
		p += srch->entry_size;
		i++;
	}
	srch->entry = p;
	srch->entry_index = i;
	rc = srch->key == ed_fetch64(p);

done:
	srch->match = rc;
	return rc;
}

#if 0
static int
map_extra(EdBSearch *srch, EdBTree **p, EdPgno no)
{
	int pos = srch->nnodes + srch->nextra;
	for (int i = pos - 1; i >= 0; i--) {
		if (srch->nodes[i]->base.no == no) {
			*p = srch->nodes[i];
			return 0;
		}
	}
	EdPg *pg = ed_pgmap(srch->fd, no, 1);
	if (pg == MAP_FAILED) { return ED_ERRNO; }

	*p = (EdBTree *)pg;
	if (pos < (int)ed_len(srch->nodes)) {
		srch->nodes[pos] = (EdBTree *)pg;
		srch->nextra++;
		return 0;
	}
	return 1;
}
#endif

static int
insert_redistribute(EdBSearch *srch, EdBNode *leaf)
{
#if 0
	EdBTree *right, *parent;
	int rc = 0, rcright = 0, rcparent = 0;

	rcright = map_extra(srch, &right, leaf->right);
	if (rcright < 0) { rc = rcright; goto done; }
	if (IS_LEAF_FULL(right, srch->entry_size)) { goto done; }
	if (right->parent != leaf->parent) { goto done; }

	rcparent = map_extra(srch, &parent, leaf->parent);
	if (rcparent < 0) { rc = rcparent; goto done; }

	uint8_t *p = leaf->data + (leaf->nkeys-1)*srch->entry_size;
	EdPgno *no = search_branch(parent, ed_fetch64(p));
	uint64_t *key = (uint64_t *)((uint8_t *)no - 8);

	// TODO move last key of leaf into right and update *key

done:
	if (rcright == 1) { ed_pgunmap(right, 1); }
	if (rcparent == 1) { ed_pgunmap(parent, 1); }
	return rc;
#else
	(void)srch;
	(void)leaf;
	return 0;
#endif
}

#if 0
static void
print_branch(EdBTree *branch, FILE *out)
{
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
#endif

static EdPgno
insert_into_parent(EdBSearch *srch, EdBNode *left, EdBNode *right, uint64_t rkey, EdPg **pg, EdPgno no)
{
	EdBTree *parent = left->parent;
	EdPgno r = right->tree->base.no, rc = 0;
	size_t pos;

	// No parent, so create new root node.
	if (parent == NULL) {
		assert(no > 0);

		// Initialize new parent node from the allocation array.
		parent = (EdBTree *)pg[0];
		parent->base.type = ED_PGBRANCH;
		parent->nkeys = 0;
		parent->right = ED_PAGE_NONE;
		rc++;

		// Assign the left pointer.
		pos = BRANCH_PTR_SIZE;
		memcpy(parent->data, &left->tree->base.no, BRANCH_PTR_SIZE);

		// Insert the new parent into the list.
		left->parent = parent;
		left->pindex = 0;
		right->parent = parent;
		right->pindex = 1;
		memmove(srch->nodes+1, srch->nodes, sizeof(srch->nodes[0]) * (srch->nnodes+srch->nextra));
		srch->nodes[0] = (EdBNode){ parent, NULL, 1, 0 };
		srch->nnodes++;
	}
	else {
		uint32_t index = left->pindex;

		// The parent branch is full, so it gets split.
		if (IS_BRANCH_FULL(parent)) {
			return 0;
		}

		// The existing parent has space, so redistribute.
		pos = BRANCH_PTR_SIZE + index*BRANCH_ENTRY_SIZE;
		memmove(parent->data+pos+BRANCH_ENTRY_SIZE, parent->data+pos,
				(parent->nkeys-index)*BRANCH_ENTRY_SIZE);
	}

	// Insert the new right node.
	memcpy(parent->data+pos, &rkey, BRANCH_KEY_SIZE);
	memcpy(parent->data+pos+BRANCH_KEY_SIZE, &r, BRANCH_PTR_SIZE);
	parent->nkeys++;

	return rc;
}

static int
insert_split(EdBSearch *srch, EdBNode *leaf, EdPgalloc *alloc)
{
	EdPgno npg = srch->nsplits, used = 1;
	EdPg *pg[npg];
	int rc = ed_pgalloc(alloc, pg, npg, true);
	if (rc < 0) { return rc; }

	EdBTree *l = leaf->tree, *r = (EdBTree *)pg[0];
	ed_btree_init(r);

	uint32_t n = l->nkeys;
	uint32_t mid = n / 2;
	size_t off = mid * srch->entry_size;
	r->base.type = ED_PGLEAF;
	r->nkeys = n - mid;
	r->right = l->right;
	l->nkeys = mid;
	l->right = r->base.no;
	memcpy(r->data, l->data+off, sizeof(l->data) - off);

	uint64_t key = mid == srch->entry_index ? srch->key : ed_fetch64(r->data);;

	EdBNode *left, *right;

	// If the entry will stay in the left node, make the right node from exta.
	if (srch->entry_index < mid) {
		left = leaf;
		right = &srch->nodes[srch->nnodes + srch->nextra++];
		right->tree = r;
		right->parent = left->parent;
	}
	// Otherwise swap the leaf to take the right node and put left in extra.
	else {
		right = leaf;
		right->tree = r;
		left = &srch->nodes[srch->nnodes + srch->nextra++];
		left->tree = l;
		left->parent = right->parent;
		left->pindex = right->pindex;

		// Update the entry to reference the right node.
		srch->entry_index -= mid;
		srch->entry = r->data + srch->entry_index*srch->entry_size;
	}

	left->dirty = 1;
	right->dirty = 1;
	right->pindex = left->pindex + 1;

	used += insert_into_parent(srch, left, right, key, pg+used, npg-used);
	// Any EdBNode could be invalid now.

	// Ideally there should be no pages left.
	if (npg > used) {
		fprintf(stderr, "allocated too much! %u -> %u\n", npg, used);
		ed_pgfree(alloc, pg + used, npg - used);
	}

	return 0;
}

int
ed_bsearch_ins(EdBSearch *srch, const void *entry, EdPgalloc *alloc)
{
	if (ed_fetch64(entry) != srch->key) {
		return ED_EINDEX_KEY_MATCH;
	}

	EdBNode *leaf;
	int rc = 0;

	// If the root was NULL, create a new root node.
	if (srch->nnodes == 0) {
		EdPg *pg;
		rc = ed_pgalloc(alloc, &pg, 1, true);
		if (rc < 0) { goto done; }
		leaf = &srch->nodes[0];
		*leaf = (EdBNode){ (EdBTree *)pg, NULL, 0, 0 };
		ed_btree_init(leaf->tree);
		srch->entry = leaf->tree->data;
		srch->entry_index = 0;
		srch->nnodes = 1;
	}
	// Otherwise use the leaf from the search.
	else {
		leaf = &srch->nodes[srch->nnodes-1];

		// If the leaf is full, it needs to be redistributed or split.
		if (IS_LEAF_FULL(leaf->tree, srch->entry_size)) {
			rc = insert_redistribute(srch, leaf);
			if (rc < 0) { goto done; }
			if (rc == 0) {
				rc = insert_split(srch, leaf, alloc);
				if (rc < 0) { goto done; }
			}
		}

		// An insert may have moved the leaf node.
		leaf = &srch->nodes[srch->nnodes-1];
	}

	// Insert the new entry before the current entry position.
	uint8_t *p = srch->entry;
	size_t esize = srch->entry_size;
	memmove(p+esize, p, (leaf->tree->nkeys - srch->entry_index) * esize);
	memcpy(p, entry, esize);
	leaf->tree->nkeys++;
	*srch->root = srch->nodes[0].tree;
	srch->nsplits = 0;
	srch->match = 1;
	rc = 0;

done:
	return rc;
}

int
ed_bsearch_set(EdBSearch *srch, const void *entry)
{
	memcpy(srch->entry, entry, srch->entry_size);
	return 0;
}

void
ed_bsearch_final(EdBSearch *srch)
{
	int n = srch->nnodes + srch->nextra;
	if (n > 0) {
		// Don't unmap the first node.
		for (int i = 1; i < n; i++) {
			ed_pgunmap(srch->nodes[i].tree, 1);
		}
		memset(srch->nodes, 0, sizeof(srch->nodes));
		srch->nnodes = 0;
		srch->nextra = 0;
	}
	srch->match = 0;
	// TODO: make insert impossible
}

