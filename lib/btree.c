#include "eddy-private.h"

/*
 * For branch nodes, the layout of the data segment looks like:
 *
 * +---------+--------+---------+--------+-----+----------+---------+
 * | Pgno[0] | Key[0] | Pgno[1] | Key[1] | ... | Key[N-1] | Pgno[N] |
 * +---------+--------+---------+--------+-----+----------+---------+
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
#define BRANCH_FIRST(n) ((EdPgno *)(n)->data)
#define BRANCH_ENTRY_SIZE (BRANCH_PTR_SIZE + BRANCH_KEY_SIZE)
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

//static bool isroot(const EdBTree *bt) { return bt->base.no == bt->parent; }

void
ed_btree_init(EdBTree *bt)
{
	bt->base.type = ED_PGLEAF;
	bt->nkeys = 0;
	bt->parent = ED_PAGE_NONE;
	bt->right = ED_PAGE_NONE;
}

static EdPgno
search_branch(EdBTree *node, uint64_t key)
{
	EdPgno *ptr = (EdPgno *)node->data;
	uint8_t *bkey = node->data + BRANCH_PTR_SIZE;
	for (uint32_t i = 0, n = node->nkeys; i < n; i++, bkey += BRANCH_ENTRY_SIZE) {
		uint64_t cmp = ed_fetch64(bkey);
		if (key < cmp) { break; }
		ptr = BRANCH_NEXT(ptr);
	}
	return *ptr;
}

static uint32_t
branch_index(EdBTree *node, EdPgno no)
{
	EdPgno *ptr = (EdPgno *)node->data;
	uint32_t i = 0;
	for (uint32_t n = node->nkeys; i <= n && *ptr != no; i++, ptr = BRANCH_NEXT(ptr)) {}
	return i;
}

int
ed_btree_search(EdBTree **root, int fd, uint64_t key, size_t esize, EdBSearch *srch)
{
	int rc = 0;
	uint32_t i = 0, n = 0;
	uint8_t *data = NULL;
	EdBTree *node = *root;

	srch->root = root;
	srch->nodes[0] = node;
	srch->key = key;
	srch->entry_size = esize;
	srch->fd = fd;
	srch->nnodes = node != NULL;
	srch->nextra = 0;

	// The NULL node is an empty tree.
	if (node == NULL) {
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
		EdPgno ptr = search_branch(node, key);
		node = ed_pgmap(fd, ptr, 1);
		if (node == MAP_FAILED) { rc = ED_ERRNO; goto done; }
		srch->nodes[srch->nnodes++] = node;
		if (IS_BRANCH_FULL(node)) {
			srch->nsplits++;
		}
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
	EdBTree *leaf = srch->nodes[srch->nnodes-1];
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
		srch->nodes[srch->nnodes++] = leaf;
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

static EdBTree *
get_mapped_node(EdBSearch *srch, EdPgno no)
{
	for (int i = srch->nnodes + srch->nextra - 1; i >= 0; i--) {
		if (srch->nodes[i]->base.no == no) {
			return srch->nodes[i];
		}
	}
	return NULL;
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
insert_redistribute(EdBSearch *srch, EdBTree *leaf)
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

static EdPgno
insert_into_parent(EdBSearch *srch, EdBTree *left, EdBTree *right, EdPg **pg, EdPgno no)
{
	// No parent, so create new root node.
	if (left->parent == ED_PAGE_NONE) {
		assert(no == 1);
		EdBTree *root = (EdBTree *)pg[0];
		root->base.type = ED_PGBRANCH;
		root->nkeys = 1;
		root->parent = ED_PAGE_NONE;
		root->right = ED_PAGE_NONE;
		memcpy(root->data, &left->base.no, BRANCH_PTR_SIZE);
		memcpy(root->data+BRANCH_PTR_SIZE, right->data, BRANCH_KEY_SIZE);
		memcpy(root->data+BRANCH_ENTRY_SIZE, &right->base.no, BRANCH_PTR_SIZE);
		left->parent = root->base.no;
		right->parent = root->base.no;
		srch->nodes[0] = root;
		return 1;
	}

	EdBTree *parent = get_mapped_node(srch, left->parent);
	uint32_t index = branch_index(parent, left->base.no);

	if (!IS_BRANCH_FULL(parent)) {
		uint8_t *p = parent->data;
		size_t rpos = BRANCH_PTR_SIZE + index*BRANCH_ENTRY_SIZE;
		memmove(p+rpos+BRANCH_ENTRY_SIZE, p+rpos, (parent->nkeys-index)*BRANCH_ENTRY_SIZE);
		memcpy(p+rpos, right->data, BRANCH_KEY_SIZE);
		memcpy(p+rpos+BRANCH_PTR_SIZE, &right->base.no, BRANCH_PTR_SIZE);
		return 0;
	}

	return 0;
}

static int
insert_split(EdBSearch *srch, EdBTree **leafp, EdPgalloc *alloc)
{
	EdPgno npg = srch->nsplits, used = 1;
	EdPg *pg[npg];
	int rc = ed_pgalloc(alloc, pg, npg, true);
	if (rc < 0) { return rc; }

	EdBTree *left = *leafp, *right = (EdBTree *)pg[0];
	ed_btree_init(right);

	uint32_t n = left->nkeys;
	uint32_t mid = n / 2;
	size_t off = mid * srch->entry_size;
	left->nkeys = mid;
	left->right = right->base.no;
	right->base.type = ED_PGLEAF;
	right->nkeys = n - mid;
	right->right = left->right;
	memcpy(right->data, left->data+off, sizeof(left->data) - off);

	used += insert_into_parent(srch, left, right, pg+used, npg-used);

	// Ideally there should be no pages left.
	if (npg > used) { fprintf(stderr, "allocated too much! %u -> %u\n", npg, used); }
	ed_pgfree(alloc, pg + used, npg - used);

	if (srch->entry_index < mid) {
		*leafp = left;
		ed_pgunmap(right, 1);
	}
	else {
		*leafp = right;
		ed_pgunmap(left, 1);
		srch->entry_index -= mid;
		srch->entry = right->data + srch->entry_index*srch->entry_size;
	}
	srch->nodes[srch->nnodes++] = *leafp;
	return 0;
}

int
ed_bsearch_ins(EdBSearch *srch, const void *entry, EdPgalloc *alloc)
{
	if (ed_fetch64(entry) != srch->key) {
		return ED_EINDEX_KEY_MATCH;
	}

	EdBTree *leaf = NULL;
	int rc = 0;

	// If the root was NULL, create a new root node.
	if (srch->nnodes == 0) {
		EdPg *pg;
		rc = ed_pgalloc(alloc, &pg, 1, true);
		if (rc < 0) { goto done; }
		leaf = (EdBTree *)pg;
		ed_btree_init(leaf);
		srch->nodes[0] = leaf;
		srch->entry = leaf->data;
		srch->entry_index = 0;
		srch->nnodes = 1;
	}
	// Otherwise use the leaf from the search.
	else {
		leaf = srch->nodes[srch->nnodes-1];

		// If the leaf is full, it needs to be redistributed or split.
		if (IS_LEAF_FULL(leaf, srch->entry_size)) {
			rc = insert_redistribute(srch, leaf);
			if (rc < 0) { goto done; }
			if (rc == 0) {
				rc = insert_split(srch, &leaf, alloc);
				if (rc < 0) { goto done; }
			}
		}
	}

	// Insert the new entry before the current entry position.
	uint8_t *p = srch->entry;
	size_t esize = srch->entry_size;
	memmove(p+esize, p, (leaf->nkeys - srch->entry_index) * esize);
	memcpy(p, entry, esize);
	leaf->nkeys++;
	ed_pgsync(leaf, 1, 0, 1);
	*srch->root = srch->nodes[0];
	rc = 0;

	srch->nsplits = 0;
	srch->match = 1;

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
	if (srch->nnodes > 0) {
		// Don't unmap the first node.
		for (int i = srch->nnodes + srch->nextra - 1; i > 0; i--) {
			ed_pgunmap(srch->nodes[i], 1);
			srch->nodes[i] = NULL;
		}
	}
	srch->nnodes = 0;
	srch->match = 0;
	// TODO: make insert impossible
}

