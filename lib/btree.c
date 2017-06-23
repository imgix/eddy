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

#define BRANCH_ORDER \
	((sizeof(((EdBTree *)0)->data) - sizeof(EdPgno)) / (sizeof(EdPgno) + 8))

#define LEAF_ORDER(ent) \
	(sizeof(((EdBTree *)0)->data) / (ent))

//static bool isroot(const EdBTree *bt) { return bt->base.no == bt->parent; }

void
ed_btree_init(EdBTree *bt)
{
	bt->base.type = ED_PGLEAF;
	bt->count = 0;
	bt->parent = ED_PAGE_NONE;
	bt->right = ED_PAGE_NONE;
}

static EdPgno *
search_branch(EdBTree *node, uint64_t key)
{
	EdPgno *ptr = (EdPgno *)node->data;
	uint8_t *cmp = node->data + sizeof(EdPgno);
	for (uint32_t i = 0, n = node->count; i < n && key >= ed_fetch64(cmp);
			i++, ptr = (EdPgno *)(cmp + 8), cmp += 8+sizeof(EdPgno)) {}
	return ptr;
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
	srch->nsplits = 0;

	// The NULL node is an empty tree.
	if (node == NULL) { goto done; }

	// Search down the branches of the tree.
	while (node->base.type == ED_PGBRANCH) {
		if (srch->nnodes == ed_len(srch->nodes)) {
			rc = ED_EINDEX_DEPTH;
			goto done;
		}
		EdPgno *ptr = search_branch(node, key);
		node = ed_pgmap(fd, *ptr, 1);
		if (node == MAP_FAILED) { rc = ED_ERRNO; goto done; }
		srch->nodes[srch->nnodes++] = node;
		if (node->count == BRANCH_ORDER) {
			srch->nsplits++;
			if (node == *root) { srch->nsplits++; }
		}
	}

	// Search the leaf node.
	data = node->data;
	for (i = 0, n = node->count; i < n; i++, data += esize) {
		uint64_t cmp = ed_fetch64(data);
		if (key == cmp) {
			rc = 1;
			break;
		}
		else if (key > cmp) {
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
	if (i == leaf->count-1) {
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

#if 0
static int
map_extra(EdBSearch *srch, EdBTree **p, EdPgno no)
{
	int pos = srch->nnodes + srch->nextra;
	for (int i = pos - 1; i > 0; i--) {
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
	if (right->count == LEAF_ORDER(srch->entry_size)) { goto done; }
	if (right->parent != leaf->parent) { goto done; }

	rcparent = map_extra(srch, &parent, leaf->parent);
	if (rcparent < 0) { rc = rcparent; goto done; }

	uint8_t *p = leaf->data + (leaf->count-1)*srch->entry_size;
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

static int
insert_split(EdBSearch *srch, EdBTree **leafp, EdPgalloc *alloc)
{
	EdPg *pg[srch->nsplits+1];
	int rc = ed_pgalloc(alloc, pg, srch->nsplits+1);
	if (rc < 0) { return rc; }

	EdBTree *left = *leafp, *right = (EdBTree *)pg[0];
	ed_btree_init(right);

	right->right = left->right;
	left->right = right->base.no;

	uint32_t n = left->count;
	uint32_t mid = n / 2;
	size_t off = mid * srch->entry_size;
	left->count = mid;
	right->count = n - mid;
	memcpy(right->data, left->data+off, sizeof(left->data) - off);

	// TODO: insert into parent

	if (srch->entry_index < mid) {
		*leafp = left;
		ed_pgunmap(right, 1);
	}
	else {
		*leafp = right;
		ed_pgunmap(left, 1);
		srch->nodes[srch->nnodes-1] = right;
		srch->entry = right->data + srch->entry_size*mid;
		srch->entry_index -= mid;
	}
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
		rc = ed_pgalloc(alloc, &pg, 1);
		if (rc < 0) { goto done; }
		leaf = (EdBTree *)pg;
		ed_btree_init(leaf);
		srch->nodes[0] = leaf;
		srch->entry = leaf->data;
		srch->entry_index = 0;
		srch->nnodes = 1;
		*srch->root = leaf;
	}
	// Otherwise use the leaf from the search.
	else {
		leaf = srch->nodes[srch->nnodes-1];

		// If the leaf is full, it needs to be redistributed or split.
		if (leaf->count == LEAF_ORDER(srch->entry_size)) {
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
	memmove(p+esize, p, (leaf->count - srch->entry_index) * esize);
	memcpy(p, entry, esize);
	leaf->count++;
	ed_pgsync(leaf, 1, 0, 1);
	*srch->root = srch->nodes[0];
	rc = 0;

done:
	ed_bsearch_final(srch);
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

