#include "eddy-private.h"

// TODO: move node list handling into separate functions

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

static inline uint64_t __attribute__((unused))
branch_key(EdBTree *b, uint16_t idx)
{
	assert(idx <= b->nkeys);
	if (idx == 0) { return 0; }
	return ed_fetch64(b->data + idx*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE);
}

static inline void __attribute__((unused))
branch_set_key(EdBTree *b, uint16_t idx, uint64_t val)
{
	assert(idx <= b->nkeys);
	if (idx == 0) { return; }
	memcpy(b->data + idx*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE, &val, sizeof(val));
}

static inline EdPgno __attribute__((unused))
branch_ptr(EdBTree *b, uint16_t idx)
{
	assert(idx <= b->nkeys);
	return ed_fetch32(b->data + idx*BRANCH_ENTRY_SIZE);
}

static inline void __attribute__((unused))
branch_set_ptr(EdBTree *b, uint16_t idx, EdPgno val)
{
	assert(idx <= b->nkeys);
	memcpy(b->data + idx*BRANCH_ENTRY_SIZE, &val, sizeof(val));
}



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

size_t
ed_btree_capacity(size_t esize, size_t depth)
{
	return llround(pow(BRANCH_ORDER, depth-1) * LEAF_ORDER(esize));
}

int
ed_btree_search(EdBTree **root, int fd, uint64_t key, size_t esize, EdBSearch *srch)
{
	int rc = 0;
	uint32_t i = 0, n = 0;
	uint8_t *data = NULL;
	EdBTree *node = *root;

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

	EdBNode *parent = srch->nodes;
	
	// The root node needs two pages when splitting.
	srch->nsplits = IS_FULL(node, esize);

	// Search down the branches of the tree.
	while (IS_BRANCH(node)) {
		if (srch->nnodes == ed_len(srch->nodes)) {
			rc = ED_EINDEX_DEPTH;
			goto done;
		}
		if (IS_BRANCH_FULL(node)) { srch->nsplits++; }
		EdPgno *ptr = search_branch(node, key);
		node = ed_pgmap(fd, *ptr, 1);
		if (node == MAP_FAILED) { rc = ED_ERRNO; goto done; }
		srch->nodes[srch->nnodes++] = (EdBNode){ node, parent, 0,
			((uint8_t *)ptr - parent->tree->data) / BRANCH_ENTRY_SIZE };
		parent++;
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

static int
map_extra(EdBSearch *srch, EdBNode *parent, uint16_t idx)
{
	assert(idx <= parent->tree->nkeys);

	EdPgno no = ed_fetch32(parent->tree->data + idx*BRANCH_ENTRY_SIZE);

	int pos = srch->nnodes + srch->nextra;
	for (int i = pos - 1; i >= 0; i--) {
		if (srch->nodes[i].tree->base.no == no) {
			return i;
		}
	}

	if (pos >= (int)ed_len(srch->nodes)) {
		return -1;
	}
	
	EdPg *pg = ed_pgmap(srch->fd, no, 1);
	if (pg == MAP_FAILED) { return ED_ERRNO; }
	srch->nodes[pos] = (EdBNode){ (EdBTree *)pg, parent, 0, idx };
	srch->nextra++;
	return pos;
}

#if 0
static void
print_branch(EdBTree *branch, FILE *out)
{
	if (out == NULL) { out = stderr; }
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
print_leaf(EdBTree *leaf, size_t esize, FILE *out)
{
	if (out == NULL) { out = stderr; }

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
			fprintf(out, " %-11llu |", ed_fetch64(p));
		}
		fprintf(out, "\n+");
		for (uint32_t i = 0; i < n; i++) {
			fprintf(out, "-------------+");
		}
		fprintf(out, "\n");
	}
}

static void
verify_branch(EdBTree *b)
{
	uint64_t key = 0;
	EdPgno ptr = ed_fetch32(b->data);
	uint8_t *p = b->data + BRANCH_PTR_SIZE;

	for (uint16_t i = 0; i < b->nkeys; i++, p += BRANCH_ENTRY_SIZE) {
		uint64_t nkey = ed_fetch64(p);
		EdPgno nptr = ed_fetch32(p + BRANCH_KEY_SIZE);
		if (key >= nkey) {
			abort();
		}
		key = nkey;
		ptr = nptr;
	}
}
#endif

static int
redistribute_leaf_left(EdBSearch *srch, EdBNode *leaf)
{
	// If the leaf is the first child, don't redistribute.
	if (leaf->pindex == 0) { return 0; }
	// Don't move left if the new insert is at the beggining.
	if (srch->entry_index == 0) { return 0; }

	int rc = map_extra(srch, leaf->parent, leaf->pindex-1);
	if (rc < 0) { return rc == -1 ? 0 : rc; }

	uint16_t ord = LEAF_ORDER(srch->entry_size);
	EdBTree *l = srch->nodes[rc].tree;

	// Bail if the left leaf is full.
	if (l->nkeys == ord) { return 0; }

	// Move half the available nodes into the left leaf.
	uint32_t n = (ord - l->nkeys + 1) / 2;
	// But don't move past the entry index.
	uint32_t max = srch->entry_index;
	if (n > max) { n = max; }

	// Move the lower range of leaf into the end of left.
	size_t size = n * srch->entry_size;
	uint8_t *src = leaf->tree->data;
	uint8_t *dst = l->data + srch->entry_size*l->nkeys;
	memcpy(dst, src, size);
	memmove(src, src+size, (leaf->tree->nkeys - n) * srch->entry_size);
	branch_set_key(leaf->parent->tree, leaf->pindex, ed_fetch64(src));

	// Update leaf counts.
	l->nkeys += n;
	leaf->tree->nkeys -= n;

	// Slide the entry index down.
	srch->entry_index -= n;
	srch->entry = leaf->tree->data + srch->entry_index*srch->entry_size;

	srch->nodes[rc].dirty = 1;
	leaf->dirty = 1;

	return 1;
}

static int
redistribute_leaf_right(EdBSearch *srch, EdBNode *leaf)
{
	// If the leaf is the last child, don't redistribute.
	if (leaf->pindex == leaf->parent->tree->nkeys) { return 0; }
	uint16_t ord = LEAF_ORDER(srch->entry_size);
	// Don't move right if the insert is at the end.
	if (srch->entry_index == ord) { return 0; }

	int rc = map_extra(srch, leaf->parent, leaf->pindex+1);
	if (rc < 0) { return rc == -1 ? 0 : rc; }

	EdBTree *r = srch->nodes[rc].tree;

	// Bail if the right leaf is full.
	if (r->nkeys == ord) { return 0; }

	// Move half the available nodes into the right leaf.
	uint32_t n = (ord - r->nkeys + 1) / 2;
	// But don't move past the entry index.
	uint32_t max = (uint32_t)ord - srch->entry_index;
	if (n > max) { n = max; }

	// Move upper range of leaf into the start of right.
	size_t size = n * srch->entry_size;
	uint8_t *src = leaf->tree->data + srch->entry_size*(ord - n);
	uint8_t *dst = r->data;
	memmove(dst+size, dst, r->nkeys*srch->entry_size);
	memcpy(dst, src, size);
	branch_set_key(leaf->parent->tree, leaf->pindex+1, ed_fetch64(dst));

	// Update leaf counts.
	r->nkeys += n;
	leaf->tree->nkeys -= n;

	srch->nodes[rc].dirty = 1;
	leaf->dirty = 1;

	return 1;
}

static int
redistribute_leaf(EdBSearch *srch, EdBNode *leaf)
{
	if (leaf->parent == NULL) { return 0; }

	int rc = redistribute_leaf_right(srch, leaf);
	if (rc == 0) {
		rc = redistribute_leaf_left(srch, leaf);
	}
	return rc;
}

static EdPgno
insert_into_parent(EdBSearch *srch, EdBNode *left, EdBNode *right, uint64_t rkey, EdPg **pg, EdPgno no)
{
	EdBNode *parent = left->parent;
	EdBTree *p;
	EdPgno r = right->tree->base.no, rc = 0;
	size_t pos;

	// No parent, so create new root node.
	if (parent == NULL) {
		assert(no > rc);

		// Initialize new parent node from the allocation array.
		p = (EdBTree *)pg[rc++];
		p->base.type = ED_PGBRANCH;
		p->nkeys = 0;
		p->right = ED_PAGE_NONE;

		// Assign the left pointer.
		pos = BRANCH_PTR_SIZE;
		memcpy(p->data, &left->tree->base.no, BRANCH_PTR_SIZE);

		// Insert the new parent into the list.
		parent = srch->nodes;
		left->pindex = 0;
		right->pindex = 1;
		memmove(srch->nodes+1, srch->nodes, sizeof(srch->nodes[0]) * (srch->nnodes+srch->nextra));
		srch->nodes[0] = (EdBNode){ p, NULL, 1, 0 };
		srch->nnodes++;
		for (uint32_t i = srch->nnodes + srch->nextra - 1; i > 0; i--) {
			if (srch->nodes[i].parent) {
				srch->nodes[i].parent++;
			}
		}
	}
	else {
		uint32_t index = left->pindex;
		p = parent->tree;

		// The parent branch is full, so it gets split.
		if (IS_BRANCH_FULL(p)) {
#if 0
			// TODO
			assert(no > rc);

			EdBTree *lb = parent, *rb = (EdBTree *)pg[rc++];
			uint32_t n = lb->nkeys;
			uint32_t mid = n / 2;

			rb->base.type = ED_PGBRANCH;
			rb->nkeys = 0;
			rb->right = lb->right;
			lb->right = rb->base.no;

			rc += insert_into_parent(srch, leftb, rightb, rbkey, pg+rc, no-rc);
#else
			fprintf(stderr, "Not Implemented!\n");
			abort();
#endif
		}

		// The parent has space, so redistribute.
		pos = BRANCH_PTR_SIZE + index*BRANCH_ENTRY_SIZE;
		memmove(p->data+pos+BRANCH_ENTRY_SIZE, p->data+pos,
				(p->nkeys-index)*BRANCH_ENTRY_SIZE);
	}

	// Insert the new right node.
	memcpy(p->data+pos, &rkey, BRANCH_KEY_SIZE);
	memcpy(p->data+pos+BRANCH_KEY_SIZE, &r, BRANCH_PTR_SIZE);
	p->nkeys++;

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
	uint32_t n = l->nkeys;
	uint32_t mid = n / 2;
	size_t off = mid * srch->entry_size;

	r->base.type = ED_PGLEAF;
	r->nkeys = n - mid;
	r->right = l->right;
	l->nkeys = mid;
	l->right = r->base.no;

	memcpy(r->data, l->data+off, sizeof(l->data) - off);

	uint64_t key = mid == srch->entry_index ? srch->key : ed_fetch64(r->data);

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
			rc = redistribute_leaf(srch, leaf);
			if (rc < 0) { goto done; }
			if (rc == 0) {
				rc = insert_split(srch, leaf, alloc);
				if (rc < 0) { goto done; }
			}
			// TODO mark parent as dirty
		}

		// An insert may have moved the leaf node.
		leaf = &srch->nodes[srch->nnodes-1];
	}

	// Insert the new entry before the current entry position.
	uint8_t *p = srch->entry;
	assert(srch->entry_index < LEAF_ORDER(srch->entry_size));
	assert(p == leaf->tree->data + (srch->entry_index*srch->entry_size));

	size_t esize = srch->entry_size;
	memmove(p+esize, p, (leaf->tree->nkeys - srch->entry_index) * esize);
	memcpy(p, entry, esize);
	if (srch->entry_index == 0 && leaf->parent != NULL) {
		branch_set_key(leaf->parent->tree, leaf->pindex, ed_fetch64(p));
	}
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

