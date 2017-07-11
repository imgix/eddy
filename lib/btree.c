#include "eddy-private.h"
#include "eddy-butil.h"

// TODO: move node list handling into separate functions

/*
 * For branch nodes, the layout of the data segment looks like:
 *
 * 0      4       12     16       24
 * +------+--------+------+--------+-----+----------+------+
 * | P[0] | Key[0] | P[1] | Key[1] | ... | Key[N-1] | P[N] |
 * +------+--------+------+--------+-----+----------+------+
 *
 * The page pointer values (P) are 32-bit numbers as a page offset for the
 * child page. The keys are 64-bit numbers. Each key on P[0] is less than
 * Key[0]. Each key on P[1] is greater or equal to Key[0], and so on. These
 * values are guaranteed to have 4-byte alignment and do not need special
 * handling to read. The Key values are 64-bit and may not be 8-byte aligned.
 * These values need to be acquired using the `ed_fetch64` to accomodate
 * unaligned reads.
 *
 * Leaf nodes use the data segment as an array of entries. Each entry *must*
 * start with a 64-bit key.
 */

_Static_assert(sizeof(EdBTree) == PAGESIZE,
		"EdBTree size invalid");

#define INS_NONE 0
#define INS_SHIFT 1
#define INS_NOSHIFT 2

#define unused __attribute__((unused))

static inline unused uint64_t
branch_key(EdBTree *b, uint16_t idx)
{
	assert(idx <= b->nkeys);
	if (idx == 0) { return 0; }
	return ed_fetch64(b->data + idx*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE);
}

static inline unused void
branch_set_key(EdBTree *b, uint16_t idx, uint64_t val)
{
	assert(idx <= b->nkeys);
	if (idx == 0) { return; }
	memcpy(b->data + idx*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE, &val, sizeof(val));
}

static inline unused EdPgno
branch_ptr(EdBTree *b, uint16_t idx)
{
	assert(idx <= b->nkeys);
	return ed_fetch32(b->data + idx*BRANCH_ENTRY_SIZE);
}

static inline unused void
branch_set_ptr(EdBTree *b, uint16_t idx, EdPgno val)
{
	assert(idx <= b->nkeys);
	memcpy(b->data + idx*BRANCH_ENTRY_SIZE, &val, sizeof(val));
}

static inline unused uint64_t
leaf_key(EdBTree *l, uint16_t idx, size_t esize)
{
	assert(idx < l->nkeys);
	return ed_fetch64(l->data + idx*esize);
}

static inline unused uint16_t
branch_index(EdBTree *node, EdPgno *ptr)
{
	assert(node->data <= (uint8_t *)ptr);
	assert((uint8_t *)ptr < node->data + BRANCH_ORDER*BRANCH_ENTRY_SIZE);
	return ((uint8_t *)ptr - node->data) / BRANCH_ENTRY_SIZE;
}

#define MAP_NODE_NO(s, par, idx, no, name) do { \
	if (s->n##name == (int)ed_len(s->name)) { return ED_EINDEX_DEPTH; } \
	EdPg *pg = ed_pgmap(s->fd, no, 1); \
	if (pg == MAP_FAILED) { return ED_ERRNO; } \
	s->name[s->n##name] = (EdBNode){ (EdBTree *)pg, par, 0, idx }; \
	return s->n##name++; \
} while (0)

#define MAP_NODE(s, par, idx, name) do { \
	assert(idx <= parent->tree->nkeys); \
	EdPgno no = ed_fetch32(parent->tree->data + idx*BRANCH_ENTRY_SIZE); \
	MAP_NODE_NO(s, par, idx, no, name); \
} while (0)

static int
map_node_no(EdBSearch *srch, EdBNode *parent, uint16_t idx, EdPgno no)
{
	MAP_NODE_NO(srch, parent, idx, no, nodes);
}

static int
map_node(EdBSearch *srch, EdBNode *parent, uint16_t idx)
{
	MAP_NODE(srch, parent, idx, nodes);
}

static int
map_extra(EdBSearch *srch, EdBNode *parent, uint16_t idx)
{
	MAP_NODE(srch, parent, idx, extra);
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
	// TODO: binary search or SIMD
	EdPgno *ptr = (EdPgno *)node->data;
	uint8_t *bkey = node->data + BRANCH_PTR_SIZE;
	for (uint32_t i = 0, n = node->nkeys; i < n; i++, bkey += BRANCH_ENTRY_SIZE) {
		uint64_t cmp = ed_fetch64(bkey);
		if (key < cmp) { break; }
		ptr = BRANCH_NEXT(ptr);
		if (key == cmp) { break; }
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
		if (IS_BRANCH_FULL(node)) { srch->nsplits++; }
		EdPgno *ptr = search_branch(node, key);
		int pos = map_node(srch, parent, branch_index(node, ptr));
		if (pos < 0) { rc = pos; goto done; }
		parent++;
		node = parent->tree;
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
		srch->nmatches = rc;
	}
	srch->match = rc;
	return rc;
}

int
ed_bsearch_next(EdBSearch *srch)
{
	if (srch->match != 1) { return srch->match; }

	int rc = 0;
	// FIXME: overflow nodes are going into extra and not getting picked up
	EdBNode *node = &srch->nodes[srch->nnodes-1];
	EdBTree *leaf = node->tree;
	uint8_t *p = srch->entry;
	uint32_t i = srch->entry_index;
	if (i == leaf->nkeys-1) {
		EdPgno right = leaf->right;
		if (right == ED_PAGE_NONE) { goto done; }

		int pos = map_node_no(srch, node, 0, right);
		if (pos < 0) { rc = pos; goto done; }

		leaf = srch->nodes[pos].tree;
		if (leaf->base.type != ED_PGOVERFLOW) { goto done; }

		p = leaf->data;
		i = 0;
	}
	else {
		p += srch->entry_size;
		i++;
	}
	srch->entry = p;
	srch->entry_index = i;
	if (srch->key == ed_fetch64(p)) {
		rc = 1;
		srch->nmatches++;
	}

done:
	srch->match = rc;
	return rc;
}

static int
redistribute_leaf_left(EdBSearch *srch, EdBNode *leaf)
{
	// If the leaf is the first child, don't redistribute.
	if (leaf->pindex == 0) { return INS_NONE; }
	// Don't move left if the new insert is at the beggining.
	if (srch->entry_index == 0) { return INS_NONE; }

	int rc = map_extra(srch, leaf->parent, leaf->pindex-1);
	if (rc < 0) { return rc == -1 ? INS_NONE : rc; }

	uint16_t ord = LEAF_ORDER(srch->entry_size);
	EdBTree *l = srch->extra[rc].tree;

	// Bail if the left leaf is full.
	if (l->nkeys == ord) { return INS_NONE; }

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

	srch->extra[rc].dirty = 1;
	leaf->dirty = 1;

	return INS_SHIFT;
}

static int
redistribute_leaf_right(EdBSearch *srch, EdBNode *leaf)
{
	// If the leaf is the last child, don't redistribute.
	if (leaf->pindex == leaf->parent->tree->nkeys) { return INS_NONE; }
	uint16_t ord = LEAF_ORDER(srch->entry_size);
	// Don't move right if the insert is at the end.
	if (srch->entry_index == ord) { return INS_NONE; }

	int rc = map_extra(srch, leaf->parent, leaf->pindex+1);
	if (rc < 0) { return rc == -1 ? INS_NONE : rc; }

	EdBTree *r = srch->extra[rc].tree;

	// Bail if the right leaf is full.
	if (r->nkeys == ord) { return INS_NONE; }

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

	srch->extra[rc].dirty = 1;
	leaf->dirty = 1;

	return INS_SHIFT;
}

static int
redistribute_leaf(EdBSearch *srch, EdBNode *leaf)
{
	// TODO: fix redistribution for repeat keys
	return INS_NONE;

	if (leaf->parent == NULL) { return INS_NONE; }

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

		// Assign the left pointer. (right is assigned below)
		pos = BRANCH_PTR_SIZE;
		memcpy(p->data, &left->tree->base.no, BRANCH_PTR_SIZE);

		// Insert the new parent into the list.
		left->pindex = 0;
		right->pindex = 1;

		assert(srch->nnodes < (int)ed_len(srch->nodes));
		memmove(srch->nodes+1, srch->nodes, sizeof(srch->nodes[0]) * srch->nnodes);
		srch->nodes[0] = (EdBNode){ p, NULL, 1, 0 };
		srch->nnodes++;

		// Bump parent references to the next node slot after shifting.
		for (uint32_t i = srch->nnodes - 1; i > 0; i--) {
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
			assert(no > rc);

			EdBTree *lb = p, *rb = (EdBTree *)pg[rc++];
			uint32_t n = lb->nkeys;
			uint32_t mid = (n+1) / 2;
			uint64_t rbkey = branch_key(lb, mid);

			rb->base.type = ED_PGBRANCH;
			rb->nkeys = n - mid;
			rb->right = lb->right;
			lb->nkeys = mid - 1;
			lb->right = rb->base.no;

			size_t off = mid * BRANCH_ENTRY_SIZE;
			memcpy(rb->data, lb->data+off, sizeof(lb->data) - off);

			EdBNode *leftb, *rightb;

			// If the entry will stay in the left node, make the right node from exta.
			if (rkey < rbkey) {
				leftb = parent;
				rightb = &srch->extra[srch->nextra++];
				rightb->tree = rb;
				rightb->parent = leftb->parent;
				rightb->pindex = leftb->pindex + 1;

				p = lb;
			}
			// Otherwise swap the branch to take the right node and put left in extra.
			else {
				rightb = parent;
				rightb->tree = rb;
				leftb = &srch->extra[srch->nextra++];
				leftb->tree = lb;
				leftb->parent = rightb->parent;
				leftb->pindex = rightb->pindex;
				rightb->pindex++;

				index -= mid;

				p = rb;
			}

			leftb->dirty = 1;
			rightb->dirty = 1;

			rc += insert_into_parent(srch, leftb, rightb, rbkey, pg+rc, no-rc);
		}

		// The parent has space, so shift space for the right node.
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
split_point(EdBSearch *srch, EdBTree *l)
{
	// TODO: this information is needed for redistribution candidates
	uint16_t n = l->nkeys, mid = n/2, min, max;
	uint64_t key;
	size_t esize = srch->entry_size;

	// The split cannot be between repeated keys.
	// If the searched index is around the mid point, use the key and search position.
	if (srch->nmatches > 0 &&
			mid <= srch->entry_index &&
			mid >= srch->entry_index - srch->nmatches + 1) {
		key = srch->key;
		min = srch->entry_index - srch->nmatches + 1;
		max = srch->entry_index + 1;
	}
	// Otherwise search back for the start of any repeat sequence.
	else {
		key = leaf_key(l, mid, esize);
		if (key == srch->key) {
			min = srch->entry_index - srch->nmatches + 1;
		}
		else {
			for (min = mid; min > 0 && leaf_key(l, min-1, esize) == key; min--) {}
		}
		max = mid + 1;
	}

	// If repeat keys span the mid point, pick the larger side to split on.
	if (min != mid) {
		for (; max < n && leaf_key(l, max, esize) == key; max++) {}
		if (min == 0 && max == n) { return -1; }
		mid = min >= n - max ? min : max;
	}
	return mid;
}

static int
overflow_leaf(EdBSearch *srch, EdBNode *leaf, EdPgalloc *alloc)
{
	assert(leaf->tree->nkeys == LEAF_ORDER(srch->entry_size));

	EdBTree *o;
	size_t esize = srch->entry_size;
	int rc;

	if (leaf->tree->right != ED_PAGE_NONE) {
		o = ed_pgmap(srch->fd, leaf->tree->right, 1);
		if (o == MAP_FAILED) { return ED_ERRNO; }
		if (o->base.type == ED_PGOVERFLOW && o->nkeys < LEAF_ORDER(esize)) {
			goto done;
		}
		ed_pgunmap(o, 1);
	}

	rc = ed_pgalloc(alloc, (EdPg **)&o, 1, true);
	if (rc < 0) { return rc; }

	o->base.type = ED_PGOVERFLOW;
	o->nkeys = 0;
	o->right = leaf->tree->right;
	leaf->tree->right = o->base.no;

done:
	assert(srch->nextra < (int)ed_len(srch->extra));
	srch->extra[srch->nextra++] = (EdBNode){ o, NULL, 1, 0 };
	memcpy(o->data + o->nkeys*esize, srch->entry, esize);
	o->nkeys++;
	return INS_NOSHIFT;
}

static int
insert_split(EdBSearch *srch, EdBNode *leaf, EdPgalloc *alloc)
{
	EdBTree *l = leaf->tree;
	int mid = split_point(srch, l);
	if (mid < 0) {
		return overflow_leaf(srch, leaf, alloc);
	}

	EdPgno npg = srch->nsplits, used = 1;
	EdPg *pg[npg];
	int rc = ed_pgalloc(alloc, pg, npg, true);
	if (rc < 0) { return rc; }

	EdBTree *r = (EdBTree *)pg[0];
	size_t esz = srch->entry_size;
	uint32_t n = l->nkeys;
	size_t off = mid * esz;

	// If the new key will be the first entry on right, use the search key.
	uint64_t rkey = (uint16_t)mid == srch->entry_index ? srch->key : ed_fetch64(l->data+off);

	assert(ed_fetch64(l->data + off - esz) < rkey);
	assert(rkey <= ed_fetch64(l->data + off));

	r->base.type = ED_PGLEAF;
	r->nkeys = n - mid;
	r->right = l->right;
	l->nkeys = mid;
	l->right = r->base.no;
	memcpy(r->data, l->data+off, sizeof(l->data) - off);

	EdBNode *left, *right;

	// If the entry will stay in the left node, make the right node from exta.
	if (srch->entry_index < (uint16_t)mid) {
		left = leaf;
		right = &srch->extra[srch->nextra++];
		right->tree = r;
		right->parent = left->parent;
		right->pindex = left->pindex + 1;
	}
	// Otherwise swap the leaf to take the right node and put left in extra.
	else {
		right = leaf;
		right->tree = r;
		left = &srch->extra[srch->nextra++];
		left->tree = l;
		left->parent = right->parent;
		left->pindex = right->pindex;
		right->pindex++;

		// Update the entry to reference the right node.
		srch->entry_index -= mid;
		srch->entry = r->data + srch->entry_index*esz;
	}

	left->dirty = 1;
	right->dirty = 1;

	// After this call, the left and right nodes could be invalid.
	used += insert_into_parent(srch, left, right, rkey, pg+used, npg-used);

	// Ideally there should be no pages left.
	if (npg > used) {
		fprintf(stderr, "allocated too much! %u -> %u\n", npg, used);
		ed_pgfree(alloc, pg + used, npg - used);
	}

	return INS_SHIFT;
}

int
ed_bsearch_ins(EdBSearch *srch, const void *entry, EdPgalloc *alloc)
{
	if (ed_fetch64(entry) != srch->key) {
		return ED_EINDEX_KEY_MATCH;
	}

	EdBNode *leaf;
	int rc = 0;
	size_t esize = srch->entry_size;

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
		rc = INS_SHIFT;
	}
	// Otherwise use the leaf from the search.
	else {
		leaf = &srch->nodes[srch->nnodes-1];

		// If the leaf is full, it needs to be redistributed or split.
		if (IS_LEAF_FULL(leaf->tree, esize)) {
			rc = redistribute_leaf(srch, leaf);
			if (rc < 0) { goto done; }
			if (rc == INS_NONE) {
				rc = insert_split(srch, leaf, alloc);
				if (rc < 0) { goto done; }
			}
			// TODO: mark parent as dirty

			leaf = &srch->nodes[srch->nnodes-1];
		}
		else {
			rc = INS_SHIFT;
		}
	}

	// Insert the new entry before the current entry position.
	uint8_t *p = srch->entry;
	assert(srch->entry_index < LEAF_ORDER(esize));
	assert(p == leaf->tree->data + (srch->entry_index*esize));

	if (rc == INS_SHIFT) {
		memmove(p+esize, p, (leaf->tree->nkeys - srch->entry_index) * esize);
		leaf->tree->nkeys++;
	}
	memcpy(p, entry, esize);
	if (srch->entry_index == 0 && leaf->parent != NULL) {
		branch_set_key(leaf->parent->tree, leaf->pindex, ed_fetch64(p));
	}
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
	int n = srch->nnodes;
	if (n > 0) {
		// Don't unmap the first node.
		for (int i = 1; i < n; i++) { ed_pgunmap(srch->nodes[i].tree, 1); }
		memset(srch->nodes, 0, sizeof(srch->nodes));
		srch->nnodes = 0;
	}
	n = srch->nextra;
	for (int i = 0; i < n; i++) { ed_pgunmap(srch->extra[i].tree, 1); }
	srch->nextra = 0;
	srch->match = 0;
	// TODO: make insert impossible
}

