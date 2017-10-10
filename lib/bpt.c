#include "eddy-private.h"

_Static_assert(sizeof(EdBpt) == PAGESIZE,
		"EdBpt size invalid");
_Static_assert(offsetof(EdBpt, data) % 8 == 0,
		"EdBpt data not 8-byte aligned");

#define BRANCH_KEY_SIZE 8
#define BRANCH_PTR_SIZE (sizeof(EdPgno))
#define BRANCH_ENTRY_SIZE (BRANCH_PTR_SIZE + BRANCH_KEY_SIZE)
#define BRANCH_NEXT(pg) ((EdPgno *)((uint8_t *)(pg) + BRANCH_ENTRY_SIZE))

#define BRANCH_ORDER \
	(((sizeof(((EdBpt *)0)->data) - BRANCH_PTR_SIZE) / BRANCH_ENTRY_SIZE) + 1)

#define LEAF_ORDER(esize) \
	(sizeof(((EdBpt *)0)->data) / (esize))

#define IS_BRANCH(n) ((n)->base.type == ED_PG_BRANCH)
#define IS_BRANCH_FULL(n) ((n)->nkeys == (BRANCH_ORDER-1))
#define IS_LEAF_FULL(n, esize) ((n)->nkeys == LEAF_ORDER(esize))
#define IS_FULL(n, esize) (IS_BRANCH(n) ? IS_BRANCH_FULL(n) : IS_LEAF_FULL(n, esize))

static inline uint64_t
branch_key(EdBpt *b, uint16_t idx)
{
	assert(idx <= b->nkeys);
	if (idx == 0) { return 0; }
	return ed_fetch64(b->data + idx*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE);
}

static inline EdPgno
branch_ptr(EdBpt *b, uint16_t idx)
{
	assert(idx <= b->nkeys);
	return ed_fetch32(b->data + idx*BRANCH_ENTRY_SIZE);
}

static inline void
branch_set_key(EdBpt *b, uint16_t idx, uint64_t val)
{
	assert(idx <= b->nkeys);
	if (idx == 0) { return; }
	memcpy(b->data + idx*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE, &val, sizeof(val));
}

static inline void
branch_set_ptr(EdBpt *b, uint16_t idx, EdPgno val)
{
	assert(idx <= b->nkeys);
	memcpy(b->data + idx*BRANCH_ENTRY_SIZE, &val, sizeof(val));
}

static inline uint16_t
branch_index(EdBpt *b, EdPgno *ptr)
{
	assert(b->data <= (uint8_t *)ptr);
	assert((uint8_t *)ptr < b->data + (BRANCH_ORDER*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE));
	return ((uint8_t *)ptr - b->data) / BRANCH_ENTRY_SIZE;
}

static EdPgno *
branch_search(EdBpt *b, uint64_t key)
{
	// TODO: binary search or SIMD
	EdPgno *ptr = (EdPgno *)b->data;
	uint8_t *bkey = b->data + BRANCH_PTR_SIZE;
	for (uint32_t i = 0, n = b->nkeys; i < n; i++, bkey += BRANCH_ENTRY_SIZE) {
		uint64_t cmp = ed_fetch64(bkey);
		if (key < cmp) { break; }
		ptr = BRANCH_NEXT(ptr);
		if (key == cmp) { break; }
	}
	return ptr;
}

static inline uint64_t
leaf_key(EdBpt *l, uint16_t idx, size_t esize)
{
	assert(idx < l->nkeys);
	return ed_fetch64(l->data + idx*esize);
}


size_t
ed_branch_order(void)
{
	return BRANCH_ORDER;
}

size_t
ed_leaf_order(size_t esize)
{
	return LEAF_ORDER(esize);
}

size_t
ed_bpt_capacity(size_t esize, size_t depth)
{
	return llround(pow(BRANCH_ORDER, depth-1) * LEAF_ORDER(esize));
}

int
ed_bpt_find(EdTxn *txn, unsigned db, uint64_t key, void **ent)
{
	if (txn->state != ED_TXN_OPEN) {
		// FIXME: return proper error code
		return ed_esys(EINVAL);
	}

	EdTxnDb *dbp = ed_txn_db(txn, db, true);

	int rc = 0;
	uint32_t i = 0, n = 0;
	uint8_t *data = NULL;
	size_t esize = dbp->entry_size;
	uint64_t kmin = 0, kmax = UINT64_MAX;
	EdNode *node = dbp->root;
	if (node == NULL) {
		dbp->nsplits = 1;
		goto done;
	}

	// The root node needs two pages when splitting.
	dbp->nsplits = IS_FULL(node->tree, esize);

	// Search down the branches of the tree.
	while (IS_BRANCH(node->tree)) {
		if (IS_BRANCH_FULL(node->tree)) { dbp->nsplits++; }
		else { dbp->nsplits = 0; }
		EdPgno *ptr = branch_search(node->tree, key);
		uint16_t bidx = branch_index(node->tree, ptr);
		EdPgno no = ed_fetch32(node->tree->data + bidx*BRANCH_ENTRY_SIZE);
		EdNode *next;
		rc = ed_txn_map(txn, no, node, bidx, &next);
		if (rc < 0) { goto done; }
		if (bidx > 0) {
			kmin = branch_key(node->tree, bidx);
		}
		if (bidx < node->tree->nkeys) {
			kmax = branch_key(node->tree, bidx + 1) - 1;
		}
		node = next;
		dbp->find = node;
	}
	if (IS_LEAF_FULL(node->tree, esize)) { dbp->nsplits++; }
	else { dbp->nsplits = 0; }

	// Search the leaf node.
	data = node->tree->data;
	for (i = 0, n = node->tree->nkeys; i < n; i++, data += esize) {
		uint64_t cmp = ed_fetch64(data);
		if (key == cmp) {
			kmax = cmp;
			rc = 1;
			break;
		}
		else if (key < cmp) {
			kmax = cmp;
			break;
		}
		kmin = cmp;
	}

done:
	if (rc >= 0) {
		dbp->key = key;
		dbp->kmin = kmin;
		dbp->kmax = kmax;
		dbp->entry = dbp->start = data;
		dbp->entry_index = i;
		dbp->nmatches = rc;
		dbp->nloops = 0;
		dbp->hasfind = true;
		dbp->haskey = true;
		if (rc == 1) {
			dbp->hasentry = true;
			if (ent) { *ent = data; }
		}
		else {
			dbp->hasentry = false;
			if (ent) { *ent = NULL; }
		}
	}
	dbp->match = rc;
	return rc;
}

static uint64_t
find_kmin(EdNode *node)
{
	if (node->parent == NULL) {
		return 0;
	}
	if (node->pindex > 0) {
		return branch_key(node->parent->tree, node->pindex-1) - 1;
	}
	return find_kmin(node->parent);
}

static uint64_t
find_kmax(EdNode *node)
{
	if (node->parent == NULL) {
		return UINT64_MAX;
	}
	if (node->pindex < node->parent->tree->nkeys) {
		return branch_key(node->parent->tree, node->pindex+1) - 1;
	}
	return find_kmax(node->parent);
}

/**
 * @brief   Move ths db find to the first entry from a start point
 * @param  txn  Transaction object
 * @param  dbp  Transaction database object
 * @param  from  Node to move from
 * @param  kmin  The current minimum key value range
 * @param  kmax  The current maximum key value range
 * @return 0 on succces, <0 on error
 */
static int
move_first(EdTxn *txn, EdTxnDb *dbp, EdNode *from, uint64_t kmin, uint64_t kmax)
{
	int rc = 0;
	if (from == NULL) { goto done; }

	while (IS_BRANCH(from->tree)) {
		EdPgno no = branch_ptr(from->tree, 0);
		EdNode *next;
		rc = ed_txn_map(txn, no, from, 0, &next);
		if (rc < 0) { goto done; }
		kmax = branch_key(from->tree, 1) - 1;
		from = next;
	}
	dbp->find = from;
	if (from->tree->nkeys > 0) {
		kmax = leaf_key(from->tree, 0, dbp->entry_size);
	}

done:
	if (rc >= 0) {
		dbp->kmin = kmin;
		dbp->kmax = kmax;
		dbp->hasentry = true;
		dbp->entry = dbp->find->tree->data;
		dbp->entry_index = 0;
	}
	dbp->match = rc;
	dbp->nmatches = 0;
	return rc;
}

/**
 * @brief   Move ths db find to the last entry from a start point
 * @param  txn  Transaction object
 * @param  dbp  Transaction database object
 * @param  from  Node to move from
 * @param  kmin  The current minimum key value range
 * @param  kmax  The current maximum key value range
 * @return 0 on succces, <0 on error
 */
static int
move_last(EdTxn *txn, EdTxnDb *dbp, EdNode *from, uint64_t kmin, uint64_t kmax)
{
	int rc = 0;
	if (from == NULL) { goto done; }

	while (IS_BRANCH(from->tree)) {
		EdPgno no = branch_ptr(from->tree, from->tree->nkeys);
		EdNode *next;
		rc = ed_txn_map(txn, no, from, from->tree->nkeys, &next);
		if (rc < 0) { goto done; }
		kmin = branch_key(from->tree, from->tree->nkeys);
		from = next;
	}
	dbp->find = from;
	if (from->tree->nkeys > 0) {
		kmin = leaf_key(from->tree, from->tree->nkeys - 1, dbp->entry_size);
	}

done:
	if (rc >= 0) {
		dbp->kmin = kmin;
		dbp->kmax = kmax;
		dbp->hasentry = true;
		dbp->entry = dbp->find->tree->data + (dbp->find->tree->nkeys - 1) * dbp->entry_size;
		dbp->entry_index = dbp->find->tree->nkeys - 1;
	}
	dbp->match = rc;
	dbp->nmatches = 0;
	return rc;
}

/**
 * @brief  Moves the db find to a right sibling node
 * @param  txn  Transaction object
 * @param  dbp  Transaction database object
 * @param  from  Node to move from
 * @return 0 on succces, <0 on error
 */
static int
move_right(EdTxn *txn, EdTxnDb *dbp, EdNode *from)
{
	assert(from->page->type == ED_PG_LEAF);

	uint64_t kmin, kmax;

	// Traverse up the nearest node that isn't the last key of its parent.
	do {
		// When at the root, wrapping is the only option.
		if (from->parent == NULL) {
			kmin = 0;
			kmax = UINT64_MAX;
			break;
		}
		// If a child node is not the last key, load the sibling.
		if (from->pindex < from->parent->tree->nkeys) {
			EdPgno no = branch_ptr(from->parent->tree, from->pindex+1);
			int rc = ed_txn_map(txn, no, from->parent, from->pindex+1, &from);
			if (rc < 0) { return rc; }
			kmin = branch_key(from->parent->tree, from->pindex);
			kmax = find_kmax(from);
			break;
		}
		from = from->parent;
	} while (1);

	// Traverse down to the left-most leaf.
	return move_first(txn, dbp, from, kmin, kmax);
}

/**
 * @brief  Moves the db find to a left sibling node
 * @param  txn  Transaction object
 * @param  dbp  Transaction database object
 * @param  from  Node to move from
 * @return 0 on succces, <0 on error
 */
static int
move_left(EdTxn *txn, EdTxnDb *dbp, EdNode *from)
{
	assert(from->page->type == ED_PG_LEAF);

	uint64_t kmin, kmax;

	// Traverse up the nearest node that isn't the last key of its parent.
	do {
		// When at the root, wrapping is the only option.
		if (from->parent == NULL) {
			kmin = 0;
			kmax = UINT64_MAX;
			break;
		}
		// If a child node is not the first key, load the sibling.
		if (from->pindex > 0) {
			EdPgno no = branch_ptr(from->parent->tree, from->pindex-1);
			int rc = ed_txn_map(txn, no, from->parent, from->pindex-1, &from);
			if (rc < 0) { return rc; }
			kmin = find_kmin(from);
			kmax = branch_key(from->parent->tree, from->pindex);
			break;
		}
		from = from->parent;
	} while (1);

	// Traverse down to the left-most leaf.
	return move_last(txn, dbp, from, kmin, kmax);
}

int
ed_bpt_first(EdTxn *txn, unsigned db, void **ent)
{
	EdTxnDb *dbp = &txn->db[db];

	int rc = move_first(txn, dbp, dbp->root, 0, UINT64_MAX);
	if (rc == 0) {
		dbp->start = dbp->entry;
		dbp->nloops = 0;
		dbp->hasfind = true;
		if (ent) { *ent = dbp->entry; }
	}
	return rc;
}

int
ed_bpt_last(EdTxn *txn, unsigned db, void **ent)
{
	EdTxnDb *dbp = &txn->db[db];

	int rc = move_last(txn, dbp, dbp->root, 0, UINT64_MAX);
	if (rc == 0) {
		dbp->start = dbp->entry;
		dbp->nloops = 0;
		dbp->hasfind = true;
		if (ent) { *ent = dbp->entry; }
	}
	return rc;
}

int
ed_bpt_next(EdTxn *txn, unsigned db, void **ent)
{
	EdTxnDb *dbp = &txn->db[db];
	if (!dbp->hasfind) { return ED_EINDEX_KEY_MATCH; }
	if (dbp->find == NULL) { return 0; }

	int rc = 0;
	uint32_t i = dbp->entry_index;
	if (dbp->hasentry) { i++; }

	if (i >= dbp->find->tree->nkeys) {
		rc = move_right(txn, dbp, dbp->find);
		if (rc < 0) { goto error; }
	}
	else if (dbp->hasentry) {
		dbp->entry = (uint8_t *)dbp->entry + dbp->entry_size;
		dbp->entry_index++;
		dbp->kmin = dbp->kmax;
		dbp->kmax = ed_fetch64(dbp->entry);
	}
	else {
		dbp->hasentry = true;
	}

	if (dbp->haskey) {
		if (dbp->key == ed_fetch64(dbp->entry)) {
			dbp->nmatches++;
			rc = 1;
		}
		else {
			dbp->haskey = false;
		}
	}

	if (ent) { *ent = dbp->entry; }
	if (dbp->entry == dbp->start) { dbp->nloops++; }
	dbp->match = rc;
	return rc;

error:
	dbp->match = 0;
	txn->error = rc;
	return rc;
}

int
ed_bpt_prev(EdTxn *txn, unsigned db, void **ent)
{
	EdTxnDb *dbp = &txn->db[db];
	if (!dbp->hasfind) { return ED_EINDEX_KEY_MATCH; }
	if (dbp->find == NULL) { return 0; }

	int rc = 0;
	uint32_t i = dbp->entry_index;

	if (i == 0) {
		rc = move_left(txn, dbp, dbp->find);
		if (rc < 0) { goto error; }
	}
	else {
		dbp->entry = (uint8_t *)dbp->entry - dbp->entry_size;
		dbp->entry_index--;
		dbp->kmax = dbp->kmin;
		dbp->kmin = ed_fetch64(dbp->entry);
		dbp->hasentry = true;
	}

	if (dbp->haskey) {
		if (dbp->key == ed_fetch64(dbp->entry)) {
			dbp->nmatches++;
			rc = 1;
		}
		else {
			dbp->haskey = false;
		}
	}

	if (ent) { *ent = dbp->entry; }
	if (dbp->entry == dbp->start) { dbp->nloops++; }
	dbp->match = rc;
	return rc;

error:
	dbp->match = 0;
	txn->error = rc;
	return rc;
}

int
ed_bpt_loop(const EdTxn *txn, unsigned db)
{
	return txn->db[db].nloops;
}

static int
set_node(EdTxn *txn, EdTxnDb *dbp, EdNode *node)
{
	EdNode *parent = node->parent;
	if (parent == NULL) {
		dbp->root = node;
		return 0;
	}
	assert(parent->page->type == ED_PG_BRANCH);
	if (parent->tree->xid < txn->xid) {
		EdNode *src = parent;
		int rc = ed_txn_clone(txn, src, &parent);
		if (rc < 0) { return rc; }
		memcpy(parent->tree->data, src->tree->data,
				src->tree->nkeys*BRANCH_ENTRY_SIZE + BRANCH_PTR_SIZE);
		node->parent = parent;
	}
	if (node->tree->xid == txn->xid) {
		branch_set_ptr(parent->tree, node->pindex, node->page->no);
	}
	return set_node(txn, dbp, parent);
}

static int
set_leaf(EdTxn *txn, EdTxnDb *dbp, EdNode *leaf, uint32_t eidx)
{
	int rc = set_node(txn, dbp, leaf);
	if (rc == 0 && eidx == 0 && leaf->pindex > 0) {
		branch_set_key(leaf->parent->tree, leaf->pindex, ed_fetch64(leaf->tree->data));
	}
	return 0;
}

static int
insert_into_parent(EdTxn *txn, EdTxnDb *dbp, EdNode *l, EdNode *r, uint64_t rkey)
{
	assert(r->pindex == l->pindex + 1);
	EdNode *branch = l->parent;
	uint32_t eidx = l->pindex;

	// When the branch is NULL, we have a new root of the tree.
	if (branch == NULL) {
		int rc = ed_txn_alloc(txn, NULL, 0, &branch);
		if (rc < 0) { return rc; }
		branch->page->type = ED_PG_BRANCH;
		branch->tree->next = ED_PG_NONE;
		branch->tree->nkeys = 1;
	}
	// If the branch is full, it needs to be split. This splitting approach is
	// less efficent than the way leaves are split: entry positions are fully
	// copied and then shifted at the insertion point. But given the additional
	// complexity of splitting branch data, and the relatively infrequent need to
	// do so, this hasn't been improved.
	else if (IS_BRANCH_FULL(branch->tree)) {
		int mid = (branch->tree->nkeys+1) / 2;
		size_t off = mid * BRANCH_ENTRY_SIZE;
		uint64_t rbkey = branch_key(branch->tree, mid);

		EdNode *left = branch, *right;
		int rc = ed_txn_alloc(txn, left->parent, left->pindex + 1, &right);
		if (rc < 0) { return rc; }
		if (branch->tree->xid < txn->xid) {
			rc = ed_txn_clone(txn, branch, &left);
			if (rc < 0) { return rc; }
			// Copy all entries left of the mid point.
			memcpy(left->tree->data, branch->tree->data, off - BRANCH_KEY_SIZE);
		}

		right->page->type = ED_PG_BRANCH;
		right->tree->next = ED_PG_NONE;
		right->tree->nkeys = branch->tree->nkeys - mid;
		left->tree->nkeys = mid - 1;

		// Copy all entries right of the mid point.
		memcpy(right->tree->data, branch->tree->data+off, sizeof(branch->tree->data) - off);

		// The new entry goes on the left-side branch.
		if (rkey < rbkey) {
			branch = left;
		}
		// The new entry goes on the right-side branch.
		else {
			branch = right;
			eidx -= mid;
			l->pindex = eidx;
			r->pindex = eidx + 1;
		}

		// Shift the new branch entries to make room for the new entry.
		size_t pos = BRANCH_PTR_SIZE + eidx*BRANCH_ENTRY_SIZE;
		memmove(branch->tree->data + pos + BRANCH_ENTRY_SIZE,
				branch->tree->data + pos,
				(branch->tree->nkeys - eidx) * BRANCH_ENTRY_SIZE);

		branch->tree->nkeys++;

		rc = insert_into_parent(txn, dbp, left, right, rbkey);
		if (rc < 0) { return rc; }
	}
	// Otherwie we expand the current node.
	else {
		size_t pos = BRANCH_PTR_SIZE + eidx*BRANCH_ENTRY_SIZE;
		EdNode *src = branch;
		// The source node must be cloned if was from a previous transaction. This
		// node could have already been modified from a prior operation within the
		// current transaction.
		if (src->tree->xid < txn->xid) {
			int rc = ed_txn_clone(txn, src, &branch);
			if (rc < 0) { return rc; }
			memcpy(branch->tree->data, src->tree->data, pos);
		}
		// Shift all entries after the new index over. The src node must be part of
		// the current transaction at this point. If the node was cloned, this will
		// copy the remaining data from the previous version. Otherwise, this will
		// shift the entries in place.
		memmove(branch->tree->data + pos + BRANCH_ENTRY_SIZE,
				src->tree->data + pos,
				(src->tree->nkeys - eidx) * BRANCH_ENTRY_SIZE);
		branch->tree->nkeys++;
	}

	l->parent = branch;
	r->parent = branch;

	set_node(txn, dbp, l);
	branch_set_key(r->parent->tree, r->pindex, rkey);
	set_node(txn, dbp, r);
	return 0;
}

static int
split_point(EdTxnDb *dbp, EdBpt *l)
{
	uint16_t n = l->nkeys, mid = n/2, min, max;
	uint64_t key;
	size_t esize = dbp->entry_size;

	// The split cannot be between repeated keys.
	// If the searched index is around the mid point, use the key and search position.
	if (dbp->nmatches > 0 &&
			mid <= dbp->entry_index &&
			mid >= dbp->entry_index - dbp->nmatches + 1) {
		key = dbp->key;
		min = dbp->entry_index - dbp->nmatches + 1;
		max = dbp->entry_index + 1;
	}
	// Otherwise search back for the start of any repeat sequence.
	else {
		key = leaf_key(l, mid, esize);
		if (key == dbp->key) {
			min = dbp->entry_index - dbp->nmatches + 1;
		}
		else {
			for (min = mid; min > 0 && leaf_key(l, min-1, esize) == key; min--) {}
		}
		max = mid + 1;
	}

	// If repeat keys span the mid point, pick the larger side to split on.
	if (min != mid) {
		for (; max < n && leaf_key(l, max, esize) == key; max++) {}
		if (min == 0 && max == n) { return ED_EINDEX_DUPKEY; }
		mid = min >= n - max ? min : max;
	}
	return mid;
}

static int
split_leaf(EdTxn *txn, EdTxnDb *dbp, EdNode *leaf, int mid)
{
	size_t esize = dbp->entry_size;
	uint32_t eidx = dbp->entry_index;
	size_t off = mid * esize;

	// If the new key will be the first entry on right, use the search key.
	uint64_t rkey = (uint16_t)mid == eidx ?
		dbp->key : ed_fetch64(leaf->tree->data+off);

	assert(ed_fetch64(leaf->tree->data + off - esize) < rkey);
	assert(rkey <= ed_fetch64(leaf->tree->data + off));

	EdNode *left = leaf, *right;
	int rc = ed_txn_alloc(txn, leaf->parent, leaf->pindex + 1, &right);
	if (rc < 0) { return rc; }
	if (leaf->tree->xid < txn->xid) {
		rc = ed_txn_clone(txn, leaf, &left);
		if (rc < 0) { return rc; }
	}

	right->page->type = ED_PG_LEAF;
	right->tree->next = ED_PG_NONE;
	right->tree->nkeys = leaf->tree->nkeys - mid;
	left->tree->nkeys = mid;

	// The new entry goes on the left-side leaf.
	if (eidx < (uint16_t)mid) {
		dbp->find = left;
		// Copy entries after the mid point to new right leaf.
		memcpy(right->tree->data, leaf->tree->data+off, sizeof(leaf->tree->data) - off);
		// If left is a newly cloned node, copy the entries left of the new index.
		if (left != leaf) {
			memcpy(left->tree->data, leaf->tree->data, eidx*esize);
		}
		// Shift entries before the mid point over in left leaf.
		memmove(left->tree->data + (eidx+1)*esize,
				leaf->tree->data + eidx*esize,
				(left->tree->nkeys - eidx) * esize);
	}
	// The new entry goes on the right-side leaf.
	else {
		eidx -= mid;
		if (left != leaf) {
			memcpy(left->tree->data, leaf->tree->data, off);
		}
		// Copy entries after the mid point but before the new index to new right leaf.
		memcpy(right->tree->data, leaf->tree->data+off, eidx*esize);
		// Copy entries after the mid point and after the new index to new right leaf.
		memcpy(right->tree->data + (eidx+1)*esize,
				leaf->tree->data + off + eidx*esize,
				(right->tree->nkeys - eidx) * esize);
		dbp->entry_index = eidx;
		dbp->find = right;
	}
	dbp->entry = dbp->find->tree->data + eidx*esize;

	return insert_into_parent(txn, dbp, left, right, rkey);
}

static int
insert_into_leaf(EdTxn *txn, EdTxnDb *dbp, const void *ent, bool replace)
{
	EdNode *leaf = dbp->find;
	size_t esize = dbp->entry_size;
	uint32_t eidx = dbp->entry_index;

	// When the leaf is NULL, we have a brand new tree.
	if (leaf == NULL) {
		int rc = ed_txn_alloc(txn, NULL, 0, &leaf);
		if (rc < 0) { return rc; }
		leaf->page->type = ED_PG_LEAF;
		leaf->tree->next = ED_PG_NONE;
		leaf->tree->nkeys = 1;
		dbp->entry = leaf->tree->data;
		dbp->entry_index = 0;
		dbp->find = leaf;
	}
	// If the leaf is full, it needs to be split.
	else if (!replace && IS_LEAF_FULL(leaf->tree, esize)) {
		int mid = split_point(dbp, leaf->tree);
		if (mid < 0) { return mid; }
		int rc = split_leaf(txn, dbp, leaf, mid);
		if (rc < 0) { return rc; }
		leaf = dbp->find;
		leaf->tree->nkeys++;
	}
	// Otherwie we expand the current node.
	else {
		EdNode *src = leaf;
		// The source node must be cloned if was from a previous transaction. This
		// node could have already been modified from a prior operation within the
		// current transaction.
		if (src->tree->xid < txn->xid) {
			int rc = ed_txn_clone(txn, src, &leaf);
			if (rc < 0) { return rc; }
			dbp->find = leaf;
			dbp->entry = leaf->tree->data + eidx*esize;
			// When replacing, copy the full data. Otherwise only copy left of then new
			// insertion index. The following memmove will copy the right side.
			memcpy(leaf->tree->data, src->tree->data,
					replace ? sizeof(src->tree->data) : eidx*esize);
		}
		if (!replace) {
			// Shift all entries after the new index over. The src node must be part of
			// the current transaction at this point. If the node was cloned, this will
			// copy the remaining data from the previous version. Otherwise, this will
			// shift the entries in place.
			memmove(leaf->tree->data + eidx*esize + esize,
					src->tree->data + eidx*esize,
					(src->tree->nkeys - eidx)*esize);
			leaf->tree->nkeys++;
		}
	}

	// The insert location is now available for assignment and it is part of the
	// current transaction.
	memcpy(dbp->entry, ent, esize);
	return set_leaf(txn, dbp, leaf, eidx);
}

int
ed_bpt_set(EdTxn *txn, unsigned db, const void *ent, bool replace)
{
	if (ed_txn_isrdonly(txn)) { return ED_EINDEX_RDONLY; }

	EdTxnDb *dbp = ed_txn_db(txn, db, false);
	uint64_t key = ed_fetch64(ent);
	if (!dbp->hasfind || key < dbp->kmin || key > dbp->kmax) {
		return ED_EINDEX_KEY_MATCH;
	}

	int rc = insert_into_leaf(txn, dbp, ent, replace && dbp->match == 1);
	if (rc < 0) {
		txn->error = rc;
		return rc;
	}

	dbp->key = key;
	dbp->kmax = key;
	dbp->nsplits = 0;
	dbp->match = 1;
	dbp->nmatches = 1;
	dbp->nloops = 0;
	dbp->haskey = true;
	dbp->hasentry = true;
	return 0;
}

int
ed_bpt_del(EdTxn *txn, unsigned db)
{
	// FIXME: this function is terrible
	if (ed_txn_isrdonly(txn)) { return ED_EINDEX_RDONLY; }

	EdTxnDb *dbp = ed_txn_db(txn, db, false);
	if (!dbp->hasfind) { return ED_EINDEX_RDONLY; }
	if (!dbp->hasentry) { return 0; }

	EdNode *leaf = dbp->find;
	size_t esize = dbp->entry_size;
	uint32_t eidx = dbp->entry_index;

	if (leaf->tree->xid < txn->xid) {
		EdNode *src = leaf;
		int rc = ed_txn_clone(txn, src, &leaf);
		if (rc < 0) { return rc; }
		memcpy(leaf->tree->data, src->tree->data, sizeof(leaf->tree->data));
		rc = set_node(txn, dbp, leaf);
		if (rc < 0) { return rc; }
		dbp->entry = leaf->tree->data + esize*eidx;
		dbp->find = leaf;
	}

	memmove(dbp->entry, (uint8_t *)dbp->entry + esize,
			(leaf->tree->nkeys - eidx - 1) * esize);
	if (leaf->tree->nkeys == 1) {
		leaf->tree->nkeys = 0;
	}
	else {
		leaf->tree->nkeys--;
#if 0
		if (leaf->parent && eidx == 0) {
			branch_set_key(leaf->parent->tree, leaf->pindex, ed_fetch64(dbp->entry));
		}
#endif
	}
	if (dbp->entry_index == leaf->tree->nkeys) {
		dbp->kmax = find_kmax(leaf);
	}
	else {
		dbp->kmax = ed_fetch64(dbp->entry);
	}
	dbp->nmatches = 0;
	dbp->hasentry = false;
	return 1;
}

static int
bpt_mark_children(EdIdx *idx, EdStat *stat, EdBpt *brch, int depth, int *max)
{
	EdPgno *ptr = (EdPgno *)brch->data;
	for (uint32_t i = 0; i <= brch->nkeys; i++, ptr = BRANCH_NEXT(ptr)) {
		EdPgno no = ed_fetch32(ptr);
		int rc = ed_stat_mark(stat, no);
		if (rc < 0) { return rc; }
		if (depth < *max) {
			EdBpt *chld = ed_pg_map(idx->fd, no, 1, true);
			if (chld == MAP_FAILED) { return ED_ERRNO; }
			if (chld->base.type == ED_PG_LEAF) {
				*max = depth;
			}
			else {
				rc = bpt_mark_children(idx, stat, chld, depth+1, max);
			}
			ed_pg_unmap(chld, 1);
			if (rc < 0) { return rc; }
		}
	}
	return 0;
}

int
ed_bpt_mark(EdIdx *idx, EdStat *stat, EdBpt *bpt)
{
	int rc = ed_stat_mark(stat, bpt->base.no);
	if (rc < 0 || bpt->base.type == ED_PG_LEAF) {
		return rc;
	}
	int max = 8;
	return bpt_mark_children(idx, stat, bpt, 1, &max);
}



#define HBAR "╌"
#define VBAR "┆"

static const char
	tl[] = "╭", tc[] = "┬", tr[] = "╮",
	ml[] = "├", mc[] = "┼", mr[] = "┤",
	bl[] = "╰", bc[] = "┴", br[] = "╯",
	hbar[] =
		HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR
		HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR HBAR;

#define COLS 6
#define COLW ((sizeof(hbar)-1)/(sizeof(HBAR)-1))

static const char
	space[COLW] = "                        ";

static void
print_page(int fd, size_t esize, uint8_t *p, FILE *out, EdBptPrint print, bool *stack, int top);

static int
print_value(const void *value, char *buf, size_t len)
{
	return snprintf(buf, len, "%" PRIu64, ed_fetch64(value));
}

static void
print_tree_branches(FILE *out, bool *stack, int top)
{
	for (int i = 0; i < top; i++) {
		if (stack[i]) {
			static const char s[] = "   ";
			fwrite(s, 1, sizeof(s)-1, out);
		}
		else {
			static const char s[] = "│  ";
			fwrite(s, 1, sizeof(s)-1, out);
		}
	}
}

static void
print_tree(FILE *out, bool *stack, int top)
{
	print_tree_branches(out, stack, top);
	if (stack[top]) {
		static const char s[] = "└─ ";
		fwrite(s, 1, sizeof(s)-1, out);
	}
	else {
		static const char s[] = "├─ ";
		fwrite(s, 1, sizeof(s)-1, out);
	}
}

static void
print_box(FILE *out, uint32_t i, uint32_t n, bool *stack, int top)
{
	if (n == 0 || i > n) { return; }

	bool last = false;
	uint32_t end = 0;

	if (i == n) {
		last = true;
		uint32_t c = n % COLS;
		if (c == 0) { c = COLS; }
		end = i + c;
	}
	else if (i % COLS == 0) {
		end = n < COLS ? n : i + COLS;
	}

	if (i < end) {
		if (i) { fprintf(out, VBAR); }
		fputc('\n', out);
		print_tree_branches(out, stack, top);
		fwrite(i == 0 ? tl : (i < n ? ml : bl), 1, sizeof(tl)-1, out);
		fwrite(hbar, 1, sizeof(hbar)-1, out);
		for (i++; i < end; i++) {
			fwrite(!last && i < COLS ? tc : (i <= n ? mc : bc), 1, sizeof(tc)-1, out);
			fwrite(hbar, 1, sizeof(hbar)-1, out);
		}
		fwrite(!last && i <= COLS ? tr : (i <= n ? mr : br), 1, sizeof(tr)-1, out);
		fputc('\n', out);
		if (last) { return; }
		print_tree_branches(out, stack, top);
	}

	fprintf(out, VBAR);
}

static void
print_leaf(int fd, size_t esize, EdBpt *leaf, FILE *out, EdBptPrint print, bool *stack, int top)
{
	fprintf(out, "leaf p%u, xid=%" PRIu64 ", nkeys=%u/%zu",
			leaf->base.no, leaf->xid, leaf->nkeys, LEAF_ORDER(esize));

	uint32_t n = leaf->nkeys;
	if (n == 0) {
		fputc('\n', out);
		return;
	}

	uint8_t *p = leaf->data;
	for (uint32_t i = 0; i < n; i++, p += esize) {
		char buf[COLW+1];
		int len = print(p, buf, sizeof(buf));
		if (len < 0 || len > (int)COLW) {
			len = 0;
		}
		print_box(out, i, n, stack, top);
		fwrite(buf, 1, len, out);
		fwrite(space, 1, COLW-len, out);
	}
	print_box(out, n, n, stack, top);

	if (leaf->next != ED_PG_NONE) {
		EdBpt *next = ed_pg_map(fd, leaf->next, 1, true);
		print_tree(out, stack, top-1);
		fprintf(out, "= %" PRIu64 ", ", ed_fetch64(next->data));
		print_leaf(fd, esize, next, out, print, stack, top);
		ed_pg_unmap(next, 1);
	}
}

static void
print_branch(int fd, size_t esize, EdBpt *branch, FILE *out, EdBptPrint print, bool *stack, int top)
{
	fprintf(out, "branch p%u, xid=%" PRIu64 ", nkeys=%u/%zu\n",
			branch->base.no, branch->xid, branch->nkeys, BRANCH_ORDER-1);

	uint32_t end = branch->nkeys;
	uint8_t *p = branch->data + BRANCH_PTR_SIZE;

	stack[top] = end == 0;
	print_tree(out, stack, top);
	fprintf(out, "< %" PRIu64 ", ", ed_fetch64(p));
	print_page(fd, esize, p-BRANCH_PTR_SIZE, out, print, stack, top+1);

	for (uint32_t i = 1; i <= end; i++, p += BRANCH_ENTRY_SIZE) {
		stack[top] = i == end;
		print_tree(out, stack, top);
		fprintf(out, "≥ %" PRIu64 ", ", ed_fetch64(p));
		print_page(fd, esize, p+BRANCH_KEY_SIZE, out, print, stack, top+1);
	}
}

static void
print_node(int fd, size_t esize, EdBpt *t, FILE *out, EdBptPrint print, bool *stack, int top)
{
	switch (t->base.type) {
	case ED_PG_LEAF:
		print_leaf(fd, esize, t, out, print, stack, top);
		break;
	case ED_PG_BRANCH:
		print_branch(fd, esize, t, out, print, stack, top);
		break;
	}
}

static void
print_page(int fd, size_t esize, uint8_t *p, FILE *out, EdBptPrint print, bool *stack, int top)
{
	EdBpt *t = ed_pg_map(fd, ed_fetch32(p), 1, true);
	if (t == MAP_FAILED) {
		fprintf(out, "MAP FAILED (%s)\n", strerror(errno));
		return;
	}
	print_node(fd, esize, t, out, print, stack, top);
	ed_pg_unmap(t, 1);
}

static int
verify_leaf(int fd, size_t esize, EdBpt *l, FILE *out, uint64_t min, uint64_t max)
{
	if (l->nkeys == 0) { return 0; }

	uint8_t *p = l->data;
	uint64_t last;
	for (uint32_t i = 0; i < l->nkeys; i++, p += esize) {
		uint64_t key = ed_fetch64(p);
		if (key < min || key > max) {
			if (out != NULL) {
				fprintf(out,
						"leaf key out of range: %" PRIu64 ", %" PRIu64 "...%" PRIu64 "\n",
						key, min, max);
				bool stack[16] = {0};
				print_leaf(fd, esize, l, out, print_value, stack, 0);
			}
			return -1;
		}
		if (i > 0 && key < last) {
			if (out != NULL) {
				fprintf(out, "leaf key out of order: %" PRIu64 "\n", key);
				bool stack[16] = {0};
				print_leaf(fd, esize, l, out, print_value, stack, 0);
			}
			return -1;
		}
		last = key;
	}
	return 0;
}

int
verify_node(int fd, size_t esize, EdBpt *t, FILE *out, uint64_t min, uint64_t max)
{
	if (t->base.type == ED_PG_LEAF) {
		return verify_leaf(fd, esize, t, out, min, max);
	}

	uint8_t *p = t->data;
	uint64_t nmin = min;
	EdBpt *chld;
	int rc;

	for (uint16_t i = 0; i < t->nkeys; i++, p += BRANCH_ENTRY_SIZE) {
		uint64_t nmax = ed_fetch64(p + BRANCH_PTR_SIZE);
		if (nmax < min || nmax > max) {
			if (out != NULL) {
				fprintf(out,
						"branch key out of range: %" PRIu64 ", %" PRIu64 "...%" PRIu64 "\n",
						nmax, min, max);
				bool stack[16] = {0};
				print_branch(fd, esize, t, out, print_value, stack, 0);
			}
			return -1;
		}

		chld = ed_pg_map(fd, ed_fetch32(p), 1, true);
		if (chld == MAP_FAILED) { return ED_ERRNO; }
		rc = verify_node(fd, esize, chld, out, nmin, nmax - 1);
		ed_pg_unmap(chld, 1);
		if (rc < 0) { return rc; }
		nmin = nmax;
	}

	chld = ed_pg_map(fd, ed_fetch32(p), 1, true);
	if (chld == MAP_FAILED) { return ED_ERRNO; }
	rc = verify_node(fd, esize, chld, out, nmin, max);
	ed_pg_unmap(chld, 1);
	if (rc < 0) { return rc; }

	return 0;
}

void
ed_bpt_print(EdBpt *t, int fd, size_t esize, FILE *out, EdBptPrint print)
{
	if (t == NULL) { return; }
	if (out == NULL) { out = stdout; }
	if (print == NULL) { print = print_value; }

	bool stack[16] = {1};
	fwrite(space, 1, 3, out);
	print_node(fd, esize, t, out, print, stack, 1);
}

int
ed_bpt_verify(EdBpt *t, int fd, size_t esize, FILE *out)
{
	if (t == NULL) { return 0; }
	return verify_node(fd, esize, t, out, 0, UINT64_MAX);
}

