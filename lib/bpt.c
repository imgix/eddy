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

static inline void
branch_set_key(EdNode *b, uint16_t idx, uint64_t val)
{
	assert(idx <= b->tree->nkeys);
	if (idx == 0) { return; }
	memcpy(b->tree->data + idx*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE, &val, sizeof(val));
}

static inline EdPgno
branch_ptr(EdNode *b, uint16_t idx)
{
	assert(idx <= b->tree->nkeys);
	return ed_fetch32(b->tree->data + idx*BRANCH_ENTRY_SIZE);
}

static inline uint64_t
leaf_key(EdBpt *l, uint16_t idx, size_t esize)
{
	assert(idx < l->nkeys);
	return ed_fetch64(l->data + idx*esize);
}

static inline uint16_t
branch_index(EdBpt *node, EdPgno *ptr)
{
	assert(node->data <= (uint8_t *)ptr);
	assert((uint8_t *)ptr < node->data + (BRANCH_ORDER*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE));
	return ((uint8_t *)ptr - node->data) / BRANCH_ENTRY_SIZE;
}



size_t
ed_bpt_capacity(size_t esize, size_t depth)
{
	return llround(pow(BRANCH_ORDER, depth-1) * LEAF_ORDER(esize));
}

static EdPgno *
search_branch(EdBpt *node, uint64_t key)
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

int
ed_bpt_find(EdTxn *txn, unsigned db, uint64_t key, void **ent)
{
	if (!txn->isopen) {
		// FIXME: return proper error code
		return ed_esys(EINVAL);
	}

	EdTxnDb *dbp = ed_txn_db(txn, db, true);
	dbp->key = key;
	dbp->haskey = true;

	int rc = 0;
	uint32_t i = 0, n = 0;
	uint8_t *data = NULL;
	size_t esize = dbp->entry_size;
	EdNode *node = dbp->head;
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
		EdPgno *ptr = search_branch(node->tree, key);
		uint16_t bidx = branch_index(node->tree, ptr);
		EdPgno no = ed_fetch32(node->tree->data + bidx*BRANCH_ENTRY_SIZE);
		EdNode *next;
		rc = ed_txn_map(txn, no, node, bidx, &next);
		if (rc < 0) { goto done; }
		node = next;
		dbp->tail = node;
	}
	if (IS_LEAF_FULL(node->tree, esize)) { dbp->nsplits++; }
	else { dbp->nsplits = 0; }

	// Search the leaf node.
	data = node->tree->data;
	for (i = 0, n = node->tree->nkeys; i < n; i++, data += esize) {
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
	if (rc >= 0) {
		dbp->entry = dbp->start = data;
		dbp->entry_index = i;
		dbp->nmatches = rc;
		dbp->nloops = 0;
		dbp->caninsert = !txn->isrdonly;
		if (ent) { *ent = data; }
	}
	dbp->match = rc;
	return rc;
}

static int
move_first(EdTxn *txn, EdTxnDb *dbp, EdNode *from)
{
	dbp->haskey = false;
	dbp->caninsert = false;

	int rc = 0;
	if (from == NULL) { goto done; }

	while (IS_BRANCH(from->tree)) {
		EdPgno no = ed_fetch32(from->tree->data);
		EdNode *next;
		rc = ed_txn_map(txn, no, from, 0, &next);
		if (rc < 0) { goto done; }
		from = next;
	}
	dbp->tail = from;

done:
	dbp->match = rc;
	dbp->nmatches = 0;
	return rc;
}

/**
 * @brief  Moves the db tail to a right sibling node
 * @param  txn  Transaction object
 * @param  dbp  Transaction database object
 * @param  from  Node to move from
 * @return 0 on succces, <0 on error
 */
static int
move_right(EdTxn *txn, EdTxnDb *dbp, EdNode *from)
{
	// Traverse up to leaf node from any overflow nodes.
	for (; from->page->type == ED_PG_OVERFLOW; from = from->parent) {}
	assert(from->page->type == ED_PG_LEAF);

	// Traverse up the nearest node that isn't the last key of its parent.
	do {
		// When at the root, wrapping is the only option.
		if (from->parent == NULL) {
			break;
		}
		// If a child node is not the last key, load the sibling.
		if (from->pindex < from->parent->tree->nkeys) {
			EdPgno no = branch_ptr(from->parent, from->pindex+1);
			int rc = ed_txn_map(txn, no, from->parent, from->pindex+1, &from);
			if (rc < 0) { return rc; }
			break;
		}
		from = from->parent;
	} while (1);

	// Traverse down to the left-most leaf.
	return move_first(txn, dbp, from);
}

int
ed_bpt_first(EdTxn *txn, unsigned db, void **ent)
{
	EdTxnDb *dbp = &txn->db[db];

	int rc = move_first(txn, dbp, dbp->head);
	if (rc == 0) {
		void *data = dbp->tail->tree->data;
		dbp->entry = dbp->start = data;
		dbp->entry_index = 0;
		dbp->nloops = 0;
		if (ent) { *ent = data; }
	}
	return rc;
}

int
ed_bpt_next(EdTxn *txn, unsigned db, void **ent)
{
	EdTxnDb *dbp = &txn->db[db];
	int rc = 0;
	EdNode *node = dbp->tail;
	EdBpt *leaf = node->tree;
	uint8_t *p = dbp->entry;
	uint32_t i = dbp->entry_index;

	if (i == leaf->nkeys-1) {
		EdPgno next = leaf->next;
		if (next == ED_PG_NONE) {
			rc = move_right(txn, dbp, node);
			if (rc < 0) { goto done; }
		}
		else {
			rc = ed_txn_map(txn, next, node, 0, &node);
			if (rc < 0) { goto done; }
			dbp->tail = node;
		}
		p = dbp->tail->tree->data;
		i = 0;
	}
	else {
		p += dbp->entry_size;
		i++;
	}

	dbp->entry = p;
	dbp->entry_index = i;
	if (ent) { *ent = p; }

	if (dbp->haskey) {
		if (dbp->key == ed_fetch64(p)) {
			dbp->nmatches++;
			rc = 1;
		}
		else {
			dbp->haskey = false;
			dbp->caninsert = false;
		}
	}

done:
	if (dbp->entry == dbp->start) { dbp->nloops++; }
	dbp->match = rc;
	return rc;
}

int
ed_bpt_loop(const EdTxn *txn, unsigned db)
{
	return txn->db[db].nloops;
}

static void
insert_into_parent(EdTxn *txn, EdTxnDb *dbp, EdNode *left, EdNode *right, uint64_t rkey)
{
	EdNode *parent = left->parent;
	EdBpt *p;
	EdPgno r = right->tree->base.no;
	size_t pos;
	int rc;

	// No parent, so create new root node.
	if (parent == NULL) {
		// Initialize new parent node from the allocation array.
		rc = ed_txn_alloc(txn, NULL, 0, &parent);
		assert(rc >= 0); // FIXME
		dbp->head = parent;
		p = parent->tree;
		p->base.type = ED_PG_BRANCH;
		p->next = ED_PG_NONE;
		p->nkeys = 0;

		// Assign the left pointer. (right is assigned below)
		pos = BRANCH_PTR_SIZE;
		memcpy(p->data, &left->tree->base.no, BRANCH_PTR_SIZE);

		// Insert the new parent into the list.
		left->pindex = 0;
		right->pindex = 1;
	}
	else {
		uint32_t index = left->pindex;
		p = parent->tree;

		// The parent branch is full, so it gets split.
		if (IS_BRANCH_FULL(p)) {
			uint32_t n = p->nkeys;
			uint32_t mid = (n+1) / 2;
			size_t off = mid * BRANCH_ENTRY_SIZE;
			uint64_t rbkey = branch_key(p, mid);

			EdNode *leftb = parent, *rightb;
			rc = ed_txn_alloc(txn, leftb->parent, leftb->pindex + 1, &rightb);
			assert(rc >= 0); // FIXME

			rightb->tree->base.type = ED_PG_BRANCH;
			rightb->tree->next = ED_PG_NONE;
			rightb->tree->nkeys = n - mid;
			leftb->tree->nkeys = mid - 1;

			memcpy(rightb->tree->data, leftb->tree->data+off, sizeof(leftb->tree->data) - off);

			if (rkey >= rbkey) {
				index -= mid;
				p = rightb->tree;
			}

			insert_into_parent(txn, dbp, leftb, rightb, rbkey);
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
		if (min == 0 && max == n) { return -1; }
		mid = min >= n - max ? min : max;
	}
	return mid;
}

static int
overflow_leaf(EdTxn *txn, EdTxnDb *dbp, EdNode *leaf)
{
	assert(leaf->tree->nkeys == LEAF_ORDER(dbp->entry_size));

	EdNode *node;
	size_t esize = dbp->entry_size;

	if (leaf->tree->next != ED_PG_NONE &&
			ed_txn_map(txn, leaf->tree->next, leaf, 0, &node) == 0 &&
			node->tree->nkeys < LEAF_ORDER(esize)) {
		goto done;
	}

	int rc = ed_txn_alloc(txn, leaf, 0, &node);
	assert(rc >= 0); // FIXME
	node->tree->base.type = ED_PG_OVERFLOW;
	node->tree->next = leaf->tree->next;
	node->tree->nkeys = 0;
	leaf->tree->next = node->tree->base.no;

done:
	dbp->tail = node;
	dbp->entry = node->tree->data + esize*node->tree->nkeys;
	dbp->entry_index = node->tree->nkeys;
	return 0;
}

static int
split_leaf(EdTxn *txn, EdTxnDb *dbp, EdNode *leaf, int mid)
{
	size_t esize = dbp->entry_size;
	uint32_t n = leaf->tree->nkeys;
	size_t off = mid * esize;

	// If the new key will be the first entry on right, use the search key.
	uint64_t rkey = (uint16_t)mid == dbp->entry_index ?
		dbp->key : ed_fetch64(leaf->tree->data+off);

	assert(ed_fetch64(leaf->tree->data + off - esize) < rkey);
	assert(rkey <= ed_fetch64(leaf->tree->data + off));

	EdNode *left = leaf, *right;
	int rc = ed_txn_alloc(txn, leaf->parent, leaf->pindex + 1, &right);
	if (rc < 0) { return rc; }

	/*
	if (leaf->tree->xid < txn->xid) {
		rc = ed_txn_alloc(txn, leaf->parent, leaf->pindex, &left);
		if (rc < 0) { return rc; }
		left->tree->next = leaf->tree->next;
		memcpy(left->tree->data, left->tree->data, off);
	}
	*/

	right->tree->base.type = ED_PG_LEAF;
	right->tree->next = ED_PG_NONE;
	right->tree->nkeys = n - mid;
	left->tree->nkeys = mid;

	if (dbp->entry_index < (uint16_t)mid) {
		dbp->tail = left;
		// Copy entries after the mid point to new right leaf.
		memcpy(right->tree->data, leaf->tree->data+off, sizeof(leaf->tree->data) - off);
		// Shift entries before the mid point over in left leaf.
		memmove(left->tree->data + (dbp->entry_index+1)*esize,
				left->tree->data + dbp->entry_index*esize,
				(left->tree->nkeys - dbp->entry_index) * esize);
	}
	else {
		dbp->entry_index -= mid;
		dbp->tail = right;
		// Copy entries after the mid point but before the new index to new right leaf.
		memcpy(right->tree->data, leaf->tree->data+off, dbp->entry_index*esize);
		// Copy entries after the mid point and after the new index to new right leaf.
		memcpy(right->tree->data + (dbp->entry_index+1)*esize,
				leaf->tree->data + off + dbp->entry_index*esize,
				(right->tree->nkeys - dbp->entry_index) * esize);
	}
	dbp->entry = dbp->tail->tree->data + dbp->entry_index*esize;

	insert_into_parent(txn, dbp, left, right, rkey);
	/* TODO
	rc = insert_into_parent(txn, dbp, left, right, rkey);
	if (rc < 0) { return rc; }
	*/

	return 0;
}

int
ed_bpt_set(EdTxn *txn, unsigned db, const void *ent, bool replace)
{
	if (txn->isrdonly) { return ED_EINDEX_RDONLY; }

	EdTxnDb *dbp = ed_txn_db(txn, db, false);
	if (!dbp->haskey || ed_fetch64(ent) != dbp->key) { return ED_EINDEX_KEY_MATCH; }

	if (replace && dbp->match == 1) {
		memcpy(dbp->entry, ent, dbp->entry_size);
		return 0;
	}

	EdNode *leaf = dbp->tail;
	size_t esize = dbp->entry_size;
	int rc = 0;

	// If the root was NULL, create a new root node.
	if (leaf == NULL) {
		rc = ed_txn_alloc(txn, NULL, 0, &leaf);
		if (rc < 0) { return rc; }
		leaf->tree->base.type = ED_PG_LEAF;
		leaf->tree->next = ED_PG_NONE;
		leaf->tree->nkeys = 0;
		dbp->entry = leaf->tree->data;
		dbp->entry_index = 0;
		dbp->head = dbp->tail = leaf;
	}

	// Otherwise use the leaf from the search. If full, it needs to be split.
	else if (IS_LEAF_FULL(leaf->tree, esize)) {
		int mid = split_point(dbp, leaf->tree);
		rc = mid < 0 ?
			overflow_leaf(txn, dbp, leaf) :
			split_leaf(txn, dbp, leaf, mid);
		if (rc < 0) { return rc; }
		leaf = dbp->tail;
	}

	else {
		uint8_t *p = dbp->entry;
		memmove(p+esize, dbp->entry, (leaf->tree->nkeys - dbp->entry_index) * esize);
	}

	memcpy(dbp->entry, ent, esize);
	if (dbp->entry_index == 0 && leaf->parent != NULL) {
		branch_set_key(leaf->parent, leaf->pindex, ed_fetch64(ent));
	}
	if (*dbp->no != dbp->head->page->no) {
		*dbp->no = dbp->head->page->no;
	}

	leaf->tree->nkeys++;
	dbp->nsplits = 0;
	dbp->match = 1;
	return 1;
}

int
ed_bpt_del(EdTxn *txn, unsigned db)
{
	if (txn->isrdonly) { return ED_EINDEX_RDONLY; }

	EdTxnDb *dbp = ed_txn_db(txn, db, false);
	if (dbp->match < 1) { return 0; }

	EdNode *leaf = dbp->tail;
	size_t esize = dbp->entry_size;

	memmove(dbp->entry, (uint8_t *)dbp->entry + esize,
			(leaf->tree->nkeys - dbp->entry_index - 1) * esize);
	if (leaf->tree->nkeys == 1) {
		// FIXME: this is terrible
		leaf->tree->nkeys = 0;
	}
	else {
		leaf->tree->nkeys--;
		if (leaf->parent && dbp->entry_index == 0) {
			branch_set_key(leaf->parent, leaf->pindex, ed_fetch64(dbp->entry));
		}
	}
	return 1;
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
	return snprintf(buf, len, "%llu", ed_fetch64(value));
}

static void
print_tree_branches(FILE *out, bool *stack, int top)
{
	for (int i = 0; i < top; i++) {
		if (stack[i]) {
			static const char s[] = "    ";
			fwrite(s, 1, sizeof(s)-1, out);
		}
		else {
			static const char s[] = "│   ";
			fwrite(s, 1, sizeof(s)-1, out);
		}
	}
}

static void
print_tree(FILE *out, bool *stack, int top)
{
	print_tree_branches(out, stack, top);
	if (stack[top]) {
		static const char s[] = "└── ";
		fwrite(s, 1, sizeof(s)-1, out);
	}
	else {
		static const char s[] = "├── ";
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
	fprintf(out, "%s p%u, nkeys=%u/%zu",
			leaf->base.type == ED_PG_LEAF ? "leaf" : "overflow",
			leaf->base.no, leaf->nkeys, LEAF_ORDER(esize));

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
		printf("next: %u\n", leaf->next);
		EdBpt *next = ed_pg_map(fd, leaf->next, 1);
		print_tree(out, stack, top-1);
		fprintf(out, "= %llu, ", ed_fetch64(next->data));
		print_leaf(fd, esize, next, out, print, stack, top);
		ed_pg_unmap(next, 1);
	}
}

static void
print_branch(int fd, size_t esize, EdBpt *branch, FILE *out, EdBptPrint print, bool *stack, int top)
{
	fprintf(out, "branch p%u, nkeys=%u/%zu\n",
			branch->base.no, branch->nkeys, BRANCH_ORDER);

	uint32_t end = branch->nkeys;
	uint8_t *p = branch->data + BRANCH_PTR_SIZE;

	stack[top] = end == 0;
	print_tree(out, stack, top);
	fprintf(out, "< %llu, ", ed_fetch64(p));
	print_page(fd, esize, p-BRANCH_PTR_SIZE, out, print, stack, top+1);

	for (uint32_t i = 1; i <= end; i++, p += BRANCH_ENTRY_SIZE) {
		stack[top] = i == end;
		print_tree(out, stack, top);
		fprintf(out, "≥ %llu, ", ed_fetch64(p));
		print_page(fd, esize, p+BRANCH_KEY_SIZE, out, print, stack, top+1);
	}
}

static void
print_node(int fd, size_t esize, EdBpt *t, FILE *out, EdBptPrint print, bool *stack, int top)
{
	switch (t->base.type) {
	case ED_PG_OVERFLOW:
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
	EdBpt *t = ed_pg_map(fd, ed_fetch32(p), 1);
	if (t == MAP_FAILED) {
		fprintf(out, "MAP FAILED (%s)\n", strerror(errno));
		return;
	}
	print_node(fd, esize, t, out, print, stack, top);
	ed_pg_unmap(t, 1);
}

static int
verify_overflow(int fd, size_t esize, EdBpt *o, FILE *out, uint64_t expect)
{
	uint8_t *p = o->data;
	for (uint32_t i = 0; i < o->nkeys; i++, p += esize) {
		uint64_t key = ed_fetch64(p);
		if (key != expect) {
			if (out != NULL) {
				fprintf(out, "overflow key incorrect: %llu, %llu\n", key, expect);
				bool stack[16] = {0};
				print_leaf(fd, esize, o, out, print_value, stack, 0);
			}
			return -1;
		}
	}
	return 0;
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
				fprintf(out, "leaf key out of range: %llu, %llu...%llu\n", key, min, max);
				bool stack[16] = {0};
				print_leaf(fd, esize, l, out, print_value, stack, 0);
			}
			return -1;
		}
		if (i > 0 && key < last) {
			if (out != NULL) {
				fprintf(out, "leaf key out of order: %llu\n", key);
				bool stack[16] = {0};
				print_leaf(fd, esize, l, out, print_value, stack, 0);
			}
			return -1;
		}
		last = key;
	}

	EdPgno ptr = l->next;
	while (ptr != ED_PG_NONE) {
		EdBpt *next = ed_pg_map(fd, ptr, 1);
		int rc = verify_overflow(fd, esize, next, out, last);
		ptr = next->next;
		ed_pg_unmap(next, 1);
		if (rc < 0) { return -1; }
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
				fprintf(out, "branch key out of range: %llu, %llu...%llu\n", nmax, min, max);
				bool stack[16] = {0};
				print_branch(fd, esize, t, out, print_value, stack, 0);
			}
			return -1;
		}

		chld = ed_pg_map(fd, ed_fetch32(p), 1);
		if (chld == MAP_FAILED) { return ED_ERRNO; }
		rc = verify_node(fd, esize, chld, out, nmin, nmax - 1);
		ed_pg_unmap(chld, 1);
		if (rc < 0) { return rc; }
		nmin = nmax;
	}

	chld = ed_pg_map(fd, ed_fetch32(p), 1);
	if (chld == MAP_FAILED) { return ED_ERRNO; }
	rc = verify_node(fd, esize, chld, out, nmin, max);
	ed_pg_unmap(chld, 1);
	if (rc < 0) { return rc; }

	return 0;
}

void
ed_bpt_print(EdBpt *t, int fd, size_t esize, FILE *out, EdBptPrint print)
{
	if (out == NULL) { out = stdout; }
	if (print == NULL) { print = print_value; }

	if (t == NULL) {
		fprintf(out, "#<Eddy:BTree:(null)>\n");
		return;
	}

	fprintf(out, "#<Eddy:BTree:%p> {\n", (void *)t);
	bool stack[16] = {1};
	fwrite(space, 1, 4, out);
	print_node(fd, esize, t, out, print, stack, 1);
	fprintf(out, "}\n");
}

int
ed_bpt_verify(EdBpt *t, int fd, size_t esize, FILE *out)
{
	return verify_node(fd, esize, t, out, 0, UINT64_MAX);
}

