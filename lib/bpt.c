#include "eddy-private.h"

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

_Static_assert(sizeof(EdBpt) == PAGESIZE,
		"EdBpt size invalid");

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

#define INS_NONE 0
#define INS_SHIFT 1
#define INS_NOSHIFT 2

static int
map_extra(EdTxn *tx, EdPgNode *parent, uint16_t idx, EdPgNode **out)
{
	assert(idx <= parent->tree->nkeys);
	EdPgno no = ed_fetch32(parent->tree->data + idx*BRANCH_ENTRY_SIZE);
	return ed_txn_map(tx, no, parent, idx, out);
}

static inline uint64_t
branch_key(EdBpt *b, uint16_t idx)
{
	assert(idx <= b->nkeys);
	if (idx == 0) { return 0; }
	return ed_fetch64(b->data + idx*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE);
}

static inline void
branch_set_key(EdPgNode *b, uint16_t idx, uint64_t val)
{
	assert(idx <= b->tree->nkeys);
	if (idx == 0) { return; }
	memcpy(b->tree->data + idx*BRANCH_ENTRY_SIZE - BRANCH_KEY_SIZE, &val, sizeof(val));
	b->dirty = 1;
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
	assert((uint8_t *)ptr < node->data + BRANCH_ORDER*BRANCH_ENTRY_SIZE);
	return ((uint8_t *)ptr - node->data) / BRANCH_ENTRY_SIZE;
}



size_t
ed_bpt_capacity(size_t esize, size_t depth)
{
	return llround(pow(BRANCH_ORDER, depth-1) * LEAF_ORDER(esize));
}

void
ed_bpt_init(EdBpt *bt)
{
	bt->base.type = ED_PG_LEAF;
	bt->nkeys = 0;
	bt->right = ED_PG_NONE;
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
ed_bpt_find(EdTxn *tx, unsigned db, uint64_t key, void **ent)
{
	if (!tx->isopen) {
		// FIXME: return proper error code
		return ed_esys(EINVAL);
	}

	EdTxnDb *dbp = ed_txn_db(tx, db, true);
	dbp->key = key;

	int rc = 0;
	uint32_t i = 0, n = 0;
	uint8_t *data = NULL;
	size_t esize = dbp->entry_size;

	if (dbp->head == NULL) {
		dbp->nsplits = 1;
		goto done;
	}

	EdPgNode *node = dbp->head;

	// The root node needs two pages when splitting.
	dbp->nsplits = IS_FULL(node->tree, esize);

	// Search down the branches of the tree.
	while (IS_BRANCH(node->tree)) {
		if (IS_BRANCH_FULL(node->tree)) { dbp->nsplits++; }
		else { dbp->nsplits = 0; }
		EdPgno *ptr = search_branch(node->tree, key);
		uint16_t bidx = branch_index(node->tree, ptr);
		EdPgno no = ed_fetch32(node->tree->data + bidx*BRANCH_ENTRY_SIZE);
		EdPgNode *next;
		rc = ed_txn_map(tx, no, node, bidx, &next);
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
		dbp->entry = data;
		dbp->entry_index = i;
		dbp->nmatches = rc;
		if (ent) { *ent = data; }
	}
	dbp->match = rc;
	return rc;
}

int
ed_bpt_next(EdTxn *tx, unsigned db, void **ent)
{
	EdTxnDb *dbp = &tx->db[db];
	if (dbp->match != 1) { return dbp->match; }

	int rc = 0;
	EdPgNode *node = dbp->tail;
	EdBpt *leaf = node->tree;
	uint8_t *p = dbp->entry;
	uint32_t i = dbp->entry_index;
	if (i == leaf->nkeys-1) {
		EdPgno right = leaf->right;
		if (right == ED_PG_NONE) { goto done; }

		rc = ed_txn_map(tx, right, node, 0, &node);
		if (rc < 0) { goto done; }

		leaf = node->tree;
		if (leaf->base.type != ED_PG_OVERFLOW) { goto done; }

		p = leaf->data;
		i = 0;
	}
	else {
		p += dbp->entry_size;
		i++;
	}
	dbp->entry = p;
	dbp->entry_index = i;
	if (dbp->key == ed_fetch64(p)) {
		dbp->tail = node;
		dbp->nmatches++;
		if (ent) { *ent = p; }
		rc = 1;
	}

done:
	dbp->match = rc;
	return rc;
}

int
ed_bpt_set(EdTxn *tx, unsigned db, const void *ent, bool replace)
{
	if (tx->isrdonly) { return ed_esys(EINVAL); }
	EdTxnDb *dbp = ed_txn_db(tx, db, false);
	if (ed_fetch64(ent) != dbp->key) { return ED_EINDEX_KEY_MATCH; }
	memcpy(dbp->scratch, ent, dbp->entry_size);
	if (replace && dbp->match == 1) {
		dbp->apply = ED_BPT_REPLACE;
		return 0;
	}
	else {
		dbp->apply = ED_BPT_INSERT;
		return 1;
	}
}

int
ed_bpt_del(EdTxn *tx, unsigned db)
{
	if (tx->isrdonly) { return ed_esys(EINVAL); }
	EdTxnDb *dbp = ed_txn_db(tx, db, false);
	if (dbp->match == 1) {
		dbp->apply = ED_BPT_DELETE;
		return 1;
	}
	else {
		dbp->apply = ED_BPT_NONE;
		return 0;
	}
}

static int
redistribute_leaf_left(EdTxn *tx, EdTxnDb *dbp, EdPgNode *leaf)
{
	// If the leaf is the first child, don't redistribute.
	if (leaf->pindex == 0) { return INS_NONE; }
	// Don't move left if the new insert is at the beggining.
	if (dbp->entry_index == 0) { return INS_NONE; }

	EdPgNode *ln;
	int rc = map_extra(tx, leaf->parent, leaf->pindex-1, &ln);
	if (rc < 0) { return rc == -1 ? INS_NONE : rc; }

	size_t esize = dbp->entry_size;
	uint16_t ord = LEAF_ORDER(esize);
	EdBpt *l = ln->tree;

	// Bail if the left leaf is full.
	if (l->nkeys == ord) { return INS_NONE; }

	// Move half the available nodes into the left leaf.
	uint32_t n = (ord - l->nkeys + 1) / 2;
	// But don't move past the entry index.
	uint32_t max = dbp->entry_index;
	if (n > max) { n = max; }

	// Check if the split point spans a repeating key.
	size_t size = n * esize;
	uint8_t *src = leaf->tree->data;
	uint64_t key = ed_fetch64(src + size);
	if (key == ed_fetch64(src + size - esize)) {
		// TODO: find an available split point
		return INS_NONE;
	}

	// Move the lower range of leaf into the end of left.
	uint8_t *dst = l->data + esize*l->nkeys;
	memcpy(dst, src, size);
	memmove(src, src+size, (leaf->tree->nkeys - n) * esize);
	branch_set_key(leaf->parent, leaf->pindex, key);

	// Update leaf counts.
	l->nkeys += n;
	leaf->tree->nkeys -= n;

	// Slide the entry index down.
	dbp->entry_index -= n;
	dbp->entry = leaf->tree->data + dbp->entry_index*esize;

	ln->dirty = 1;
	leaf->dirty = 1;

	return INS_SHIFT;
}

static int
redistribute_leaf_right(EdTxn *tx, EdTxnDb *dbp, EdPgNode *leaf)
{
	// If the leaf is the last child, don't redistribute.
	if (leaf->pindex == leaf->parent->tree->nkeys) { return INS_NONE; }

	size_t esize = dbp->entry_size;
	uint16_t ord = LEAF_ORDER(esize);

	// Don't move right if the insert is at the end.
	if (dbp->entry_index == ord) { return INS_NONE; }

	EdPgNode *rn;
	int rc = map_extra(tx, leaf->parent, leaf->pindex+1, &rn);
	if (rc < 0) { return rc == -1 ? INS_NONE : rc; }

	EdBpt *r = rn->tree;

	// Bail if the right leaf is full.
	if (r->nkeys == ord) { return INS_NONE; }

	// Move half the available nodes into the right leaf.
	uint32_t n = (ord - r->nkeys + 1) / 2;
	// But don't move past the entry index.
	uint32_t max = (uint32_t)ord - dbp->entry_index;
	if (n > max) { n = max; }

	// Check if the split point spans a repeating key.
	uint8_t *src = leaf->tree->data + esize*(ord - n);
	uint64_t key = ed_fetch64(src);
	if (key == ed_fetch64(src - esize)) {
		// TODO: find an available split point
		return INS_NONE;
	}

	// Move upper range of leaf into the start of right.
	uint8_t *dst = r->data;
	size_t size = n * esize;
	memmove(dst+size, dst, r->nkeys*esize);
	memcpy(dst, src, size);
	branch_set_key(leaf->parent, leaf->pindex+1, key);

	// Update leaf counts.
	r->nkeys += n;
	leaf->tree->nkeys -= n;

	rn->dirty = 1;
	leaf->dirty = 1;

	return INS_SHIFT;
}

static int
redistribute_leaf(EdTxn *tx, EdTxnDb *dbp, EdPgNode *leaf)
{
	if (leaf->parent == NULL) { return INS_NONE; }

	int rc = redistribute_leaf_right(tx, dbp, leaf);
	if (rc == 0) {
		rc = redistribute_leaf_left(tx, dbp, leaf);
	}
	return rc;
}

static void
insert_into_parent(EdTxn *tx, EdTxnDb *dbp, EdPgNode *left, EdPgNode *right, uint64_t rkey)
{
	EdPgNode *parent = left->parent;
	EdBpt *p;
	EdPgno r = right->tree->base.no;
	size_t pos;

	// No parent, so create new root node.
	if (parent == NULL) {
		// Initialize new parent node from the allocation array.
		parent = ed_txn_alloc(tx, NULL, 0);
		dbp->head = parent;
		p = parent->tree;
		p->base.type = ED_PG_BRANCH;
		p->nkeys = 0;
		p->right = ED_PG_NONE;

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

			EdPgNode *leftb = parent, *rightb = ed_txn_alloc(tx, leftb->parent, leftb->pindex + 1);

			rightb->tree->base.type = ED_PG_BRANCH;
			rightb->tree->nkeys = n - mid;
			rightb->tree->right = leftb->tree->right;
			leftb->tree->nkeys = mid - 1;
			leftb->tree->right = rightb->tree->base.no;
			leftb->dirty = 1;

			memcpy(rightb->tree->data, leftb->tree->data+off, sizeof(leftb->tree->data) - off);

			if (rkey >= rbkey) {
				index -= mid;
				p = rightb->tree;
			}

			insert_into_parent(tx, dbp, leftb, rightb, rbkey);
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
overflow_leaf(EdTxn *tx, EdTxnDb *dbp, EdPgNode *leaf)
{
	assert(leaf->tree->nkeys == LEAF_ORDER(dbp->entry_size));

	EdPgNode *node;
	size_t esize = dbp->entry_size;

	if (leaf->tree->right != ED_PG_NONE) {
		if (ed_txn_map(tx, leaf->tree->right, leaf, 0, &node) == 0) {
			if (node->tree->base.type == ED_PG_OVERFLOW && node->tree->nkeys < LEAF_ORDER(esize)) {
				goto done;
			}
		}
	}

	node = ed_txn_alloc(tx, leaf, 0);
	node->tree->base.type = ED_PG_OVERFLOW;
	node->tree->nkeys = 0;
	node->tree->right = leaf->tree->right;
	leaf->tree->right = node->tree->base.no;

done:
	dbp->tail = node;
	dbp->entry = node->tree->data + esize*node->tree->nkeys;
	dbp->entry_index = node->tree->nkeys;
	return INS_NOSHIFT;
}

static int
split_leaf(EdTxn *tx, EdTxnDb *dbp, EdPgNode *leaf, int mid)
{
	size_t esize = dbp->entry_size;
	uint32_t n = leaf->tree->nkeys;
	size_t off = mid * esize;

	// If the new key will be the first entry on right, use the search key.
	uint64_t rkey = (uint16_t)mid == dbp->entry_index ?
		dbp->key : ed_fetch64(leaf->tree->data+off);

	assert(ed_fetch64(leaf->tree->data + off - esize) < rkey);
	assert(rkey <= ed_fetch64(leaf->tree->data + off));

	EdPgNode *left = leaf, *right = ed_txn_alloc(tx, left->parent, left->pindex + 1);

	right->tree->base.type = ED_PG_LEAF;
	right->tree->nkeys = n - mid;
	right->tree->right = left->tree->right;
	left->tree->nkeys = mid;
	left->tree->right = right->tree->base.no;
	left->dirty = 1;

	memcpy(right->tree->data, left->tree->data+off, sizeof(left->tree->data) - off);

	if (dbp->entry_index >= (uint16_t)mid) {
		dbp->entry_index -= mid;
		dbp->entry = right->tree->data + dbp->entry_index*esize;
		dbp->tail = right;
	}

	insert_into_parent(tx, dbp, left, right, rkey);

	return INS_SHIFT;
}

void
ed_bpt_apply(EdTxn *tx, unsigned db, const void *ent, EdBptApply a)
{
	EdTxnDb *dbp = ed_txn_db(tx, db, false);
	EdPgNode *leaf = dbp->tail;
	size_t esize = dbp->entry_size;
	int rc = INS_SHIFT;

	switch (a) {

	case ED_BPT_NONE:
		break;

	case ED_BPT_INSERT:
		// If the root was NULL, create a new root node.
		if (leaf == NULL) {
			leaf = ed_txn_alloc(tx, NULL, 0);
			leaf->tree->base.type = ED_PG_LEAF;
			leaf->tree->right = ED_PG_NONE;
			dbp->entry = leaf->tree->data;
			dbp->entry_index = 0;
			dbp->head = dbp->tail = leaf;
			rc = INS_NOSHIFT;
		}
		// Otherwise use the leaf from the search.
		// If the leaf is full, it needs to be redistributed or split.
		else if (IS_LEAF_FULL(leaf->tree, esize)) {
			rc = redistribute_leaf(tx, dbp, leaf);
			if (rc == INS_NONE) {
				int mid = split_point(dbp, leaf->tree);
				rc = mid < 0 ?
					overflow_leaf(tx, dbp, leaf) :
					split_leaf(tx, dbp, leaf, mid);
				leaf = dbp->tail;
			}
		}

		// Insert the new entry before the current entry position.
		uint8_t *p = dbp->entry;
		assert(dbp->entry_index < LEAF_ORDER(esize));
		assert(p == leaf->tree->data + (dbp->entry_index*esize));

		if (rc == INS_SHIFT) {
			memmove(p+esize, p, (leaf->tree->nkeys - dbp->entry_index) * esize);
		}
		memcpy(p, ent, esize);
		leaf->tree->nkeys++;
		if (dbp->entry_index == 0 && leaf->parent != NULL) {
			branch_set_key(leaf->parent, leaf->pindex, ed_fetch64(p));
		}
		if (*dbp->root != dbp->head->page->no) {
			*dbp->root = dbp->head->page->no;
		}
		dbp->nsplits = 0;
		dbp->match = 1;
		break;

	case ED_BPT_REPLACE:
		memcpy(dbp->entry, ent, dbp->entry_size);
		break;

	case ED_BPT_DELETE:
		memmove(dbp->entry, (uint8_t *)dbp->entry + esize,
				(leaf->tree->nkeys - dbp->entry_index - 1) * esize);
		if (leaf->tree->nkeys == 1) {
			// FIXME: this is terrible
			leaf->tree->nkeys = 0;
			leaf->dirty = 1;
		}
		else {
			leaf->tree->nkeys--;
			leaf->dirty = 1;
			if (leaf->parent && dbp->entry_index == 0) {
				branch_set_key(leaf->parent, leaf->pindex, ed_fetch64(dbp->entry));
			}
		}
		break;
	}
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
	fprintf(out, "%s p%u, nkeys=%u/%zu, right=p%jd",
			leaf->base.type == ED_PG_LEAF ? "leaf" : "overflow",
			leaf->base.no, leaf->nkeys, LEAF_ORDER(esize),
			leaf->right == ED_PG_NONE ? INTMAX_C(-1) : (intmax_t)leaf->right);

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

	if (leaf->right != ED_PG_NONE) {
		EdBpt *next = ed_pg_map(fd, leaf->right, 1);
		if (next->base.type == ED_PG_OVERFLOW) {
			print_tree(out, stack, top-1);
			fprintf(out, "= %llu, ", ed_fetch64(next->data));
			print_leaf(fd, esize, next, out, print, stack, top);
		}
		ed_pg_unmap(next, 1);
	}
}

static void
print_branch(int fd, size_t esize, EdBpt *branch, FILE *out, EdBptPrint print, bool *stack, int top)
{
	fprintf(out, "branch p%u, nkeys=%u/%zu, right=p%jd\n",
			branch->base.no, branch->nkeys, BRANCH_ORDER,
			branch->right == ED_PG_NONE ? INTMAX_C(-1) : (intmax_t)branch->right);

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

	EdPgno ptr = l->right;
	while (ptr != ED_PG_NONE) {
		EdBpt *next = ed_pg_map(fd, ptr, 1);
		int rc = 0;
		if (next->base.type == ED_PG_OVERFLOW) {
			rc = verify_overflow(fd, esize, next, out, last);
			ptr = next->right;
		}
		else {
			ptr = ED_PG_NONE;
		}
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

