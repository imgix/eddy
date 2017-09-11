#include "eddy-private.h"

#define ED_TXN_FCRIT (ED_FNOTLCK)

#define ed_txn_fclose(f, crit) ((f) & (~(ED_TXN_FCRIT|ED_FNOBLOCK)) | (crit))

/**
 * @brief  Calculates the allocation size for a node array
 * @param  nslot  The number of nodes desired
 * @return  Size in bytes of the allocation required
 */
static inline size_t
node_size(unsigned nslot)
{
	return sizeof(EdTxnNode) + (nslot-1)*sizeof(((EdTxnNode *)0)->nodes[0]);
}

/**
 * @brief  Allocates a new node array
 *
 * This will allocate the next array and push it onto the link list head.
 *
 * @param  head  Indirect pointer to the current head node
 * @param  nslot  Minimum size of the array
 * @return  0 on success, <0 on error
 */
static inline int
node_alloc(EdTxnNode **head, unsigned nslot)
{
	nslot = ed_power2(nslot);
	EdTxnNode *next = calloc(1, node_size(nslot));
	if (next == NULL) { return ED_ERRNO; }
	next->next = *head;
	next->nslot = nslot;
	*head = next;
	return 0;
}

/**
 * @brief   Wraps a mapped page into a node.
 *
 * Nodes are a memory representation used to simplify tracking parent
 * relationships as well as the dirty state of the page's contents.
 */
static EdNode *
node_wrap(EdTxn *txn, EdPg *pg, EdNode *par, uint16_t pidx)
{
	assert(txn != NULL);
	assert(txn->nodes->nused < txn->nodes->nslot);
	assert(pg != NULL);

	EdNode *n = &txn->nodes->nodes[txn->nodes->nused++];
	n->page = pg;
	n->parent = par;
	n->pindex = pidx;
	return n;
}

/**
 * @brief  Checks if a node is valid for a transaction
 *
 * If the transaction is valid, then all nodes within the transaction are valid.
 * However, in an erroring transaction, only nodes from prior transactions can
 * be considered valid.
 */
static bool
node_is_live(EdTxn *txn, EdNode *node)
{
	return node && node->tree && !node->gc &&
		(txn->error == 0 || node->tree->xid < txn->xid);
}

int
ed_txn_new(EdTxn **txnp, EdIdx *idx)
{
	assert(txnp != NULL);
	assert(idx != NULL);

	int rc = 0;
	EdTxn *txn;
	unsigned nslot = ed_len(txn->db) * 12;
	size_t sznodes = node_size(nslot);
	size_t offnodes = ed_align_max(sizeof(*txn));

	if ((txn = calloc(1, offnodes + sznodes)) == NULL) {
		rc = ED_ERRNO;
		goto error;
	}

	txn->idx = idx;
	txn->nodes = (EdTxnNode *)((uint8_t *)txn + offnodes);
	txn->nodes->nslot = nslot;

	txn->db[ED_DB_KEYS].entry_size = sizeof(EdEntryKey);
	txn->db[ED_DB_BLOCKS].entry_size = sizeof(EdEntryBlock);

	for (unsigned i = 0; i < ed_len(txn->db); i++) {
		EdPgno *no = &idx->hdr->tree[i];
		if (*no != ED_PG_NONE) {
			rc = ed_txn_map(txn, *no, NULL, 0, &txn->db[i].root);
			if (rc < 0) { break; }
			assert(txn->db[i].root != NULL && txn->db[i].root->page != NULL &&
				(txn->db[i].root->page->type == ED_PG_BRANCH || txn->db[i].root->page->type == ED_PG_LEAF));
			txn->db[i].find = txn->db[i].root;
		}
		txn->db[i].no = no;
	}

	if (rc < 0) { goto error; }
	*txnp = txn;
	return 0;

error:
	ed_txn_close(&txn, ED_FNOSYNC);
	return rc;
}

int
ed_txn_open(EdTxn *txn, uint64_t flags)
{
	if (txn == NULL || txn->isopen) { return ed_esys(EINVAL); }
	if (txn->idx->pid != getpid()) { return ED_EINDEX_FORK; }
	bool rdonly = flags & ED_FRDONLY;
	int rc = 0;

	if (!rdonly) {
		rc = ed_lck(&txn->idx->lck, txn->idx->fd, ED_LCK_EX, flags);
		if (rc < 0) { return rc; }
	}

	rc = ed_idx_acquire_snapshot(txn->idx, txn->roots);
	if (rc < 0) { return rc; };

	for (int i = 0; i < ED_NDB; i++) {
		txn->db[i].find = txn->db[i].root = txn->roots[i] ?
			node_wrap(txn, (EdPg *)txn->roots[i], NULL, 0) : NULL;
		txn->roots[i] = NULL;
	}

	if (!rdonly) {
		EdPgIdx *hdr = txn->idx->hdr;
		txn->xid = hdr->xid + 1;

		// Any active pages at this point are a result from an abandoned transaction.
		// These could be reused right away, but for simlicity they are moved into
		// the free list. These pages MUST NOT be allowed in both the free list AND
		// the active list at the same time. It is preferrable to risk leaking pages.
		EdPgno nactive = hdr->nactive;
		if (nactive > 0) {
			hdr->nactive = 0;
			rc = ed_free_pgno(txn->idx, 0, hdr->active, nactive);
			if (rc < 0) {
				hdr->nactive = nactive;
				ed_lck(&txn->idx->lck, txn->idx->fd, ED_LCK_UN, flags);
				return rc;
			}
			memset(hdr->active, 0xff, nactive * sizeof(hdr->active[0]));
		}

		// Split pending pages into active and inactive groups. Active pages are the
		// pages mapped into the transaction page cache. Inactive pages need to be
		// returned to the free list, and active pages get recorded in the active list.
		EdConn *conn = &hdr->conns[txn->idx->conn];
		EdPgno inactive[ed_len(conn->pending)], ninactive = 0;
		EdPgno npg = txn->npg, npending = conn->npending;
		for (EdPgno i = 0, j; i < npending; i++) {
			EdPgno no = conn->pending[i];
			for (j = 0; j < npg && no == txn->pg[j]->no; j++) {
				if (no == txn->pg[j]->no) { break; }
			}
			if (j == npg) { inactive[ninactive++] = no; }
		}
		if (npending > 0) {
			conn->npending = 0;
			memset(conn->pending, 0xff, ed_len(conn->pending) * sizeof(conn->pending[0]));
		}
		ed_free_pgno(txn->idx, 0, inactive, ninactive);
		if (npg > 0) {
			for (EdPgno i = 0; i < npg; i++) {
				hdr->active[i] = txn->pg[i]->no;
			}
			hdr->nactive = npg;
		}
	}

	txn->cflags = flags & ED_TXN_FCRIT;
	txn->isrdonly = rdonly;
	txn->isopen = true;
	return 0;
}

int
ed_txn_commit(EdTxn **txnp, uint64_t flags)
{
	EdTxn *txn = *txnp;
	int rc = 0;

	if (txn == NULL || !txn->isopen || txn->isrdonly) {
		rc = ED_EINDEX_RDONLY;
		goto close;
	}

	ed_fault_trigger(COMMIT_BEGIN);

	EdPgIdx *hdr = txn->idx->hdr;

	// Unmark all active pages. This will leak pages until the close completes.
	EdPgno nactive = hdr->nactive;
	hdr->nactive = 0;
	memset(hdr->active, 0xff, nactive * sizeof(hdr->active[0]));
	ed_fault_trigger(ACTIVE_CLEARED);

	if (txn->error < 0) {
		rc = ED_EINDEX_RDONLY;
		goto close;
	}

	// Shift over any unused pages to the start of the array. When closed, these
	// will either be used for the next transaction or discarded.
	memmove(txn->pg, txn->pg+txn->npgused,
			(txn->npg-txn->npgused) * sizeof(txn->pg[0]));
	txn->npg -= txn->npgused;
	txn->npgused = 0;

	// Collect tree root page updates.
	union { uint64_t vtree; EdPgno tree[2]; } update;
	for (unsigned i = 0; i < ed_len(txn->db); i++) {
		EdNode *root = txn->db[i].root;
		update.tree[i] = root && root->page ? root->page->no : ED_PG_NONE;
	}

	// Updating the tree pages first means a reader could hold an xid that is
	// older than the committed tree pages. This is still a valid state, however,
	// the opposite is not.
	hdr->vtree = update.vtree;
	ed_fault_trigger(UPDATE_TREE);
	hdr->xid = txn->xid;

	// Pass all replaced pages to be reused. If this fails they are leaked.
	ed_free(txn->idx, txn->xid, txn->gc, txn->ngcused);
	txn->ngcused = 0;

close:
	ed_txn_close(txnp, flags);
	return rc;
}

void
ed_txn_close(EdTxn **txnp, uint64_t flags)
{
	EdTxn *txn = *txnp;
	if (txn == NULL) { return; }
	flags = ed_txn_fclose(flags, txn->cflags);

	ed_fault_trigger(CLOSE_BEGIN);

	// Stash the mapped heads back into the roots array.
	for (unsigned i = 0; i < ed_len(txn->db); i++) {
		if (node_is_live(txn, txn->db[i].root)) {
			txn->roots[i] = txn->db[i].root->tree;
			txn->db[i].root->tree = NULL;
		}
	}

	// Unmap all node pages, and free extra node lists.
	EdTxnNode *node = txn->nodes;
	do {
		for (int i = (int)node->nused-1; i >= 0; i--) {
			if (node_is_live(txn, &node->nodes[i])) {
				ed_pg_unmap(node->nodes[i].page, 1);
				node->nodes[i].page = NULL;
			}
		}
		EdTxnNode *next = node->next;
		if (next == NULL) {
			// The first node array is allocated with the txn, so don't free.
			txn->nodes = node;
			break;
		}
		free(node);
		node = next;
	} while(1);

	// Mark all pages saved for a subsequent transaction as pending. This list is
	// limited in size, so extra pages must be freed. If we aren't reseting, free
	// all the pages.
	EdPg **pg = txn->pg;
	EdPgno npg = txn->npg;
	bool locked = false;

	if (txn->isopen && !txn->isrdonly) {
		locked = true;
	}
	else if (npg > 0) {
		if (ed_lck(&txn->idx->lck, txn->idx->fd, ED_LCK_EX, flags) == 0) {
			locked = true;
		}
	}

	if (flags & ED_FRESET) {
		if (txn->isopen) {
			ed_idx_release_xid(txn->idx);
		}
	}
	else {
		ed_idx_release_snapshot(txn->idx, txn->roots);
	}

	if (locked) {
		EdConn *conn = &txn->idx->hdr->conns[txn->idx->conn];
		ed_fault_trigger(PENDING_BEGIN);
		if (flags & ED_FRESET) {
			EdPgno keep = ed_len(conn->pending);
			if (keep > npg) { keep = npg; }
			for (EdPgno i = 0; i < keep; i++) {
				conn->pending[i] = pg[i]->no;
			}
			conn->npending = keep;
			pg += keep;
			npg -= keep;
			txn->npg = keep;
		}
		else {
			conn->npending = 0;
			memset(conn->pending, 0xff, ed_len(conn->pending) * sizeof(conn->pending[0]));
		}
		ed_fault_trigger(PENDING_FINISH);
		ed_free(txn->idx, 0, pg, npg);

		ed_lck(&txn->idx->lck, txn->idx->fd, ED_LCK_UN, flags);
		if (!(flags & ED_FNOSYNC)) {
			fsync(txn->idx->fd);
		}
	}

	if (flags & ED_FRESET) {
		txn->npgused = 0;
		txn->ngcused = 0;
		txn->nodes->nused = 0;
		memset(txn->nodes->nodes, 0, txn->nodes->nslot*sizeof(txn->nodes->nodes[0]));
		txn->error = 0;
		txn->isopen = false;
		for (int i = 0; i < ED_NDB; i++) {
			EdTxnDb *dbp = &txn->db[i];
			dbp->find = dbp->root = NULL;
			dbp->key = 0;
			dbp->start = NULL;
			dbp->entry = NULL;
			dbp->entry_index = 0;
			dbp->nsplits = 0;
			dbp->match = 0;
			dbp->nmatches = 0;
		}
	}
	else {
		free(txn->pg);
		free(txn->gc);
		free(txn);
		*txnp = NULL;
	}
}

int
ed_txn_map(EdTxn *txn, EdPgno no, EdNode *par, uint16_t pidx, EdNode **out)
{
	for (EdTxnNode *node = txn->nodes; node != NULL; node = node->next) {
		for (int i = (int)node->nused-1; i >= 0; i--) {
			if (node->nodes[i].page->no == no) {
				*out = &node->nodes[i];
				return 0;
			}
		}
	}

	if (txn->nodes->nused == txn->nodes->nslot) {
		int rc = node_alloc(&txn->nodes, txn->nodes->nslot+1);
		if (rc < 0) { return (txn->error = rc); }
	}

	EdPg *pg = ed_pg_map(txn->idx->fd, no, 1);
	if (pg == MAP_FAILED) { return (txn->error = ED_ERRNO); }
	*out = node_wrap(txn, pg, par, pidx);
	return 0;
}

int
ed_txn_alloc(EdTxn *txn, EdNode *par, uint16_t pidx, EdNode **out)
{
	unsigned npg = txn->npg;
	if (txn->npgused == npg) {
		if (txn->npgslot == npg) {
			unsigned npgslot = ed_len(txn->db)*5;
			npgslot = ED_ALIGN_SIZE(npg+1, npgslot);
			EdPg **pg = realloc(txn->pg, npgslot*sizeof(pg[0]));
			if (pg == NULL) { return (txn->error = ED_ERRNO); }
			txn->pg = pg;
			txn->npgslot = npgslot;
		}

		EdPgIdx *hdr = txn->idx->hdr;

		unsigned nalloc = txn->npgslot - npg;
		int rc = ed_alloc(txn->idx, txn->pg+npg, nalloc);
		if (rc < 0) { return (txn->error = rc); }

		if (hdr->nactive + nalloc > ed_len(hdr->active)) {
			nalloc = ed_len(hdr->active) - hdr->nactive;
		}

		// Mark as many pages as active that will fit. If the transaction is
		// abandoned, excess pages can only be recovered during a repair.
		for (unsigned i = 0; i < nalloc; i++) {
			hdr->active[hdr->nactive] = txn->pg[npg+i]->no;
			hdr->nactive++;
		}
		txn->npg = txn->npgslot;
	}

	if (txn->nodes == NULL || txn->nodes->nused == txn->nodes->nslot) {
		int rc = node_alloc(&txn->nodes, txn->nodes ? txn->nodes->nslot + 1 : npg);
		if (rc < 0) { return (txn->error = rc); }
	}

	EdNode *node = node_wrap(txn, txn->pg[txn->npgused++], par, pidx);
	node->tree->xid = txn->xid;
	*out = node;
	return 0;
}

int
ed_txn_calloc(EdTxn *txn, EdNode *par, uint16_t pidx, EdNode **out)
{
	EdNode *node;
	int rc = ed_txn_alloc(txn, par, pidx, &node);
	if (rc == 0) {
		node->tree->next = ED_PG_NONE;
		node->tree->nkeys = 0;
		memset(node->tree->data, 0, sizeof(node->tree->data));
		*out = node;
	}
	return rc;
}

int
ed_txn_clone(EdTxn *txn, EdNode *node, EdNode **out)
{
	EdNode *copy = NULL;
	int rc = ed_txn_alloc(txn, node->parent, node->pindex, &copy);
	assert(copy != NULL);
	if (rc >= 0) {
		copy->page->type = node->page->type;
		copy->tree->next = node->tree->next;
		copy->tree->nkeys = node->tree->nkeys;
		rc = ed_txn_discard(txn, node);
		if (rc < 0) {
			txn->npgused--;
			return rc;
		}
		*out = copy;
	}
	return rc;
}

int
ed_txn_discard(EdTxn *txn, EdNode *node)
{
	if (!node->gc) {
		unsigned ngcslot = txn->ngcslot;
		if (txn->ngcused == ngcslot) {
			ngcslot = ngcslot ? ngcslot * 2 : ed_len(txn->db)*12;
			EdPg **gc = realloc(txn->gc, ngcslot * sizeof(*gc));
			if (gc == NULL) { return (txn->error = ED_ERRNO); }
			txn->gc = gc;
			txn->ngcslot = ngcslot;
		}
		txn->gc[txn->ngcused++] = node->page;
		node->gc = true;
	}
	return 0;
}

EdTxnDb *
ed_txn_db(EdTxn *txn, unsigned db, bool reset)
{
	assert(db < ed_len(txn->db));
	EdTxnDb *dbp = &txn->db[db];
	if (reset) {
		dbp->find = dbp->root;
		dbp->entry = NULL;
		dbp->entry_index = 0;
		dbp->nsplits = 0;
		dbp->match = 0;
		dbp->nmatches = 0;
	}
	return dbp;
}

