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
	assert(txn->nodes != NULL);
	assert(txn->nodes->nused < txn->nodes->nslot);
	assert(pg != NULL);

	EdNode *n = &txn->nodes->nodes[txn->nodes->nused++];
	n->page = pg;
	n->parent = par;
	n->pindex = pidx;
	return n;
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
	if (txn == NULL || txn->state != ED_TXN_CLOSED) { return ed_esys(EINVAL); }
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
		txn->pos = txn->idx->hdr->pos;

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
			memset(hdr->active, 0xff, sizeof(hdr->active));
		}

		// Split pending pages into active and inactive groups. Active pages are the
		// pages mapped into the transaction page cache. Inactive pages need to be
		// returned to the free list, and active pages get recorded in the active list.
		EdConn *conn = txn->idx->conn;
		assert(conn->npending <= ed_len(conn->pending));
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
			memset(conn->pending, 0xff, sizeof(conn->pending));
		}
		ed_free_pgno(txn->idx, 0, inactive, ninactive);
		if (npg > 0) {
			for (EdPgno i = 0; i < npg; i++) {
				hdr->active[i] = txn->pg[i]->no;
			}
			assert(npg <= ed_len(hdr->active));
			hdr->nactive = npg;
		}
	}

	txn->cflags = flags & ED_TXN_FCRIT;
	txn->isrdonly = rdonly;
	txn->state = ED_TXN_OPEN;
	return 0;
}

static void
flush_active(EdPgIdx *hdr)
{
	if (hdr->nactive) {
		hdr->nactive = 0;
		memset(hdr->active, 0xff, sizeof(hdr->active));
	}
}

int
ed_txn_commit(EdTxn **txnp, uint64_t flags)
{
	EdTxn *txn = *txnp;
	int rc = 0;

	if (txn == NULL || txn->state != ED_TXN_OPEN || txn->error < 0 || txn->isrdonly) {
		rc = ED_EINDEX_RDONLY;
		goto close;
	}

	ed_fault_trigger(COMMIT_BEGIN);

	EdPgIdx *hdr = txn->idx->hdr;

	// Unmark all the active pages. This will leave them unreferenced until the
	// close completes. A crash/kill during this period necessitates a repair.
	flush_active(hdr);
	ed_fault_trigger(ACTIVE_CLEARED);

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
	hdr->pos = txn->pos;

	// Pass all replaced pages to be reused. If this fails they are leaked.
	ed_free_pgno(txn->idx, txn->xid, txn->gc, txn->ngcused);
	txn->ngcused = 0;
	txn->state = ED_TXN_COMMITTED;

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

	EdTxnId xid = txn->xid;
	EdTxnState state = txn->state;
	if (state == ED_TXN_OPEN) {
		txn->state = state = ED_TXN_CANCELLED;
	}

	ed_fault_trigger(CLOSE_BEGIN);

	// Stash the mapped heads back into the roots array if they are still active.
	for (unsigned i = 0; i < ed_len(txn->db); i++) {
		EdNode *node = txn->db[i].root;
		if (node && (state == ED_TXN_COMMITTED || node->tree->xid != xid)) {
			txn->roots[i] = txn->db[i].root->tree;
			txn->db[i].root->tree = NULL;
		}
	}

	// Unmap all live node pages, and free extra node lists.
	EdTxnNode *nodes = txn->nodes;
	do {
		for (int i = (int)nodes->nused-1; i >= 0; i--) {
			EdNode *node = &nodes->nodes[i];
			if (node->page && (state == ED_TXN_COMMITTED || node->tree->xid != xid)) {
				ed_pg_unmap(node->page, 1);
			}
			node->page = NULL;
		}
		EdTxnNode *next = nodes->next;
		if (next == NULL) {
			// The first node array is allocated with the txn, so don't free.
			txn->nodes = nodes;
			break;
		}
		free(nodes);
		nodes = next;
	} while(1);

	// Mark all pages saved for a subsequent transaction as pending. This list is
	// limited in size, so extra pages must be freed. If we aren't reseting, free
	// all the pages.
	EdPg **pg = txn->pg;
	EdPgno npg = txn->npg;
	bool locked = false;

	if (txn->state >= ED_TXN_OPEN && !txn->isrdonly) {
		locked = true;
	}
	else if (npg > 0) {
		if (ed_lck(&txn->idx->lck, txn->idx->fd, ED_LCK_EX, flags) == 0) {
			locked = true;
		}
	}

	if (flags & ED_FRESET) {
		if (txn->state ) {
			ed_idx_release_xid(txn->idx);
		}
	}
	else {
		ed_idx_release_snapshot(txn->idx, txn->roots);
	}

	if (locked) {
		flush_active(txn->idx->hdr);

		EdConn *conn = txn->idx->conn;
		ed_fault_trigger(PENDING_BEGIN);
		if (flags & ED_FRESET) {
			EdPgno keep = ed_len(conn->pending);
			if (keep > npg) { keep = npg; }
			for (EdPgno i = 0; i < keep; i++) {
				conn->pending[i] = pg[i]->no;
			}
			assert(keep <= ed_len(conn->pending));
			conn->npending = txn->npg = keep;
			pg += keep;
			npg -= keep;
		}
		else {
			conn->npending = 0;
			memset(conn->pending, 0xff, sizeof(conn->pending));
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
		txn->xid = 0;
		txn->state = ED_TXN_CLOSED;
		txn->error = 0;
		for (int i = 0; i < ED_NDB; i++) {
			EdTxnDb *dbp = &txn->db[i];
			dbp->find = dbp->root = NULL;
			dbp->key = 0;
			dbp->kmin = 0;
			dbp->kmax = 0;
			dbp->start = NULL;
			dbp->entry = NULL;
			dbp->entry_index = 0;
			dbp->nsplits = 0;
			dbp->match = 0;
			dbp->nmatches = 0;
			dbp->nloops = 0;
			dbp->haskey = false;
			dbp->hasfind = false;
		}
	}
	else {
		free(txn->pg);
		free(txn->gc);
		free(txn);
		*txnp = NULL;
	}
}

EdBlkno
ed_txn_block(const EdTxn *txn)
{
	if (txn->state != ED_TXN_OPEN || txn->error < 0 || txn->isrdonly) {
		return txn->idx->hdr->pos;
	}
	return txn->pos;
}

int
ed_txn_set_block(EdTxn *txn, EdBlkno pos)
{
	if (txn == NULL || txn->state != ED_TXN_OPEN || txn->error < 0 || txn->isrdonly) {
		return ED_EINDEX_RDONLY;
	}
	txn->pos = pos;
	return 0;
}

bool
ed_txn_isrdonly(const EdTxn *txn)
{
	return txn->error < 0 || txn->isrdonly;
}

int
ed_txn_map(EdTxn *txn, EdPgno no, EdNode *par, uint16_t pidx, EdNode **out)
{
	for (EdTxnNode *node = txn->nodes; node != NULL; node = node->next) {
		for (int i = (int)node->nused-1; i >= 0; i--) {
			if (node->nodes[i].page->no == no) {
				node->nodes[i].parent = par;
				node->nodes[i].pindex = pidx;
				*out = &node->nodes[i];
				return 0;
			}
		}
	}

	if (txn->nodes->nused == txn->nodes->nslot) {
		int rc = node_alloc(&txn->nodes, txn->nodes->nslot+1);
		if (rc < 0) { return (txn->error = rc); }
	}

	EdPg *pg = ed_pg_map(txn->idx->fd, no, 1, true);
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
			// Try to ease into the page array size. This keeps the page allocation
			// cache small when the transaction is only used a few times. However,
			// if the transaction keeps getting reused, the buffer will expand. This
			// helps to balance the needs of single-use transactions--such as the
			// command line tools--with long-lived processes.
			if (npg > 0 && npg < ed_len(txn->db)*5) {
				txn->npgslot += ed_len(txn->db);
			}
			else {
				unsigned npgslot = ed_len(txn->db)*5;
				npgslot = ED_ALIGN_SIZE(npg+1, npgslot);
				EdPg **pg = realloc(txn->pg, npgslot*sizeof(pg[0]));
				if (pg == NULL) { return (txn->error = ED_ERRNO); }
				txn->pg = pg;
				txn->npgslot = npg ? npgslot : ed_len(txn->db);
			}
		}

		EdPgIdx *hdr = txn->idx->hdr;
		EdPgno nactive = hdr->nactive;

		unsigned nalloc = txn->npgslot - npg;
		int rc = ed_alloc(txn->idx, txn->pg+npg, nalloc, true);
		if (rc < 0) { return (txn->error = rc); }

		if (nactive + nalloc > ed_len(hdr->active)) {
			nalloc = ed_len(hdr->active) - nactive;
		}

		// Mark as many pages as active that will fit. If the transaction is
		// abandoned, excess pages can only be recovered during a repair.
		for (unsigned i = 0; i < nalloc; i++) {
			hdr->active[nactive++] = txn->pg[npg+i]->no;
		}
		txn->npg = txn->npgslot;
		assert(nactive <= ed_len(hdr->active));
		hdr->nactive = nactive;
	}

	assert(txn->nodes != NULL);
	if (txn->nodes->nused == txn->nodes->nslot) {
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
	EdNode *node = NULL;
	int rc = ed_txn_alloc(txn, par, pidx, &node);
	if (rc == 0) {
		assert(node != NULL);
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
			EdPgno *gc = realloc(txn->gc, ngcslot * sizeof(*gc));
			if (gc == NULL) { return (txn->error = ED_ERRNO); }
			txn->gc = gc;
			txn->ngcslot = ngcslot;
		}
		txn->gc[txn->ngcused++] = node->page->no;
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

