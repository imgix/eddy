#include "eddy-private.h"

#define ED_TXN_FCRIT (ED_FNOTLCK)

#define ed_txn_fclose(f, crit) ((f) & (~(ED_TXN_FCRIT|ED_FNOBLOCK)) | (crit))

/**
 * @brief  Calculates the allocation size for a node array
 * @param  nnodes  The number of nodes desired
 * @return  Size in bytes of the allocation required
 */
static inline size_t
node_size(unsigned nnodes)
{
	return sizeof(EdTxnNode) + (nnodes-1)*sizeof(((EdTxnNode *)0)->nodes[0]);
}

/**
 * @brief  Allocates a new node array
 *
 * This will allocate the next array and push it onto the link list head.
 *
 * @param  head  Indirect pointer to the current head node
 * @param  nnodes  Minimum size of the array
 * @return  0 on success, <0 on error
 */
static inline int
node_alloc(EdTxnNode **head, unsigned nnodes)
{
	nnodes = ed_power2(nnodes);
	EdTxnNode *next = calloc(1, node_size(nnodes));
	if (next == NULL) { return ED_ERRNO; }
	next->next = *head;
	next->nnodes = nnodes;
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
	assert(txn->nodes->nnodesused < txn->nodes->nnodes);
	assert(pg != NULL);

	EdNode *n = &txn->nodes->nodes[txn->nodes->nnodesused++];
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
	return node && node->tree && (txn->error == 0 || node->tree->xid < txn->xid);
}

/**
 * @brief  Gets the minimum active transaction id
 */
static EdTxnId
get_min_xid(EdTxnType *xtype, EdTxnId xmin, EdTime tmin)
{
	EdConn *c = xtype->conns;
	int nconns = xtype->nconns;
	int conn = xtype->conn;
	EdTxnId xid = *xtype->gxid;

	for (int i = 0; i < nconns; i++, c++) {
		if (i == conn || c->pid == 0 || c->xid == 0) { continue; }
		if (c->xid < xmin || (tmin > 0 && c->active > 0 && tmin < c->active)) {
			printf("TODO trylock\n");
#if 0
			if (lock() == 0) {
				memset(c, 0, sizeof(*c));
				unlock();
				continue;
			}
#endif
		}
		if (c->xid < xid) { xid = c->xid; }
	}
	return xid;
}

int
ed_txn_new(EdTxn **txnp, EdTxnType *xtype, EdTxnRef *ref, unsigned nref)
{
	if (nref == 0 || nref > ED_TXN_MAX_REF) {
		return ed_esys(EINVAL);
	}

	unsigned nnodes = nref * 8;
	int rc = 0;
	EdTxn *txn;
	size_t sztx = sizeof(*txn) + (nref-1)*sizeof(txn->db[0]);
	size_t sznodes = node_size(nnodes);
	size_t offnodes = ed_align_max(sztx);

	if ((txn = calloc(1, offnodes + sznodes)) == NULL) {
		rc = ED_ERRNO;
		goto error;
	}

	txn->xtype = xtype;
	txn->nodes = (EdTxnNode *)((uint8_t *)txn + offnodes);
	txn->nodes->nnodes = nnodes;

	for (unsigned i = 0; i < nref; i++) {
		EdPgno *no = ref[i].no;
		if (no == NULL) { rc = ed_esys(EINVAL); break; }
		if (*no != ED_PG_NONE) {
			rc = ed_txn_map(txn, *no, NULL, 0, &txn->db[i].head);
			if (rc < 0) { break; }
			assert(txn->db[i].head != NULL && txn->db[i].head->page != NULL &&
				(txn->db[i].head->page->type == ED_PG_BRANCH || txn->db[i].head->page->type == ED_PG_LEAF));
			txn->db[i].tail = txn->db[i].head;
		}
		txn->db[i].no = no;
		txn->db[i].entry_size = ref[i].entry_size;
		txn->ndb++;
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
	bool rdonly = flags & ED_FRDONLY;
	if (rdonly) {
		EdConn *conn = &txn->xtype->conns[txn->xtype->conn];
		conn->xid = *txn->xtype->gxid;
		conn->active = ed_time_from_unix(txn->xtype->epoch, ed_now_unix());
	}
	else {
		int rc = ed_lck(&txn->xtype->lck, txn->xtype->alloc.fd, ED_LCK_EX, flags);
		if (rc < 0) { return rc; }
		txn->xid = *txn->xtype->gxid + 1;
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

	if (txn == NULL || !txn->isopen || txn->isrdonly || txn->error < 0) {
		rc = ed_esys(EINVAL);
		goto close;
	}
	else {
		EdPg *gc[txn->npg];
		unsigned ngc = 0;
		for (EdTxnNode *node = txn->nodes; node; node = node->next) {
			for (int i = (int)node->nnodesused-1; i >= 0; i--) {
				if (node->nodes[i].gc) {
					gc[ngc++] = node->nodes[i].gc->page;
					node->nodes[i].gc->page = NULL;
					node->nodes[i].gc = NULL;
				}
			}
		}
		rc = ed_gc_put(&txn->xtype->alloc, txn->xid, gc, ngc);
		if (rc < 0) { goto close; }
	}

	memmove(txn->pg, txn->pg+txn->npgused, (txn->npg-txn->npgused) * sizeof(txn->pg[0]));
	txn->npg -= txn->npgused;
	txn->npgused = 0;

	// FIXME: this needs to be atomic
	// Possibly move page numbers to a sequential array. That way, 1, 2, and 4 way
	// transactions can use lock;cmpxchg.
	for (unsigned i = 0; i < txn->ndb; i++) {
		EdNode *head = txn->db[i].head;
		*txn->db[i].no = head && head->page ? head->page->no : ED_PG_NONE;
	}

	// Updating the tree pages first means a reader could hold an xid that is
	// older than the committed tree pages. This is still a valid state, however.
	*txn->xtype->gxid = txn->xid;

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

	if (txn->isopen) {
		EdTime t = ed_time_from_unix(txn->xtype->epoch, ed_now_unix());

		if (txn->isrdonly) {
			EdConn *conn = &txn->xtype->conns[txn->xtype->conn];
			conn->xid = 0;
			conn->active = t;
		}
		else {
			if (!(flags & ED_FNOVACUUM)) {
				EdTxnId xmin = *txn->xtype->gxid > 16 ? *txn->xtype->gxid - 16 : 0;
				EdTxnId xid = get_min_xid(txn->xtype, xmin, t - 10);
				ed_gc_run(&txn->xtype->alloc, xid, 2);
			}
			if (!(flags & ED_FNOSYNC)) {
				fsync(txn->xtype->alloc.fd);
			}
			ed_lck(&txn->xtype->lck, txn->xtype->alloc.fd, ED_LCK_UN, flags);
		}
	}

	// If reseting for reuse, stash the mapped heads so they don't get unmapped.
	EdPg *heads[ED_TXN_MAX_REF];
	if (flags & ED_FRESET) {
		for (unsigned i = 0; i < txn->ndb; i++) {
			EdTxnDb *dbp = &txn->db[i];
			if (node_is_live(txn, dbp->head)) {
				heads[i] = dbp->head->page;
				dbp->head->page = NULL;
			}
			else {
				heads[i] = NULL;
			}
		}
	}

	EdTxnNode *node = txn->nodes;
	do {
		for (int i = (int)node->nnodesused-1; i >= 0; i--) {
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

	if (flags & ED_FRESET) {
		txn->npgused = 0;
		txn->nodes->nnodesused = 0;
		txn->error = 0;
		txn->isopen = false;
		for (unsigned i = 0; i < txn->ndb; i++) {
			EdTxnDb *dbp = &txn->db[i];
			// Restore the stashed mapped head page.
			dbp->tail = dbp->head = heads[i] ? node_wrap(txn, heads[i], NULL, 0) : NULL;
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
		ed_free(&txn->xtype->alloc, txn->pg, txn->npg);
		free(txn->pg);
		free(txn);
		*txnp = NULL;
	}
}

int
ed_txn_map(EdTxn *txn, EdPgno no, EdNode *par, uint16_t pidx, EdNode **out)
{
	for (EdTxnNode *node = txn->nodes; node != NULL; node = node->next) {
		for (int i = (int)node->nnodesused-1; i >= 0; i--) {
			if (node->nodes[i].page->no == no) {
				*out = &node->nodes[i];
				return 0;
			}
		}
	}

	if (txn->nodes->nnodesused == txn->nodes->nnodes) {
		int rc = node_alloc(&txn->nodes, txn->nodes->nnodes+1);
		if (rc < 0) { return rc; }
	}

	EdPg *pg = ed_pg_map(txn->xtype->alloc.fd, no, 1);
	if (pg == MAP_FAILED) { return ED_ERRNO; }
	*out = node_wrap(txn, pg, par, pidx);
	return 0;
}

int
ed_txn_alloc(EdTxn *txn, EdNode *par, uint16_t pidx, EdNode **out)
{
	unsigned npg = txn->npg, need = txn->npgused + 1;
	if (npg < need) {
		if (txn->npgslot < need) {
			unsigned npgslot = txn->ndb*5;
			npgslot = ED_ALIGN_SIZE(need, npgslot);
			EdPg **pg = realloc(txn->pg, npgslot*sizeof(pg[0]));
			if (pg == NULL) { return ED_ERRNO; }
			txn->pg = pg;
			txn->npgslot = npgslot;
		}
		int rc = ed_alloc(&txn->xtype->alloc, txn->pg+npg, txn->npgslot-npg, true);
		if (rc < 0) { return rc; }
		txn->npg = txn->npgslot;
	}

	if (txn->nodes == NULL || txn->nodes->nnodesused == txn->nodes->nnodes) {
		int rc = node_alloc(&txn->nodes, txn->nodes ? txn->nodes->nnodes + 1 : npg);
		if (rc < 0) { return rc; }
	}

	EdNode *node = node_wrap(txn, txn->pg[txn->npgused++], par, pidx);
	node->tree->xid = txn->xid;
	*out = node;
	return 0;
}

int
ed_txn_clone(EdTxn *txn, EdNode *node, EdNode **out)
{
	EdNode *copy;
	int rc = ed_txn_alloc(txn, node->parent, node->pindex, &copy);
	if (rc >= 0) {
		copy->page->type = node->page->type;
		copy->tree->next = node->tree->next;
		copy->tree->nkeys = node->tree->nkeys;
		copy->gc = node;
		*out = copy;
	}
	return rc;
}

EdTxnDb *
ed_txn_db(EdTxn *txn, unsigned db, bool reset)
{
	assert(db < txn->ndb);
	EdTxnDb *dbp = &txn->db[db];
	if (reset) {
		dbp->tail = dbp->head;
		dbp->entry = NULL;
		dbp->entry_index = 0;
		dbp->nsplits = 0;
		dbp->match = 0;
		dbp->nmatches = 0;
	}
	return dbp;
}

