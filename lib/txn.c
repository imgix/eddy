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
static EdPgNode *
node_wrap(EdTxn *txn, EdPg *pg, EdPgNode *par, uint16_t pidx, uint8_t dirty)
{
	assert(txn->nodes->nnodesused < txn->nodes->nnodes);

	EdPgNode *n = &txn->nodes->nodes[txn->nodes->nnodesused++];
	n->page = pg;
	n->parent = par;
	n->pindex = pidx;
	n->dirty = dirty;
	return n;
}

int
ed_txn_new(EdTxn **txnp, EdPgAlloc *alloc, EdLck *lck, EdTxnType *type, unsigned ntype)
{
	if (ntype == 0 || ntype > ED_TXN_MAX_TYPE) {
		return ed_esys(EINVAL);
	}

	unsigned nnodes = ntype * 8;
	int rc = 0;
	EdTxn *txn;
	size_t sztx = sizeof(*txn) + (ntype-1)*sizeof(txn->db[0]);
	size_t sznodes = node_size(nnodes);

	size_t szscratch = 0;
	for (unsigned i = 0; i < ntype; i++) { szscratch += ed_align_max(type[i].entry_size); }

	size_t offnodes = ed_align_max(sztx);
	size_t offscratch = ed_align_max(offnodes + sznodes);

	if ((txn = calloc(1, offscratch + szscratch)) == NULL) {
		rc = ED_ERRNO;
		goto error;
	}

	txn->lck = lck;
	txn->alloc = alloc;
	txn->nodes = (EdTxnNode *)((uint8_t *)txn + offnodes);
	txn->nodes->nnodes = nnodes;

	uint8_t *scratch = (uint8_t *)txn + offscratch;
	for (unsigned i = 0; i < ntype; i++) {
		EdPgno *no = type[i].no;
		if (no == NULL) { rc = ed_esys(EINVAL); break; }
		if (*no != ED_PG_NONE) {
			rc = ed_txn_map(txn, *no, NULL, 0, &txn->db[i].head);
			if (rc < 0) { break; }
			assert(txn->db[i].head != NULL && txn->db[i].head->page != NULL &&
				(txn->db[i].head->page->type == ED_PG_BRANCH || txn->db[i].head->page->type == ED_PG_LEAF));
			txn->db[i].tail = txn->db[i].head;
		}
		txn->db[i].root = no;
		txn->db[i].scratch = scratch;
		txn->db[i].entry_size = type[i].entry_size;
		txn->ndb++;
		scratch += ed_align_max(type[i].entry_size);
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
	EdLckType lck = rdonly ? ED_LCK_SH : ED_LCK_EX;
	int rc = ed_lck(txn->lck, txn->alloc->fd, lck, flags);
	if (rc < 0) { return rc; }
	txn->cflags = flags & ED_TXN_FCRIT;
	txn->isrdonly = rdonly;
	txn->isopen = true;
	return 0;
}

int
ed_txn_commit(EdTxn **txnp, uint64_t flags)
{
	EdTxn *txn = *txnp;
	if (txn == NULL || !txn->isopen || txn->isrdonly) { return ed_esys(EINVAL); }

	int rc = 0;
	unsigned npg = 0;
	for (unsigned i = 0; i < txn->ndb; i++) {
		if (txn->db[i].apply == ED_BPT_INSERT) { npg += txn->db[i].nsplits; }
	}
	unsigned avail = txn->nodes->nnodes - txn->nodes->nnodesused;
	if (npg > avail) {
		rc = node_alloc(&txn->nodes, txn->nodes->nnodesused + npg);
		if (rc < 0) { goto done; }
	}
	if (npg > 0) {
		if ((txn->pg = calloc(npg, sizeof(txn->pg[0]))) == NULL) {
			rc = ED_ERRNO;
			goto done;
		}
		rc = ed_pg_alloc(txn->alloc, txn->pg, npg, true);
		if (rc < 0) { goto done; }
		txn->npg = npg;
		rc = 0;
	}

	for (unsigned i = 0; i < txn->ndb; i++) {
		ed_bpt_apply(txn, i, txn->db[i].scratch, txn->db[i].apply);
	}

done:
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
		ed_lck(txn->lck, txn->alloc->fd, ED_LCK_UN, flags);
	}

	EdPg *heads[ED_TXN_MAX_TYPE];
	if (flags & ED_FRESET) {
		for (unsigned i = 0; i < txn->ndb; i++) {
			EdTxnDb *dbp = &txn->db[i];
			if (dbp->head) {
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
			ed_pg_sync(node->nodes[i].page, 1, flags, node->nodes[i].dirty);
			if (node->nodes[i].page) {
				ed_pg_unmap(node->nodes[i].page, 1);
			}
		}
		EdTxnNode *next = node->next;
		if (next == NULL) {
			txn->nodes = node;
			break;
		}
		free(node);
		node = next;
	} while(1);

	ed_pg_free(txn->alloc, txn->pg+txn->npgused, txn->npg-txn->npgused);
	free(txn->pg);

	ed_pg_alloc_sync(txn->alloc);

	if (flags & ED_FRESET) {
		txn->pg = NULL;
		txn->npg = 0;
		txn->npgused = 0;
		txn->nodes->nnodesused = 0;
		txn->isopen = false;
		for (unsigned i = 0; i < txn->ndb; i++) {
			EdTxnDb *dbp = &txn->db[i];
			dbp->tail = dbp->head = heads[i] ?
				node_wrap(txn, heads[i], NULL, 0, 0) : NULL;
			dbp->key = 0;
			dbp->start = NULL;
			dbp->entry = NULL;
			dbp->entry_index = 0;
			dbp->nsplits = 0;
			dbp->match = 0;
			dbp->nmatches = 0;
			dbp->apply = ED_BPT_NONE;
		}
	}
	else {
		free(txn);
		*txnp = NULL;
	}
}

int
ed_txn_map(EdTxn *txn, EdPgno no, EdPgNode *par, uint16_t pidx, EdPgNode **out)
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

	EdPg *pg = ed_pg_map(txn->alloc->fd, no, 1);
	if (pg == MAP_FAILED) { return ED_ERRNO; }
	*out = node_wrap(txn, pg, par, pidx, 0);
	return 0;
}

EdPgNode *
ed_txn_alloc(EdTxn *txn, EdPgNode *par, uint16_t pidx)
{
	if (txn->npg == txn->npgused || txn->nodes->nnodesused == txn->nodes->nnodes) {
		fprintf(stderr, "*** too few pages allocated for transaction (%u)\n", txn->npg);
#if ED_BACKTRACE
		ed_backtrace_print(NULL, 0, stderr);
#endif
		abort();
	}
	return node_wrap(txn, txn->pg[txn->npgused++], par, pidx, 1);
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
		dbp->apply = ED_BPT_NONE;
	}
	return dbp;
}

