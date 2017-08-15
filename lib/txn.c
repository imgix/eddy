#include "eddy-private.h"

#define ED_TXN_FCRIT (ED_FNOTLCK|ED_FNOFLCK)

#define ed_txn_fclose(f, crit) ((f) & (~(ED_TXN_FCRIT|ED_FNOBLOCK)) | (crit))

/**
 * @brief   Wraps a mapped page into a node.
 *
 * Nodes are a memory representation used to simplify tracking parent
 * relationships as well as the dirty state of the page's contents.
 */
static EdPgNode *
wrap_node(EdTxn *txn, EdPg *pg, EdPgNode *par, uint16_t pidx, uint8_t dirty)
{
	assert(txn->nnodesused < txn->nnodes);

	EdPgNode *n = &txn->nodes[txn->nnodesused++];
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

	unsigned nnodes = ntype * 16;
	int rc = 0;
	EdTxn *txn;
	size_t sztx = sizeof(*txn) + (ntype-1)*sizeof(txn->db[0]);
	size_t sznodes = sizeof(txn->nodes[0]) * nnodes;

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
	txn->nodes = (EdPgNode *)((uint8_t *)txn + offnodes);
	txn->nnodes = nnodes;

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
	if (npg > txn->nnodes) {
		rc = ed_esys(ENOBUFS); // FIXME: proper error code
		goto done;
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

	for (int i = (int)txn->nnodesused-1; i >= 0; i--) {
		ed_pg_sync(txn->nodes[i].page, 1, flags, txn->nodes[i].dirty);
		if (txn->nodes[i].page) {
			ed_pg_unmap(txn->nodes[i].page, 1);
		}
	}
	ed_pg_free(txn->alloc, txn->pg+txn->npgused, txn->npg-txn->npgused);
	free(txn->pg);

	ed_pg_alloc_sync(txn->alloc);

	if (flags & ED_FRESET) {
		txn->pg = NULL;
		txn->npg = 0;
		txn->npgused = 0;
		txn->nnodesused = 0;
		txn->isopen = false;
		for (unsigned i = 0; i < txn->ndb; i++) {
			EdTxnDb *dbp = &txn->db[i];
			dbp->tail = dbp->head = heads[i] ?
				wrap_node(txn, heads[i], NULL, 0, 0) : NULL;
			dbp->key = 0;
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
	for (int i = (int)txn->nnodesused-1; i >= 0; i--) {
		if (txn->nodes[i].page->no == no) {
			*out = &txn->nodes[i];
			return 0;
		}
	}

	if (txn->nnodesused == txn->nnodes) {
		return ed_esys(ENOBUFS); // FIXME: proper error code
	}

	EdPg *pg = ed_pg_map(txn->alloc->fd, no, 1);
	if (pg == MAP_FAILED) { return ED_ERRNO; }
	*out = wrap_node(txn, pg, par, pidx, 0);
	return 0;
}

EdPgNode *
ed_txn_alloc(EdTxn *txn, EdPgNode *par, uint16_t pidx)
{
	if (txn->npg == txn->npgused || txn->nnodesused == txn->nnodes) {
		fprintf(stderr, "*** too few pages allocated for transaction (%u)\n", txn->npg);
#if ED_BACKTRACE
		ed_backtrace_print(NULL, 0, stderr);
#endif
		abort();
	}
	return wrap_node(txn, txn->pg[txn->npgused++], par, pidx, 1);
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

