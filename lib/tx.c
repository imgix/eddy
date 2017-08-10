#include "eddy-private.h"

#define ED_TX_CLOSED 0
#define ED_TX_OPEN 1

static EdPgNode *
wrap_node(EdTxn *tx, EdPg *pg, EdPgNode *par, uint16_t pidx, uint8_t dirty)
{
	assert(tx->nnodesused < tx->nnodes);

	EdPgNode *n = &tx->nodes[tx->nnodesused++];
	n->page = pg;
	n->parent = par;
	n->pindex = pidx;
	n->dirty = dirty;
	return n;
}

int
ed_txn_new(EdTxn **txp, EdPgAlloc *alloc, EdLock *lock, EdTxnType *type, unsigned ntype)
{
	unsigned nnodes = ntype * 16;
	int rc = 0;
	EdTxn *tx;
	size_t sztx = sizeof(*tx) + (ntype-1)*sizeof(tx->db[0]);
	size_t sznodes = sizeof(tx->nodes[0]) * nnodes;

	size_t szscratch = 0;
	for (unsigned i = 0; i < ntype; i++) { szscratch += ED_ALIGN(type[i].entry_size); }

	size_t offnodes = ED_ALIGN(sztx);
	size_t offscratch = ED_ALIGN(offnodes + sznodes);

	if ((tx = calloc(1, offscratch + szscratch)) == NULL) {
		rc = ED_ERRNO;
		goto error;
	}

	tx->lock = lock;
	tx->alloc = alloc;
	tx->nodes = (EdPgNode *)((uint8_t *)tx + offnodes);
	tx->nnodes = nnodes;

	uint8_t *scratch = (uint8_t *)tx + offscratch;
	for (unsigned i = 0; i < ntype; i++) {
		EdPgno *no = type[i].no;
		if (no == NULL) { rc = ed_esys(EINVAL); break; }
		if (*no != ED_PAGE_NONE) {
			rc = ed_txn_map(tx, *no, NULL, 0, &tx->db[i].head);
			if (rc < 0) { break; }
			tx->db[i].tail = tx->db[i].head;
		}
		tx->db[i].root = no;
		tx->db[i].scratch = scratch;
		tx->db[i].entry_size = type[i].entry_size;
		tx->ndb++;
		scratch += ED_ALIGN(type[i].entry_size);
	}

	if (rc < 0) { goto error; }
	*txp = tx;
	return 0;

error:
	ed_txn_close(&tx, ED_FNOSYNC);
	return rc;
}

int
ed_txn_open(EdTxn *tx, bool rdonly, uint64_t flags)
{
	if (tx == NULL || tx->isopen) { return ed_esys(EINVAL); }
	EdLockType lock = rdonly ? ED_LOCK_SH : ED_LOCK_EX;
	int rc = ed_lock(tx->lock, tx->alloc->fd, lock, true, flags);
	if (rc < 0) { return rc; }
	tx->isopen = true;
	tx->rdonly = rdonly;
	return 0;
}

int
ed_txn_commit(EdTxn **txp, uint64_t flags)
{
	EdTxn *tx = *txp;
	if (tx == NULL || !tx->isopen || tx->rdonly) { return ed_esys(EINVAL); }

	int rc = 0;
	unsigned npg = 0;
	for (unsigned i = 0; i < tx->ndb; i++) {
		if (tx->db[i].apply == ED_BT_INSERT) { npg += tx->db[i].nsplits; }
	}
	if (npg > tx->nnodes) {
		rc = ed_esys(ENOBUFS); // FIXME: proper error code
		goto done;
	}
	if (npg > 0) {
		if ((tx->pg = calloc(npg, sizeof(tx->pg[0]))) == NULL) {
			rc = ED_ERRNO;
			goto done;
		}
		rc = ed_pg_alloc(tx->alloc, tx->pg, npg, true);
		if (rc < 0) { goto done; }
		tx->npg = npg;
		rc = 0;
	}

	for (unsigned i = 0; i < tx->ndb; i++) {
		ed_bt_apply(tx, i, tx->db[i].scratch, tx->db[i].apply);
	}

done:
	ed_txn_close(txp, flags);
	return rc;
}

void
ed_txn_close(EdTxn **txp, uint64_t flags)
{
	EdTxn *tx = *txp;
	if (tx == NULL) { return; }

	if (tx->isopen) {
		ed_lock(tx->lock, tx->alloc->fd, ED_LOCK_UN, true, flags);
	}

	EdPg *heads[tx->ndb];
	if (flags & ED_FRESET) {
		for (unsigned i = 0; i < tx->ndb; i++) {
			EdTxnSearch *srch = &tx->db[i];
			if (srch->head) {
				heads[i] = srch->head->page;
				srch->head->page = NULL;
			}
			else {
				heads[i] = NULL;
			}
		}
	}

	for (int i = (int)tx->nnodesused-1; i >= 0; i--) {
		ed_pg_sync(tx->nodes[i].page, 1, flags, tx->nodes[i].dirty);
		if (tx->nodes[i].page) {
			ed_pg_unmap(tx->nodes[i].page, 1);
		}
	}
	ed_pg_free(tx->alloc, tx->pg+tx->npgused, tx->npg-tx->npgused);
	free(tx->pg);

	if (flags & ED_FRESET) {
		tx->pg = NULL;
		tx->npg = 0;
		tx->npgused = 0;
		tx->nnodesused = 0;
		tx->isopen = false;
		for (unsigned i = 0; i < tx->ndb; i++) {
			EdTxnSearch *srch = &tx->db[i];
			srch->tail = srch->head = heads[i] ?
				wrap_node(tx, heads[i], NULL, 0, 0) : NULL;
			srch->key = 0;
			srch->entry = NULL;
			srch->entry_index = 0;
			srch->nsplits = 0;
			srch->match = 0;
			srch->nmatches = 0;
			srch->apply = ED_BT_NONE;
		}
	}
	else {
		free(tx);
		*txp = NULL;
	}
}

int
ed_txn_map(EdTxn *tx, EdPgno no, EdPgNode *par, uint16_t pidx, EdPgNode **out)
{
	for (int i = (int)tx->nnodesused-1; i >= 0; i--) {
		if (tx->nodes[i].page->no == no) {
			*out = &tx->nodes[i];
			return 0;
		}
	}

	if (tx->nnodesused == tx->nnodes) {
		return ed_esys(ENOBUFS); // FIXME: proper error code
	}

	EdPg *pg = ed_pg_map(tx->alloc->fd, no, 1);
	if (pg == MAP_FAILED) { return ED_ERRNO; }
	*out = wrap_node(tx, pg, par, pidx, 0);
	return 0;
}

EdPgNode *
ed_txn_alloc(EdTxn *tx, EdPgNode *par, uint16_t pidx)
{
	if (tx->npg == tx->npgused || tx->nnodesused == tx->nnodes) {
		fprintf(stderr, "*** too few pages allocated for transaction (%u)\n", tx->npg);
#if ED_BACKTRACE
		ed_backtrace_print(NULL, 0, stderr);
#endif
		abort();
	}
	return wrap_node(tx, tx->pg[tx->npgused++], par, pidx, 1);
}

EdTxnSearch *
ed_txn_search(EdTxn *tx, unsigned db, bool reset)
{
	assert(db < tx->ndb);
	EdTxnSearch *srch = &tx->db[db];
	if (reset) {
		srch->tail = srch->head;
		srch->entry = NULL;
		srch->entry_index = 0;
		srch->nsplits = 0;
		srch->match = 0;
		srch->nmatches = 0;
		srch->apply = ED_BT_NONE;
	}
	return srch;
}

