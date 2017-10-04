#ifndef INCLUDED_EDDY_PRIVATE_H
#define INCLUDED_EDDY_PRIVATE_H

#include "eddy.h"

#include <stddef.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <inttypes.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <pthread.h>
#include <assert.h>

#if defined(__linux__)
# include <linux/fs.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
# include <sys/disk.h>
#else
# error Platform not supported
#endif

#ifndef ED_ALLOC_COUNT
# define ED_ALLOC_COUNT 16
#endif

#ifndef ED_MAX_ALIGN
# ifdef __BIGGEST_ALIGNMENT__
#  define ED_MAX_ALIGN __BIGGEST_ALIGNMENT__
# else
#  define ED_MAX_ALIGN 16
# endif
#endif

#define ED_LOCAL __attribute__((visibility ("hidden")))
#define ED_INLINE static inline __attribute__((always_inline))

#define ED_DB_KEYS 0
#define ED_DB_BLOCKS 1
#define ED_NDB 2

#define ED_STR2(v) #v
#define ED_STR(v) ED_STR2(v)



/** @brief  Seconds from an internal epoch */
typedef uint32_t EdTime;

typedef struct EdLck EdLck;

typedef uint32_t EdPgno;
typedef uint64_t EdBlkno;

typedef struct EdPg EdPg;
typedef struct EdPgGc EdPgGc;
typedef struct EdPgGcList EdPgGcList;
typedef struct EdPgGcState EdPgGcState;
typedef struct EdPgIdx EdPgIdx;

typedef struct EdNode EdNode;
typedef struct EdBpt EdBpt;

typedef uint64_t EdTxnId;
typedef struct EdTxn EdTxn;
typedef struct EdTxnDb EdTxnDb;
typedef struct EdTxnNode EdTxnNode;

typedef struct EdIdx EdIdx;
typedef struct EdConn EdConn;

typedef struct EdStat EdStat;

typedef struct EdEntryBlock EdEntryBlock;
typedef struct EdEntryKey EdEntryKey;
typedef struct EdObjectHdr EdObjectHdr;

typedef volatile EdPgno EdPgnoV;
typedef volatile EdBlkno EdBlknoV;
typedef volatile EdTxnId EdTxnIdV;



/**
 * @defgroup  fault  Debugging Fault Mechanism
 *
 * When enabled, allows tests to schedule faults in various critical parts of
 * the transaction system.
 *
 * @{
 */
#define ED_FAULT_NOPRINT UINT16_C(1<<0)

#if ED_FAULT

#define ED_FAULT_MAP(XX) \
	XX(NONE) \
	XX(COMMIT_BEGIN) \
	XX(ACTIVE_CLEARED) \
	XX(UPDATE_TREE) \
	XX(CLOSE_BEGIN) \
	XX(PENDING_BEGIN) \
	XX(PENDING_FINISH) \

typedef enum {
#define XX(f) ED_FAULT_##f,
	ED_FAULT_MAP(XX)
#undef XX
} EdFault;

ED_LOCAL void
ed_fault__enable(EdFault f, uint32_t count, uint16_t flags, const char *file, int line);

ED_LOCAL void
ed_fault__trigger(EdFault f, const char *file, int line);

#define ed_fault_enable(f, count, flags) \
	ed_fault__enable(ED_FAULT_##f, UINT32_C(count), flags, __FILE__, __LINE__)
#define ed_fault_trigger(f) \
	ed_fault__trigger(ED_FAULT_##f, __FILE__, __LINE__)

#else

#define ed_fault_enable(f, count, flags)
#define ed_fault_trigger(f)

#endif
/** @} */



/**
 * @defgroup  lck  Lock Module
 *
 * A combined thread and file lock with shared and exclusive locking modes.
 *
 * @{
 */

typedef enum EdLckType {
	ED_LCK_SH = F_RDLCK,
	ED_LCK_EX = F_WRLCK,
	ED_LCK_UN = F_UNLCK,
} EdLckType;

/**
 * @brief  Thread and file lock information
 */
struct EdLck {
	off_t        start;            /**< Byte offset for the range of the lock */
	off_t        len;              /**< Byte length for the range of the lock */
	pthread_rwlock_t rw;           /**< Lock for thread-level concurrency */
};

/**
 * @brief  Initializes the lock in an unlocked state.
 *
 * The lock controls both thread-level, and file-level reader/writer locking.
 * A byte range is specified for file locking, allowing multiple independant
 * locks per file.
 *
 * @param  lck  Pointer to a lock value
 * @param  start  Starting byte off
 * @param  len  Number of bytes from #start
 */
ED_LOCAL void
ed_lck_init(EdLck *lck, off_t start, off_t len);

/**
 * @brief  Cleans up any resources for the lock.
 *
 * It is expected that the lock has been released by #ed_lck().
 *
 * @param  lck  Pointer to a lock value
 */
ED_LOCAL void
ed_lck_final(EdLck *lck);

/**
 * @brief  Controls the lock state.
 *
 * This is used to both aquires a lock (shared or exlusive) and release it.
 * It is undefined behavior to acquire a lock multiple times. The lock must
 * be released before #ed_lck_final().
 * 
 * Supported values for #type are:
 *   - #ED_LCK_SH
 *       - Acquires a read-only shared lock.
 *   - #ED_LCK_EX
 *       - Acquires a read-write exclusive lock.
 *   - #ED_LCK_UN
 *       - Unlocks either the shared or exclusive lock.
 * 
 * Supported flags are:
 *   - #ED_FNOTLCK
 *       - Disable thread locking.
 *   - #ED_FNOBLOCK
 *       - Return EAGAIN if locking would block.
 *
 * When unlocking, the #ED_FNOTLCK flag must be equivalent in use when locking.
 *
 * @param  lck  Pointer to a lock value
 * @param  fd  Open file descriptor to lock
 * @param  type  The lock action to take
 * @param  flags  Modify locking behavior
 */
ED_LOCAL int
ed_lck(EdLck *lck, int fd, EdLckType type, uint64_t flags);

/**
 * @brief  Low-level file lock
 *
 * This is used to both aquires a lock (shared or exlusive) and release it.
 * It is undefined behavior to acquire a lock multiple times. The lock must
 * be released before #ed_lck_final().
 * 
 * Supported values for #type are:
 *   - #ED_LCK_SH
 *       - Acquires a read-only shared lock.
 *   - #ED_LCK_EX
 *       - Acquires a read-write exclusive lock.
 *   - #ED_LCK_UN
 *       - Unlocks either the shared or exclusive lock.
 * 
 * Supported flags are:
 *   - #ED_FNOBLOCK
 *       - Return EAGAIN if locking would block.
 *
 * @param  lck  Pointer to a lock value
 * @param  fd  Open file descriptor to lock
 * @param  type  The lock action to take
 * @param  start  Starting offset for the locked region of the file
 * @param  len  Number of bytes for locked region of the file
 * @param  flags  Modify locking behavior
 */
ED_LOCAL int
ed_flck(int fd, EdLckType type, off_t start, off_t len, uint64_t flags);

/** @} */



/**
 * @defgroup  pg  Page Module
 *
 * Page utility functions, file-backed page allocator, and garbage collector.
 *
 * @{
 */

#define ED_PG_INDEX     UINT32_C(0x58444e49)
#define ED_PG_BRANCH    UINT32_C(0x48435242)
#define ED_PG_LEAF      UINT32_C(0x4641454c)
#define ED_PG_GC        UINT32_C(0x4c4c4347)

#define ED_PG_NONE UINT32_MAX
#define ED_PG_MAX (UINT32_MAX-1)
#define ED_BLK_NONE UINT64_MAX

ED_LOCAL   void * ed_pg_map(int fd, EdPgno no, EdPgno count, bool need);
ED_LOCAL      int ed_pg_unmap(void *p, EdPgno count);
ED_LOCAL   void * ed_pg_load(int fd, EdPg **pgp, EdPgno no, bool need);
ED_LOCAL     void ed_pg_unload(EdPg **pgp);
ED_LOCAL      int ed_pg_mark_gc(EdIdx *idx, EdStat *stat);

/**
 * @brief  Allocates a page from the underlying file
 *
 * This call is guaranteed atomic with regards to changes within the index.
 * That is, all requested pages will be allocated or the index will remain
 * in its current state. The only possible side-effect from an erroring
 * allocation is an unclaimed expansion of the underlying file. The index will
 * remain unchanged however. Given the idempotency of growing the file, this
 * space will simply be used for a later allocation.
 *
 * Exclusive access to allocator is assumed for this call.
 *
 * @param  idx  Index object
 * @param  p  Array to store allocated pages into
 * @param  n  Number of pages to allocate
 * @param  need  Hint that the pages will be needed soon
 * @return  >0 the number of pages allocated from the tail or free list,
 *          <0 error code
 */
ED_LOCAL int
ed_alloc(EdIdx *idx, EdPg **, EdPgno n, bool need);

/**
 * @brief  Allocates and resets a page from the underlying file.
 * @see  ed_alloc()
 * @param  idx  Index object
 * @param  p  Array to store allocated pages into
 * @param  n  Number of pages to allocate
 * @return  >0 the number of pages allocated from the tail or free list,
 *          <0 error code
 */
ED_LOCAL int
ed_calloc(EdIdx *idx, EdPg **, EdPgno n);

/**
 * @brief  Frees disused page objects for a transaction
 *
 * This call transfers all pages to the free list atomically. That is, all
 * pages are recorded as free, or none are. Multiple calls do not have this
 * guarantees even within a single lock acquisition. Ownership of all pages
 * is transfered to the free list, and any external pointers should be
 * considered invalid after a successful call.
 *
 * The transaction id will gate the reuse of these pages until it is safe to
 * do so. Using `0` for the transaction id implies the pages may be reclaimed
 * immediately. This will not reclaim the disk space used, however, it will
 * become available for later allocations.
 *
 * Exclusive access to allocator is assumed for this call.
 *
 * @param  idx  Index object
 * @param  xid  The transaction ID that is discarding these pages
 * @param  p  Array of pages objects to deallocate
 * @param  n  Number of pages to deallocate
 */
ED_LOCAL int
ed_free(EdIdx *idx, EdTxnId xid, EdPg **p, EdPgno n);

/**
 * @brief  Frees disused page numbers
 * @see ed_free
 * @param  idx  Index object
 * @param  xid  The transaction ID that is discarding these pages
 * @param  p  Array of pages numbers to deallocate
 * @param  n  Number of pages to deallocate
 */
ED_LOCAL int
ed_free_pgno(EdIdx *idx, EdTxnId xid, EdPgno *p, EdPgno n);

/** @} */



#if ED_MMAP_DEBUG
/**
 * @defgroup  pgtrack  Page Usage Tracking Module
 *
 * When enabled, tracks improper page uses including leaked pages, double
 * unmaps, and unmapping uninitialized addresses.
 *
 * @{
 */

ED_LOCAL     void ed_pg_track(EdPgno no, uint8_t *pg, EdPgno count);
ED_LOCAL     void ed_pg_untrack(uint8_t *pg, EdPgno count);
ED_LOCAL      int ed_pg_check(void);

/** @} */
#endif



#if ED_BACKTRACE
/**
 * @defgroup  backtrace  Backtrace Module
 *
 * When enabled, provides stack traces for error conditions with the page
 * tracker and transactions.
 *
 * @{
 */

/**
 * @brief  Opaque type for capturing backtrace information
 */
typedef struct EdBacktrace EdBacktrace;

ED_LOCAL      int ed_backtrace_new(EdBacktrace **);
ED_LOCAL     void ed_backtrace_free(EdBacktrace **);
ED_LOCAL     void ed_backtrace_print(EdBacktrace *, int skip, FILE *);
ED_LOCAL      int ed_backtrace_index(EdBacktrace *, const char *name);

/** @} */
#endif



/**
 * @defgroup  bpt  B+Tree Module
 *
 * @{
 */

/**
 * @brief  In-memory node to wrap a b+tree node
 *
 * This captures the parent node when mapping each subsequent page, allowing
 * simpler reverse-traversal withough the complexity of storing the relationship.
 */
struct EdNode {
	union {
		EdPg *      page;             /**< Mapped page */
		EdBpt *     tree;             /**< Mapped page as a tree */
	};
	EdNode *        parent;           /**< Parent node */
	uint16_t        pindex;           /**< Index of page in the parent */
	bool            gc;               /**< Is this node marked to discard */
};

typedef int (*EdBptPrint)(const void *, char *buf, size_t len);

ED_LOCAL   size_t ed_branch_order(void);
ED_LOCAL   size_t ed_leaf_order(size_t esize);
ED_LOCAL   size_t ed_bpt_capacity(size_t esize, size_t depth);
ED_LOCAL      int ed_bpt_find(EdTxn *txn, unsigned db, uint64_t key, void **ent);
ED_LOCAL      int ed_bpt_first(EdTxn *txn, unsigned db, void **ent);
ED_LOCAL      int ed_bpt_last(EdTxn *txn, unsigned db, void **ent);
ED_LOCAL      int ed_bpt_next(EdTxn *txn, unsigned db, void **ent);
ED_LOCAL      int ed_bpt_prev(EdTxn *txn, unsigned db, void **ent);
ED_LOCAL      int ed_bpt_loop(const EdTxn *txn, unsigned db);
ED_LOCAL      int ed_bpt_set(EdTxn *txn, unsigned db, const void *ent, bool replace);
ED_LOCAL      int ed_bpt_del(EdTxn *txn, unsigned db);
ED_LOCAL      int ed_bpt_mark(EdIdx *, EdStat *, EdBpt *);
ED_LOCAL     void ed_bpt_print(EdBpt *, int fd, size_t esize, FILE *, EdBptPrint);
ED_LOCAL      int ed_bpt_verify(EdBpt *, int fd, size_t esize, FILE *);

/** @} */



/**
 * @brief  txn  Transaction Module
 *
 * This implements the transaction system for working with multiple database
 * b+trees. These aren't "real" transactions, in that only a single change
 * operation is supported per database. However, single changes made to
 * multiple databases are handled as a single change. This is all that's
 * needed for the index, so the simplicity is preferrable for the moment.
 * The design of the API is intended to accomodate full transactions if that
 * does become necessary.
 *
 * @{
 */

/**
 * @brief  Verifies the transaction is usable
 * @param  txn  Transaction object
 */
#define ED_TXN_CHECK(txn) do { \
	if ((txn) == NULL) { return ed_esys(EINVAL); } \
	ED_IDX_CHECK((txn)->idx); \
} while (0)

/**
 * @brief  Verifies the transaction closed
 * @param  txn  Transaction object
 */
#define ED_TXN_CHECK_CLOSED(txn) do { \
	ED_TXN_CHECK(txn); \
	if ((txn)->state != ED_TXN_CLOSED) { return ed_esys(EINVAL); } \
} while (0)

/**
 * @brief  Verifies the transaction is open for reading
 * @param  txn  Transaction object
 */
#define ED_TXN_CHECK_RD(txn) do { \
	ED_TXN_CHECK(txn); \
	if ((txn)->state == ED_TXN_CLOSED) { return ED_EINDEX_TXN_CLOSED; } \
} while (0)

/**
 * @brief  Verifies the transaction is open for writing
 * @param  txn  Transaction object
 */
#define ED_TXN_CHECK_WR(txn) do { \
	ED_TXN_CHECK_RD(txn); \
	if ((txn)->isrdonly) { return ED_EINDEX_RDONLY; } \
} while (0)

/**
 * @brief  State of the the transaction object
 */
enum EdTxnState {
	ED_TXN_CLOSED,                 /**< Transaction is not ready for use. Call #ed_txn_open() before use. */
	ED_TXN_OPEN,                   /**< Transaction is ready to make modifications. */
	ED_TXN_COMMITTED,              /**< Transaction has been commited and is going to be closed. */
	ED_TXN_CANCELLED,              /**< Transaction is closing without having been committed. */
};

typedef enum EdTxnState EdTxnState;

/**
 * @brief  Transaction database reference
 *
 * This is the object allocated for each database involved in the transaction.
 * Additionally, it acts as a cursor for iterating through records in the tree.
 */
struct EdTxnDb {
	EdNode *     root;             /**< First node searched */
	EdNode *     find;             /**< Current find result */
	EdPgno *     no;               /**< Pointer to page number of root node */
	uint64_t     key;              /**< Key searched for */
	uint64_t     kmin;             /**< Minimum key value that may be inserted at the current position */
	uint64_t     kmax;             /**< Maximum key value that may be inserted at the current position */
	void *       start;            /**< Pointer to the first entry */
	void *       entry;            /**< Pointer to the entry in the leaf */
	size_t       entry_size;       /**< Size in bytes of the entry */
	uint32_t     entry_index;      /**< Index of the entry in the leaf */
	int          nsplits;          /**< Number of nodes requiring splits for an insert */
	int          match;            /**< Return code of the search */
	int          nmatches;         /**< Number of matched keys so far */
	int          nloops;           /**< Number of full iterations */
	bool         haskey;           /**< Mark if the cursor started with a find key */
	bool         hasfind;          /**< Mark if the cursor has moved into position */
	bool         hasentry;         /**< Mark if the current entry has been yielded */
};

/**
 * @brief  Transaction object
 *
 * This holds all information for an active or reset transaction. Once
 * allocated, the transaction cannot be changed. However, it may be reset and
 * used for multiple transactions agains the same database set.
 */
struct EdTxn {
	EdIdx *      idx;              /**< Index for the transaction */
	EdPg **      pg;               /**< Array to hold allocated pages */
	unsigned     npg;              /**< Number of pages allocated */
	unsigned     npgused;          /**< Number of pages used */
	unsigned     npgslot;          /**< Number of page slots in the #pg array */
	EdPgno *     gc;               /**< Array to hold discarded page numbers */
	unsigned     ngcused;          /**< Number of pages discarded */
	unsigned     ngcslot;          /**< Number of page slots in the #gc array */
	EdTxnNode *  nodes;            /**< Linked list of node arrays */
	EdTxnId      xid;              /**< Transaction ID or 0 for read-only */
	EdBlkno      pos;              /**< Current slab write block */
	uint64_t     cflags;           /**< Critical flags required during #ed_txn_commit() or #ed_txn_close() */
	EdTxnState   state;            /**< Current transaction state */
	int          error;            /**< Error code during transaction */
	bool         isrdonly;         /**< Was #ed_txn_open() called with #ED_FRDONLY */
	EdBpt *      roots[ED_NDB];    /**< Cached root pages */
	EdTxnDb      db[ED_NDB];       /**< State information for each b+tree */
};

/**
 * @brief  Contiguous allocation of node wrappers
 *
 * Each allocation is created with a specific number of nodes. When exhausted,
 * a new, larger allocation is created and pushed as the head of a linked-list.
 * In contrast with realloc-style growth, this keeps prior node pointers valid.
 */
struct EdTxnNode {
	EdTxnNode *  next;             /**< Next chunk of nodes */
	unsigned     nslot;            /**< Length of node array */
	unsigned     nused;            /**< Number of nodes used */
	EdNode       nodes[1];         /**< Flexible array of node wrapped pages */
};

/**
 * @brief  Allocates a new transaction for working with a specific set of databases.
 *
 * The object is deallocated when calling #ed_txn_commit() or #ed_txn_close().
 * However, the transaction may be reused by passing #ED_FRESET to either of
 * these functions.
 *
 * @param  txnp  Indirect pointer to a assign the allocation to
 * @return  0 on success <0 on error
 */
ED_LOCAL int
ed_txn_new(EdTxn **txnp, EdIdx *idx);

/**
 * @brief  Starts an allocated transaction
 *
 * This must be called before reading or writing to any database objects.
 * 
 * Supported flags are:
 *   - #ED_FRDONLY
 *       - The operation will not write any changes.
 *   - #ED_FNOTLCK
 *       - Disable thread locking.
 *   - #ED_FNOBLOCK
 *       - Return EAGAIN if the required lock would block.
 *
 * @param  txn  Closed transaction object
 * @param  flags  Behavior modification flags
 * @return  0 on success <0 on error
 */
ED_LOCAL int
ed_txn_open(EdTxn *txn, uint64_t flags);

/**
 * @brief  Commits the changes to each database
 * 
 * Supported flags are:
 *   - #ED_FNOSYNC
 *       - Disable file syncing.
 *   - #ED_FASYNC
 *       - Don't wait for pages to complete syncing.
 *   - #ED_FRESET
 *       - Reset the transaction for another use.
 *
 * @param  txnp  Indirect pointer an open transaction
 * @return  0 on success <0 on error
 */
ED_LOCAL int
ed_txn_commit(EdTxn **txnp, uint64_t flags);

/**
 * @brief  Closes the transaction and abandons any pending changes
 * 
 * Supported flags are:
 *   - #ED_FNOSYNC
 *       - Disable file syncing.
 *   - #ED_FASYNC
 *       - Don't wait for pages to complete syncing.
 *   - #ED_FRESET
 *       - Reset the transaction for another use.
 *
 * @param  txnp  Indirect pointer an open transaction
 * @param  flags  Behavior modification flags
 * @return  0 on success <0 on error
 */
ED_LOCAL void
ed_txn_close(EdTxn **txnp, uint64_t flags);

/**
 * @brief  Gets the current slab position
 * @param  txn  Transaction object
 * @return  slab block number
 */
ED_LOCAL EdBlkno
ed_txn_block(const EdTxn *txn);

/**
 * @brief  Sets the slab position to write on commit
 * @param  txn  Transaction object
 * @param  pos  New slab write position
 * @return  0 on success <0 on error
 */
ED_LOCAL int
ed_txn_set_block(EdTxn *txn, EdBlkno pos);

/**
 * @brief  Checks if a transaction is in read-only mode
 *
 * A transaction is read-only when opened with the #ED_FRDONLY flag set or once
 * error has occurred during a modifcation.
 *
 * @param  txn  Transaction object
 * @return  Read-only state
 */
ED_LOCAL bool
ed_txn_isrdonly(const EdTxn *txn);

/**
 * @brief  Checks if a transaction has been opened
 * @param  txn  Transaction object
 * @return  Open state
 */
ED_LOCAL bool
ed_txn_isopen(const EdTxn *txn);

/**
 * @brief  Maps a page wrapped into a node
 *
 * The page is unmapped and possibly synced when the transaction is complete.
 *
 * @param  txn  Transaction object
 * @param  no  Page number to map
 * @param  par  Parent node or `NULL`
 * @param  pidx  Index of the page in the parent
 * @param  out  Node pointer to assign to
 * @return  0 on success <0 on error
 */
ED_LOCAL int
ed_txn_map(EdTxn *txn, EdPgno no, EdNode *par, uint16_t pidx, EdNode **out);

/**
 * @brief  Retrieves the next allocated page wrapped into a node
 *
 * The number of pages needed is determined by the modifications made to the
 * databases. Requesting too many pages will result in an abort.
 *
 * @param  txn  Transaction object
 * @param  par  Parent node or `NULL`
 * @param  pidx  Index of the page in the parent
 * @param  out  Node pointer to assign to
 * @return  0 on success <0 on error
 */
ED_LOCAL int
ed_txn_alloc(EdTxn *txn, EdNode *par, uint16_t pidx, EdNode **out);

/**
 * @brief  Allocates a new node that clones the node header of a prior version
 *
 * This does not clone the main node data. If successful, #node will be marked
 * for discarding using #ed_txn_discard().
 *
 * @param  txn  Transaction object
 * @param  node  Node to clone
 * @param  out  Node pointer to assign to
 * @return  0 on success <0 on error
 */
ED_LOCAL int
ed_txn_clone(EdTxn *txn, EdNode *node, EdNode **out);

/**
 * @brief  Marks a page for removal
 *
 * This node will only be discarded fully when, and if, the transaction is
 * committed.
 *
 * @param  txn  Transaction object
 * @param  node  Node to discard
 * @return  0 on success <0 on error
 */
ED_LOCAL int
ed_txn_discard(EdTxn *txn, EdNode *node);

/**
 * @brief  Gets the #EdTxnDb object for the numbered database
 * @param  txn  Transaction object
 * @param  db  Database number, 0-based
 * @param  reset  Reset the search and modification state
 */
ED_LOCAL EdTxnDb *
ed_txn_db(EdTxn *txn, unsigned db, bool reset);

/** @} */



/**
 * @defgroup  idx  Index Module
 *
 * @{
 */

struct EdIdx {
	EdPgIdx *    hdr;              /**< Index header reference */
	char *       path;             /**< Path copied when opening */
	int          fd;               /**< Open file discriptor for the file to allocate from */
	int          slabfd;           /**< Open file descriptor for the slab */
	EdLck        lck;              /**< Write lock */
	EdPgGc *     gc_head;          /**< Currently mapped head of the garbage collected pages */
	EdPgGc *     gc_tail;          /**< Currently mapped tail of the garbage collected pages */
	uint64_t     flags;            /**< Open flags merged with the saved flags */
	EdConn *     conn;             /**< Current connection or NULL */
	int          nconns;           /**< Number of available connections */
	int          pid;              /**< Process ID that opened the index */
	uint64_t     seed;             /**< Randomized seed */
	EdTimeUnix   epoch;            /**< Epoch adjustment in seconds */
	EdBlkno      slab_block_count; /**< Number of blocks in the slab */
};

#define ED_IDX_PAGES(nconns) ed_count_pg(offsetof(EdPgIdx, conns) + sizeof(EdConn)*nconns)

#define ed_idx_active(idx) ((idx)->pid == getpid())
#define ed_idx_assert(idx) assert(ed_idx_active(idx))

#define ED_IDX_CHECK(idx) do { \
	if (!ed_idx_active(idx)) { return ED_EINDEX_FORK; } \
} while (0)


ED_LOCAL      int ed_idx_open(EdIdx *, const EdConfig *cfg);
ED_LOCAL     void ed_idx_close(EdIdx *);
ED_LOCAL  EdTxnId ed_idx_xmin(EdIdx *idx, EdTime now);
ED_LOCAL      int ed_idx_lock(EdIdx *, EdLckType type);
ED_LOCAL  EdTxnId ed_idx_acquire_xid(EdIdx *);
ED_LOCAL     void ed_idx_release_xid(EdIdx *);
ED_LOCAL      int ed_idx_acquire_snapshot(EdIdx *, EdBpt **trees);
ED_LOCAL     void ed_idx_release_snapshot(EdIdx *, EdBpt **trees);
ED_LOCAL      int ed_idx_repair_leaks(EdIdx *, EdStat *, uint64_t flags);

/** @} */


/**
 * @defgroup  stat  Index Status Module
 *
 * @{
 */

struct EdStat {
	struct stat  index;
	char *       index_path;
	EdTxnId      xid;
	EdPgno *     mult;
	size_t       nmultused;
	size_t       nmultslots;
	size_t       npending;
	size_t       nactive;
	size_t       ngc;
	size_t       nbpt;
	size_t *     mark;
	EdPgno       header;
	EdPgno       tail_start;       /**< Page number for the start of the tail pages */
	EdPgno       tail_count;       /**< Number of pages available at #tail_start */
	EdPgno       no;               /**< Number of pages in the index */
	uint8_t      vec[1];           /**< Bit vector of referenced pages */
};

ED_LOCAL      int ed_stat_new(EdStat **statp, EdIdx *idx, uint64_t flags);
ED_LOCAL     void ed_stat_free(EdStat **statp);
ED_LOCAL      int ed_stat_mark(EdStat *stat, EdPgno no);
ED_LOCAL     bool ed_stat_has_leaks(const EdStat *stat);
ED_LOCAL     bool ed_stat_has_leak(EdStat *stat, EdPgno no);
ED_LOCAL const EdPgno *
ed_stat_multi_ref(EdStat *stat, size_t *count);
ED_LOCAL     void ed_stat_print(EdStat *stat, FILE *out);

/** @} */


/**
 * @defgroup  rnd  Random Module
 *
 * @{
 */

ED_LOCAL      int ed_rnd_open(void);
ED_LOCAL  ssize_t ed_rnd_buf(int fd, void *buf, size_t len);
ED_LOCAL      int ed_rnd_u64(int fd, uint64_t *);

/** @} */



/**
 * @defgroup  time  Time Module
 *
 * @{
 */

#define ED_TIME_DELETE ((EdTime)0)
#define ED_TIME_MAX ((EdTime)UINT32_MAX-1)
#define ED_TIME_INF ((EdTime)UINT32_MAX)

/**
 * @brief  Converts a UNIX time to an internal epoch
 * @param  epoch  The internal epoch as a UNIX timestamp
 * @param  at  The UNIX time to convert
 * @return  internal time representation
 */
ED_LOCAL EdTime
ed_time_from_unix(EdTimeUnix epoch, EdTimeUnix at);

/**
 * @brief  Converts an internal time to UNIX time
 * @param  epoch  The internal epoch as a UNIX timestamp
 * @param  at  The internal time to convert
 * @return  UNIX time representation
 */
ED_LOCAL EdTimeUnix
ed_time_to_unix(EdTimeUnix epoch, EdTime at);

/**
 * @brief  Gets the current time in UNIX representation
 * @return  UNIX time representation
 */
ED_LOCAL EdTimeUnix
ed_now_unix(void);

/**
 * @brief  Gets the internal expiry as a time-to-live from a UNIX time
 * @param  epoch  The internal epoch as a UNIX timestamp
 * @param  ttl  Time-to-live in seconds or <0 for infinite
 * @param  at  The UNIX time to convert
 * @return  internal time representation
 */
ED_LOCAL EdTime
ed_expiry_at(EdTimeUnix epoch, EdTimeTTL ttl, EdTimeUnix at);

/**
 * @brief  Gets the time-to-live from the a UNIX time
 * @param  epoch  The internal epoch as a UNIX timestamp
 * @param  exp  The absolute expiration in internal time
 * @param  at  The UNIX time to convert
 * @return  Time-to-live in seconds
 */
ED_LOCAL EdTimeTTL
ed_ttl_at(EdTimeUnix epoch, EdTime exp, EdTimeUnix at);

ED_LOCAL EdTimeUnix
ed_unix_from_ttl_at(EdTimeTTL ttl, EdTimeUnix at);

ED_LOCAL EdTimeUnix
ed_unix_from_ttl(EdTimeTTL ttl);

/**
 * @brief  Tests if the internal time is expired against the a UNIX time
 * @param  epoch  The internal epoch as a UNIX timestamp
 * @param  exp  The absolute expiration in internal time
 * @param  at  The UNIX time to compare
 * @return  true if the value is expired
 */
ED_LOCAL bool
ed_expired_at(EdTimeUnix epoch, EdTime exp, EdTimeUnix at);

/** @} */



/**
 * @defgroup  util  Utility Functions
 *
 * @{
 */

#define ED_COUNT_SIZE(n, size) (((n) + ((size)-1)) / (size))
#define ED_ALIGN_SIZE(n, size) (ED_COUNT_SIZE(n, size) * (size))

#define ed_count_max(n) ED_COUNT_SIZE(n, ED_MAX_ALIGN)
#define ed_align_max(n) ED_ALIGN_SIZE(n, ED_MAX_ALIGN)

#define ed_count_pg(n) ED_COUNT_SIZE(n, PAGESIZE)
#define ed_align_pg(n) ED_ALIGN_SIZE(n, PAGESIZE)

#define ed_count_type(n, t) ED_COUNT_SIZE(n, ed_alignof(t))
#define ed_align_type(n, t) ED_ALIGN_SIZE(n, ed_alignof(t))

#define ed_fsave(f) ((uint32_t)((f) & UINT64_C(0x00000000FFFFFFFF)))
#define ed_fopen(f) ((f) & UINT64_C(0xFFFFFFFF00000000))

#define ed_len(arr) (sizeof(arr) / sizeof((arr)[0]))
#define ed_alignof(t) offsetof(struct { char c; t m; }, m)

#if BYTE_ORDER == LITTLE_ENDIAN
# define ed_b16(v) __builtin_bswap16(v)
# define ed_l16(v) (v)
# define ed_b32(v) __builtin_bswap32(v)
# define ed_l32(v) (v)
# define ed_b64(v) __builtin_bswap64(v)
# define ed_l64(v) (v)
#elif BYTE_ORDER == BIG_ENDIAN
# define ed_b16(v) (v)
# define ed_l16(v) __builtin_bswap16(v)
# define ed_b32(v) (v)
# define ed_l32(v) __builtin_bswap32(v)
# define ed_b64(v) (v)
# define ed_l64(v) __builtin_bswap64(v)
#endif

#define ed_ptr_b32(T, b, off) ((const T *)((const uint8_t *)(b) + ed_b32(off)))

#define ed_verbose(f, ...) do { \
	if ((f) & ED_FVERBOSE) { fprintf(stderr, __VA_ARGS__); fflush(stderr); } \
} while (0)

#define ED_IS_FILE(mode) (S_ISREG(mode))
#define ED_IS_DEVICE(mode) (S_ISCHR(mode) || S_ISBLK(mode))
#define ED_IS_MODE(mode) (ED_IS_FILE(mode) || ED_IS_DEVICE(mode))

/**
 * @brief  Change the size of the open file descriptor.
 *
 * This attempts to use the most efficient means for expanding a file for the
 * host platform.
 *
 * @param  fd  File descriptor open for writing
 * @param  size  The target size of the file
 * @return  0 on success <0 on failure
 */
ED_LOCAL int
ed_mkfile(int fd, off_t size);

/**
 * @brief  64-bit seeded hash function.
 *
 * @param  val  Bytes to hash
 * @param  len  Number of bytes to hash
 * @param  seed  Seed for the hash family
 * @return  64-bit hash value
 */
ED_LOCAL uint64_t
ed_hash(const uint8_t *val, size_t len, uint64_t seed);

/**
 * @brief  CRC-32c
 *
 * @param  crc  Checksum to resume from or 0
 * @param  bytes  Bytes to check
 * @param  len  Number of bytes to check
 * @return  32-bit checksum
 */
ED_LOCAL uint32_t
ed_crc32c(uint32_t crc, const void *bytes, size_t len);

ED_LOCAL ssize_t
ed_path_join(char *out, size_t len,
		const char *a, size_t alen,
		const char *b, size_t blen);

ED_LOCAL size_t
ed_path_clean(char *path, size_t len);

ED_LOCAL ssize_t
ed_path_abs(char *out, size_t len, const char *path, size_t plen);

static inline uint32_t __attribute__((unused))
ed_fetch32(const void *p)
{
	uint32_t val;
	memcpy(&val, p, sizeof(val));
	return val;
}

static inline uint64_t __attribute__((unused))
ed_fetch64(const void *p)
{
	uint64_t val;
	memcpy(&val, p, sizeof(val));
	return val;
}

static inline unsigned __attribute__((unused))
ed_power2(unsigned p)
{
	if (p > 0) {
		p--;
		p |= p >> 1;
		p |= p >> 2;
		p |= p >> 4;
		p |= p >> 8;
#if UINT_MAX > UINT16_MAX
		p |= p >> 16;
#if UINT_MAX > UINT32_MAX
		p |= p >> 32;
#endif
#endif
		p++;
	}
	return p;
}

/** @} */



#define ED_ENTRY_BLOCK_COUNT ((PAGESIZE - sizeof(EdBpt)) / sizeof(EdEntryBlock))
#define ED_ENTRY_KEY_COUNT ((PAGESIZE - sizeof(EdBpt)) / sizeof(EdEntryKey))

struct EdCache {
	EdIdx        idx;
	EdTxn *      txn;
	int          ref;
	size_t       bytes_used;
	size_t       blocks_used;
};

struct EdObject {
	EdCache *    cache;
	uint8_t *    data;
	uint8_t *    key;
	uint8_t *    meta;
	uint16_t     keylen;
	uint16_t     metalen;
	uint32_t     datalen;
	uint32_t     dataseek;
	uint32_t     datacrc;
	EdObjectHdr *hdr;
	EdTxnId      xid;
	EdBlkno      blck;
	EdBlkno      nblcks;
	size_t       byte;
	size_t       nbytes;
	EdTime       exp;
	bool         rdonly;
	uint8_t      newkey[1];
};

#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wpadded"

/**
 * @brief  Base type for all page object
 */
struct EdPg {
	EdPgno       no;               /**< Page offset from the start of the containing file */
	uint32_t     type;             /**< Type marker for the page */
};

struct EdPgGcState {
	uint16_t     head;             /**< Data offset for the first active list object */
	uint16_t     tail;             /**< Data offset for the last list object */
	uint16_t     nlists;           /**< Number of list objects in this page */
	uint16_t     nskip;            /**< Number of pages to skip from the start of the head list */
};

/**
 * @brief  Linked list of pages pending reclamation
 */
struct EdPgGc {
	EdPg         base;             /**< Page number and type */
	EdPgGcState  state;            /**< State information for the first active list object */
	EdPgno       next;             /**< Linked list of furthur gc pages */
	uint8_t      _pad[4];
#define ED_GC_DATA (PAGESIZE - sizeof(EdPg) - sizeof(EdPgGcState) - sizeof(EdPgno) - 4)
	uint8_t      data[ED_GC_DATA]; /**< Array for #EdPgGcList values */
};

/**
 * @brief  Flexible array of pages removed from a given transaction
 */
struct EdPgGcList {
	EdTxnId      xid;              /**< Transaction id that freed these pages */
	EdPgno       npages;           /**< Number of pages in the array, or UINT32_MAX for the next list */
	EdPgno       pages[1];         /**< Flexible array of the pages to free */
};

/** Maximum number of pages for a list in a new gc page */
#define ED_GC_LIST_MAX ((ED_GC_DATA - sizeof(EdPgGcList)) / sizeof(EdPgno) + 1)

#define ED_GC_LIST_PAGE_SIZE \
	(sizeof(((EdPgGcList *)0)->pages[0]))

/**
 * @brief  Calculates the list byte size requirement for a given number of pages
 * @param  npages  Number of pages
 * @return  Size in bytes required to store a list and pages
 */
#define ED_GC_LIST_SIZE(npages) \
	ed_align_type(offsetof(EdPgGcList, pages) + (npages)*ED_GC_LIST_PAGE_SIZE, EdPgGcList)

/**
 * @brief  Connection handle for each active process
 */
struct EdConn {
	volatile int     pid;          /**< Process ID */
	volatile EdTime  active;       /**< Optional time of last activity */
	EdTxnIdV     xid;              /**< Active read transaction id */
	EdPgno       npending;         /**< Number of pages in #pending */
	EdPgno       pending[11];      /**< Allocated pages pending reuse */
};


/**
 * @brief  Page type for the index file
 */
struct EdPgIdx {
	EdPg         base;             /**< Page number and type */
	char         magic[4];         /**< Magic identifier string, "edix" */
	char         endian;           /**< Endianness of the index, 'l' or 'B' */
	uint8_t      mark;             /**< Byte mark, 0xfc */
	uint16_t     version;          /**< Version number */
	uint64_t     seed;             /**< Randomized seed */
	EdTimeUnix   epoch;            /**< Epoch adjustment in seconds */
	uint64_t     flags;            /**< Permanent flags used when creating */
	uint32_t     size_page;        /**< Saved system page size in bytes */
	uint16_t     slab_block_size;  /**< Size of the blocks in the slab */
	uint16_t     nconns;           /**< Number of process connection slots */
	EdPgnoV      tail_start;       /**< Page number for the start of the tail pages */
	EdPgnoV      tail_count;       /**< Number of pages available at #tail_start */
	EdPgnoV      gc_head;          /**< Page pointer for the garbage collector head */
	EdPgnoV      gc_tail;          /**< Page pointer for the garbage collector tail */
	union {
		uint64_t vtree;            /**< Atomic CAS value for the trees */
		EdPgno   tree[4];          /**< Page pointer for the key and slab b+trees */
	};
	EdTxnIdV     xid;              /**< Global transaction ID */
	EdBlknoV     pos;              /**< Current slab write block */
	EdBlkno      slab_block_count; /**< Number of blocks in the slab */
	uint64_t     slab_ino;         /**< Inode number of the slab */
	char         slab_path[912];   /**< Path to the slab */
	EdPgnoV      nactive;          /**< Number of pages in #active */
	EdPgno       active[255];      /**< Allocated pages in the active transaction */
	EdConn       conns[1];         /**< Flexible array of active process connections */
};

#define ED_IDX_LCK_OPEN base
#define ED_IDX_LCK_WRITE xid

#define ED_IDX_LCK_OPEN_OFF offsetof(EdPgIdx, ED_IDX_LCK_OPEN)
#define ED_IDX_LCK_OPEN_LEN sizeof(((EdPgIdx *)0)->ED_IDX_LCK_OPEN)

#define ED_IDX_LCK_WRITE_OFF offsetof(EdPgIdx, ED_IDX_LCK_WRITE)
#define ED_IDX_LCK_WRITE_LEN sizeof(((EdPgIdx *)0)->ED_IDX_LCK_WRITE)

/**
 * @brief  On-disk value for an entry in the slab
 */
struct EdObjectHdr {
	uint8_t      version;          /**< Object-specific header version */
	uint8_t      flags;            /**< Per-object flags */
	uint16_t     tag;              /**< User-defined tag */
	EdTime       created;          /**< Timestamp when the object was created */
	EdTxnId      xid;              /**< Transaction ID that create this object */
	uint16_t     keylen;           /**< Number of bytes for the key */
	uint16_t     metalen;          /**< Number of bytes for the metadata */
	uint32_t     datalen;          /**< Number of bytes for the data */
	uint64_t     keyhash;          /**< Hash of the key */
	uint32_t     metacrc;          /**< Optional CRC-32c of the object meta data */
	uint32_t     datacrc;          /**< Optional CRC-32c of the object body data */
};

/**
 * @brief  Page type for b+tree branches and leaves
 *
 * For leaf nodes, the #nkeys field is the number of entries in the leaf.
 * Branches use this field for the number of keys. Howevever, there is one
 * more child page pointer than the number of keys.
 *
 * The #data size is the remaining space of a full page after subtracting the
 * size of the header fields. Currently, #data is guaranteed to be 8-byte
 * aligned.
 *
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
struct EdBpt {
	EdPg         base;             /**< Page number and type */
	EdTxnId      xid;              /**< Transaction ID that allocated this page */
	EdPgno       next;             /**< Overflow leaf pointer */
	uint16_t     nkeys;            /**< Number of keys in the node */
	uint8_t      _pad[2];
#define ED_BPT_DATA (PAGESIZE - sizeof(EdPg) - sizeof(EdTxnId) - sizeof(EdPgno) - 4)
	uint8_t      data[ED_BPT_DATA];/**< Tree-specific data for nodes (8-byte aligned) */
};

/**
 * @brief  B+Tree value type for indexing the slab by position
 */
struct EdEntryBlock {
	EdBlkno      no;               /**< Block number for the entry */
	EdPgno       count;            /**< Number of blocks used by the entry */
	uint32_t     _pad;
	EdTxnId      xid;              /**< Transaction ID that created the entry */
};

/**
 * @brief  B+Tree value type for indexing the slab by key
 */
struct EdEntryKey {
	uint64_t     hash;             /**< Hash of the key */
	EdBlkno      no;               /**< Block number for the entry */
	EdPgno       count;            /**< Number of blocks used by the entry */
	EdTime       exp;              /**< Expiration of the entry */
};

#pragma GCC diagnostic pop

#endif

