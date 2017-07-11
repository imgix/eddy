#define BRANCH_KEY_SIZE 8
#define BRANCH_PTR_SIZE (sizeof(EdPgno))
#define BRANCH_ENTRY_SIZE (BRANCH_PTR_SIZE + BRANCH_KEY_SIZE)
#define BRANCH_NEXT(pg) ((EdPgno *)((uint8_t *)(pg) + BRANCH_ENTRY_SIZE))

#define BRANCH_ORDER \
	(((sizeof(((EdBTree *)0)->data) - BRANCH_PTR_SIZE) / BRANCH_ENTRY_SIZE) + 1)

#define LEAF_ORDER(ent) \
	(sizeof(((EdBTree *)0)->data) / (ent))

#define IS_BRANCH(n) ((n)->base.type == ED_PGBRANCH)
#define IS_LEAF(n) ((n)->base.type == ED_PGLEAF)
#define IS_BRANCH_FULL(n) ((n)->nkeys == (BRANCH_ORDER-1))
#define IS_LEAF_FULL(n, ent) ((n)->nkeys == LEAF_ORDER(ent))
#define IS_FULL(n, ent) (IS_BRANCH(n) ? IS_BRANCH_FULL(n) : IS_LEAF_FULL(n, ent))
