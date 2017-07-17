#include "eddy-private.h"

#ifdef __APPLE__
# define ED_ASM_INT ".long "
# define ED_ASM_MANGLE "_"
#else
# define ED_ASM_INT ".int "
# define ED_ASM_MANGLE ""
#endif

__asm__(
	".const_data\n"
	".globl " ED_ASM_MANGLE "ed_mimedb_data\n"
	".balign 16\n"
	ED_ASM_MANGLE "ed_mimedb_data:\n"
	".incbin \"test/mime.cache\"\n"
	".globl " ED_ASM_MANGLE "ed_mimedb_end\n"
	".balign 1\n"
	ED_ASM_MANGLE "ed_mimedb_end:\n"
	".byte 1\n"
	".globl " ED_ASM_MANGLE "ed_mimedb_size\n"
	".balign 16\n"
	ED_ASM_MANGLE "ed_mimedb_size:\n"
	ED_ASM_INT ED_ASM_MANGLE "ed_mimedb_end - " ED_ASM_MANGLE "ed_mimedb_data\n"
);

