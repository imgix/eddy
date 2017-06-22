#include "eddy-private.h"

typedef struct EdStr EdStr;

struct EdStr {
	size_t ref, len;
	char str[];
};

/* String module */
ED_LOCAL      EdStr *ed_str_new(const char *cstr, ssize_t len);
ED_LOCAL      EdStr *ed_str_ref(EdStr *);
ED_LOCAL        void ed_str_unref(EdStr **);
ED_LOCAL const char *ed_str_value(const EdStr *);
ED_LOCAL      size_t ed_str_length(const EdStr *);

EdStr *
ed_str_new(const char *cstr, ssize_t len)
{
	if (len < 0) { len = strlen(cstr); }
	EdStr *str = malloc(sizeof(*str) + len + 1);
	if (str != NULL) {
		str->ref = 1;
		str->len = len;
		memcpy(str->str, cstr, len);
		str->str[len] = '\0';
	}
	return str;
}

EdStr *
ed_str_ref(EdStr *str)
{
	if (str != NULL) { __sync_add_and_fetch(&str->ref, 1); }
	return str;
}

void
ed_str_unref(EdStr **strp)
{
	EdStr *str;
	do {
		str = *strp;
	} while (!__sync_bool_compare_and_swap(strp, str, NULL));

	if (str != NULL && __sync_sub_and_fetch(&str->ref, 1) == 0) {
		free(str);
	}
}

const char *
ed_str_value(const EdStr *str)
{
	return str->str;
}

size_t
ed_str_length(const EdStr *str)
{
	return str->len;
}

