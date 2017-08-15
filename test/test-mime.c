#include "../lib/eddy-mime.h"
#include "mu.h"

#define CACHE "mime.cache"

static EdMime *mime;

static void
close_mime(void)
{
	ed_mime_close(&mime);
}

static void
test_mime(void)
{
	mu_teardown = close_mime;

	char path[1024] = __FILE__;
	char *slash = strrchr(path, '/');

	mu_assert_ptr_ne(slash, NULL);
	mu_assert_int_lt(slash - path + 16, sizeof(path));

	memcpy(slash+1, CACHE, sizeof(CACHE));
	mu_assert_int_eq(ed_mime_open(&mime, path, 0), 0);

	memcpy(slash+1, "mime/ascii", sizeof("mime/ascii"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "text/plain");
	memcpy(slash+1, "mime/bmp", sizeof("mime/bmp"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/bmp");
	memcpy(slash+1, "mime/eps", sizeof("mime/eps"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/x-eps");
	memcpy(slash+1, "mime/gif", sizeof("mime/gif"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/gif");
	memcpy(slash+1, "mime/jp2", sizeof("mime/jp2"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/jp2");
	memcpy(slash+1, "mime/jpg", sizeof("mime/jpg"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/jpeg");
	memcpy(slash+1, "mime/jxr", sizeof("mime/jxr"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "application/octet-stream");
	memcpy(slash+1, "mime/pdf", sizeof("mime/pdf"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "application/pdf");
	memcpy(slash+1, "mime/png", sizeof("mime/png"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/png");
	memcpy(slash+1, "mime/psd", sizeof("mime/psd"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/vnd.adobe.photoshop");
	memcpy(slash+1, "mime/tga", sizeof("mime/tga"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/x-tga");
	memcpy(slash+1, "mime/tiff", sizeof("mime/tiff"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/tiff");
	memcpy(slash+1, "mime/utf16-be", sizeof("mime/utf16-be"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "text/plain; charset=utf-16be");
	memcpy(slash+1, "mime/utf16-le", sizeof("mime/utf16-le"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "text/plain; charset=utf-16le");
	memcpy(slash+1, "mime/utf32-be", sizeof("mime/utf32-be"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "text/plain; charset=utf-32be");
	memcpy(slash+1, "mime/utf32-le", sizeof("mime/utf32-le"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "text/plain; charset=utf-32le");
	memcpy(slash+1, "mime/utf8", sizeof("mime/utf8"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "text/plain; charset=utf-8");
	memcpy(slash+1, "mime/webp", sizeof("mime/webp"));
	mu_assert_str_eq(ed_mime_file_type(mime, path, true), "image/webp");
}

int
main(void)
{
	mu_init("mime");
	mu_run(test_mime);
	return 0;
}

