# Eddy

High-performance, maintenence-light, caching library and tools.

Rather than rely on precise LRU caching, eddy implements an indexing scheme on
top of a fixed size ring buffer. This can be either a pre-allocated file on disk
or an entire device; both character and block devices are supported. The ring
has the benefit of guaranteed sequential writes, no fragmentation of cached
object blocks, and no ongoing disk space utilization management.

## Building

For building the full library and tools:

```bash
make
make install
```
    
If you only want parts, these may be invoked individually:

```bash
make bin # build only executable tools (ed-new, ed-get, ed-set, etc.)
make lib # build the static and dynamic libraries
make static # build only the static library
make dynamic # build only the dynamic library and version symlinks
```

### Options

| Option | Description | Default |
| --- | --- | --- |
| `BUILD` | Build mode: `release` or `debug`. | `release` |
| `BUILD_MIME` | Build the MIME module. This is for both the command line tool and internal `mime.cache` database reader. | `yes` |
| `BUILD_MIMEDB` | Link the MIME database with the MIME module. This allows the MIME module to work without a local database. | `no` |
| `BUILD_DEV` | Build the maintainer tools. These are not needed for managing the cache. | `no` for `release`, `yes` for `debug`  |
| `OPT` | Optimization level of the build. | `3` for `release`, unset for `debug` |
| `LTO` | Enable link time optimizations. Additionally, this may be set to `amalg` which will produce an amalgamated source build rather than using the compiler LTO. The amalgamated build is always used for the static library any time LTO is enabled. | `yes` for `release`, `no` for `debug` |
| `DEBUG` | Build with debugging symbols. This allows `release` builds to retain debugging symbols. | `no` for optimized, `yes` otherwise |
| `DEBUG_MMAP` | Build with `mmap` tracking. | `no` |
| `CFLAGS` | The base compiler flags. These will be mixed into the required flags. | `-O$(OPT) -DNDEBUG` optimized, `-Wall -Werror` otherwise |
| `LDFLAGS` | The base linker flags. These will be mixed into the required flags.  | _no default_ |
| `PREFIX` | Base install directory. | `/usr/local` |
| `LIBNAME` | Base name of library products. | `eddy` |
| `BINNAME` | Base name of binary products. | `ed-` |
| `PAGESIZE` | The target page size. Generally, this should not be changed. | result of `getconf PAGESIZE` |

<sup>1</sup>This also creates an amalgamated build for the static library.
Setting this to `amalg` will disable the compiler link time optimi

A cusomized build may be maintained by placing a `Build.mk` file in the
root of the source tree. If present, this will be included in the Makefile,
allowing the persistence of overrides to these build settings.

# Thread Safety

Generally, eddy is geared towards parallel, multi-process access. Currently,
thread safety is implemented with the expectation that all threads within
a process will share the same `EdCache` object. That is, it is safe to access
and modify the cache from multiple threads _with the same handle_, but it is
currently not guaranteed to be safe to open multiple cache handles to the same
cache within a single process.

This may change in the future if the use case becomes necessary.
