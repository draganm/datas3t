# TAR Index Disk Cache

This package provides a thread-safe, persistent disk-based cache for tarindex files.

## Overview

The cache stores tarindex files for efficient retrieval of pre-computed index data without regenerating it each time. The cache is completely decoupled from any specific domain concepts and uses simple string keys.

## Cache Keys

Each tarindex file is identified by a simple string key provided by the client. The cache uses SHA-256 hashing of the key to generate unique, safe filenames for storage.

**Example keys:**
- `"my-datas3t:dataranges/000000000001-00000000000000000000-00000000000000000999.index.zst"`
- `"another-datas3t:dataranges/000000000002-00000000000001000000-00000000000001000999.index.zst"`

## Features

- **Thread-safe**: Concurrent access is supported across multiple goroutines
- **Persistent**: Cache survives application restarts  
- **Disk-based**: Uses local filesystem storage for durability
- **Automatic cleanup**: Implements LRU (Least Recently Used) eviction policy to manage disk space
- **Size-bounded**: Configurable maximum cache size in bytes that determines total disk space usage
- **Memory-mapped access**: Uses memory-mapped files for efficient index access
- **Atomic writes**: Uses temporary files and atomic renames to ensure data integrity
- **Domain-agnostic**: No coupling to specific domain concepts - uses simple string keys

## Operations

### OnIndex
The primary method for accessing cached tarindex data using a callback pattern:

```go
err := cache.OnIndex(key, callback, indexGenerator)
```

- **key**: `string` - unique identifier for the cached data
- **callback**: `func(*tarindex.Index) error` - called with the loaded index
- **indexGenerator**: `func() ([]byte, error)` - called to generate index data if not cached

If the index is cached, the callback is called immediately with the cached index. If not cached, the indexGenerator is called to create the index data, which is then stored in the cache before calling the callback.

### Additional Methods

- **Stats()**: Returns `CacheStats` with information about cache state (entry count, total size, etc.)
- **Clear()**: Removes all entries from the cache
- **Close()**: Closes all open index files and cleans up resources

## Example Usage

```go
cache, err := diskcache.NewIndexDiskCache("/path/to/cache", 100*1024*1024) // 100MB limit
if err != nil {
    return err
}
defer cache.Close()

// Cache key combines relevant identifiers
key := "my-datas3t" + "dataranges/000000000001-00000000000000000000-00000000000000000999.index.zst"

err = cache.OnIndex(key, 
    func(index *tarindex.Index) error {
        // Use the index here
        fmt.Printf("Index has %d files\n", index.NumFiles())
        return nil
    },
    func() ([]byte, error) {
        // Generate index data if not cached (e.g., download from S3)
        return downloadIndexFromS3(), nil
    },
)
```
