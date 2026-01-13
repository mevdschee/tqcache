### Cleanup / Reshard Tool

The `tqsession-cleanup` tool repairs corrupted data files and can reshard data to a different number of shards:

```bash
# Build the cleanup tool
go build -o tqsession-cleanup ./cmd/tqsession-cleanup

# Dry run to see what would be cleaned (stop the server first!)
./tqsession-cleanup -src-dir /path/to/data -dst-dir /path/to/data_clean -dry-run -verbose

# Clean and keep the same shard count (auto-detected)
./tqsession-cleanup -src-dir /path/to/data -dst-dir /path/to/data_clean

# Reshard from any number of shards to 32 shards
./tqsession-cleanup -src-dir /path/to/data -dst-dir /path/to/data_resharded -target-shards 32
```

| Flag             | Default | Description                                          |
|------------------|---------|------------------------------------------------------|
| `-src-dir`       | `data`  | Source directory containing shard data               |
| `-dst-dir`       |         | Destination directory for output (required)          |
| `-target-shards` | `0`     | Number of target shards (`0` = same as source)       |
| `-dry-run`       | `false` | Only report errors, don't write files                |
| `-verbose`       | `false` | Print detailed progress information                  |

**Features:**
- Auto-discovers source shards (scans for `shard_XX` directories)
- Validates and skips corrupted/invalid entries
- Redistributes keys using consistent FNV hash when resharding

**Important:** Stop TQSession before running the cleanup tool.