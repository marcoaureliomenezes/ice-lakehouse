
# iceber Lakehouse




- Storage layer
- File Format
- Table format
- Catalog
- Lakehouse Engine



### Copy-on-write

Whenever there is a write operation, the data is written to a new file and the old file is not modified. This is called copy-on-write. This is a very important concept in Iceberg.



### Merge-on-read

When a read operation is performed, the data is read from multiple files and merged on the fly. This is called merge-on-read. This is a very important concept in Iceberg.


### TBLPROPERTIES

- `write.delete.mode`=`copy-on-write`
- `write.update.mode`=`merge-on-read`
- `write.merge.mode`=`merge-on-read`


### Parquet Vetorization

- `read.parquet.vectorization.enabled`=`true`
- `write.parquet.compression-codec`=`snappy`
- `write.format.default`=`parquet`
- `write.delete.format.default`=`avro`

### Cleaning up Metadata Files

- `write.metadata.delete-after.enabled`=`true`
- `write.metadata.previous-versions.max`=`50`

### Column Metrics Tracking



- CALL prod.system.expire_snapshots(
    table => 'default.iceberg_table',
    older_than => now(),
    retain_last => 10
)