
# Iceberg Lakehouse

Esse repositório contém um Lakehouse construído utilizando as seguintes tecnologias:

- **Apache Spark**: Motor de processamento de dados distribuído.
- **Minio**: Object Storage para armazenamento de dados em formato de objetos similar ao S3.
- **Apache Iceberg**: Formato de tabela para armazenamento de dados em camadas.
- **Projeto Nessie**: Catalogo com controle de versão para tabelas Iceberg.
- **Dremio**: Motor de consulta SQL para acesso aos dados armazenados no Lakehouse.
- **Jupyter**: Ambiente de desenvolvimento para execução de código Python e Scala.


## 1. Introdução



## 2. Configurações Configuração de Source em Dremio

### Source Nessie

- Name:  `nessie`
- Nessie Endpoint URL: `http://nessie:19120/api/v2`
- Nessie Authentication Type: `None`
- AWS Access Key: `access_key`
- AWS Access Secret: `access_secret`
- Connection Properties:
  - `fs.s3a.path.style.access   true`
  - `dremio.s3.compat   true`
  - `fs.s3a.endpoint    minio:9000`

### Source Minio

- Name:  `minio`
- AWS Access Key: `access_key`
- AWS Access Secret: `access_secret`

- Check `Enable compatibility mode`
- Connection Properties:
  - `fs.s3a.path.style.access  true`
  - `dremio.s3.compat   true`
  - `fs.s3a.endpoint    minio:9000`


Esse repositório contém um Lakehouse construído utilizando as seguintes tecnologias:

- **Apache Spark**: Motor de processamento de dados distribuído.
- **Minio**: Object Storage para armazenamento de dados em formato de objetos similar ao S3.
- **Apache Iceberg**: Formato de tabela para armazenamento de dados em camadas.
- **Projeto Nessie**: Catalogo com controle de versão para tabelas Iceberg.
- **Dremio**: Motor de consulta SQL para acesso aos dados armazenados no Lakehouse.

### Copy-on-write

Whenever there is a write operation, the data is written to a new file and the old file is not modified. This is called copy-on-write. This is a very important concept in Iceberg.



## Tabelas Iceberg


### Merge-on-read

When a read operation is performed, the data is read from multiple files and merged on the fly. This is called merge-on-read. This is a very important concept in Iceberg.


### Iceberg TBLPROPERTIES

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