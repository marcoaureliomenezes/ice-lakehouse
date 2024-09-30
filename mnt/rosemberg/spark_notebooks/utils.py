import pyspark
from pyspark.sql import SparkSession
import os



def get_spark_session(spark_app):

  MASTER = "spark://spark-master:7077"
  jar_packages = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.95.0",
    "software.amazon.awssdk:bundle:2.17.178",
    "software.amazon.awssdk:url-connection-client:2.17.178",
    #"org.apache.hadoop:hadoop-aws:3.4.0"
  ]

  spark_extensions = [
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
  ]

  conf = (
    pyspark.SparkConf()
    .setAppName('iceberg_hello_world')
    .set('spark.jars.packages', ','.join(jar_packages))
    .set('spark.sql.extensions', ','.join(spark_extensions))
    .set('spark.sql.catalog.nessie', "org.apache.iceberg.spark.SparkCatalog")
    .set('spark.sql.catalog.nessie.uri', os.getenv("NESSIE_URI"))
    .set('spark.sql.catalog.nessie.ref', 'main')
    .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
    .set("spark.executor.cores", "2")
    .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
    .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .set('spark.sql.catalog.nessie.s3.endpoint', os.getenv("MINIO_HOST"))
    .set('spark.sql.catalog.nessie.warehouse', 's3a://warehouse')
    .set('spark.hadoop.fs.s3a.access.key', os.getenv("MINIO_SERVER_ACCESS_KEY"))
    .set('spark.hadoop.fs.s3a.secret.key', os.getenv("MINIO_SERVER_SECRET_KEY"))
    .set("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_HOST"))
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  )
  spark = SparkSession.builder.config(conf=conf).master(MASTER).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  return spark