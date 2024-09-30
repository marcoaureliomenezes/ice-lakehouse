import faker

from rand_engine.bulk.benchmarks import Benchmark
from rand_engine.bulk.dataframe_builder import BulkRandEngine
from rand_engine.bulk.core_distincts import CoreDistincts
from rand_engine.bulk.core_numeric import CoreNumeric
from rand_engine.bulk.core_datetime import CoreDatetime
from rand_engine.bulk.templates import RandEngineTemplates
from pyspark.sql.functions import date_format, col



def gen_bulk_data(spark, size):
  bulk_rand_engine = BulkRandEngine()
  fake = faker.Faker(locale="pt_BR")
  metadata = {
  "id": dict(method=CoreNumeric.gen_ints, parms=dict(min=0, max=10**7)),
  "name": dict(method=CoreDistincts.gen_distincts_typed, parms=dict(distinct=[fake.name() for _ in range(1000)])),
  "age": dict(method=CoreNumeric.gen_ints, parms=dict(min=0, max=100)),
  "salary": dict(method=CoreNumeric.gen_floats_normal, parms=dict(mean=10**3, std=10**1, round=2)),
  "purchase_date": dict(method=CoreDatetime.gen_timestamps, parms=dict(start="01-01-2020", end="31-12-2020", format="%d-%m-%Y"))
  }
  df = (
    bulk_rand_engine.create_spark_df(spark, size, metadata)
      .withColumn("last_purchase_date", date_format(col("purchase_date"), "yyyy-MM-dd"))
      .withColumnRenamed("purchase_date", "last_purchase"))
  df.count()
  df.printSchema()
  return df
