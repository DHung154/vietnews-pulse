"""Spark ETL for raw observations -> normalized, globally deduplicated clean data."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .contracts import CLEAN_SCHEMA, RAW_SCHEMA
from .normalization import normalize_record


def read_raw(spark: SparkSession, input_path: str) -> DataFrame:
    return spark.read.option("recursiveFileLookup", "true").schema(RAW_SCHEMA).json(input_path)


def read_input(spark: SparkSession, input_path: str, input_format: str = "raw") -> DataFrame:
    if input_format == "normalized":
        return spark.read.schema(CLEAN_SCHEMA).parquet(input_path)
    return read_raw(spark, input_path)


def normalize_news(df: DataFrame) -> DataFrame:
    normalize_udf = F.udf(normalize_record, CLEAN_SCHEMA)
    raw_columns = [F.col(field.name) for field in RAW_SCHEMA.fields]
    return df.select(normalize_udf(F.struct(*raw_columns)).alias("normalized")).select("normalized.*")


def deduplicate_news(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(F.col("valid") & F.col("article_id").isNotNull())
    # Content is versioned by observation recency. Category is metadata and is
    # resolved separately so an older categorized item cannot replace newer text.
    window = Window.partitionBy("dataset_id", "article_id").orderBy(F.col("last_seen_at").desc_nulls_last(), F.col("observation_id").desc_nulls_last())
    ranked = valid.withColumn("_rn", F.row_number().over(window))
    duplicate = ranked.filter(F.col("_rn") > 1).withColumn("invalid_reason", F.lit("duplicate_article_id")).drop("_rn")
    clean = ranked.filter(F.col("_rn") == 1).drop("_rn")
    seen = valid.groupBy("dataset_id", "article_id").agg(F.min("first_seen_at").alias("first_seen_at"), F.max("last_seen_at").alias("last_seen_at"))
    clean = clean.drop("first_seen_at", "last_seen_at").join(seen, ["dataset_id", "article_id"], "left")
    category_priority = F.when(F.col("category") == "other", F.lit(0)).when(F.col("category_basis") == "item_mapping", F.lit(2)).otherwise(F.lit(1))
    category_window = Window.partitionBy("dataset_id", "article_id").orderBy(category_priority.desc(), F.col("category").asc(), F.col("observation_id").desc_nulls_last())
    category = (
        valid.withColumn("_category_rn", F.row_number().over(category_window))
        .filter(F.col("_category_rn") == 1)
        .select("dataset_id", "article_id", "category", "category_basis", "category_mapping_version", "feed_id", "feed_category_raw", "feed_set_version")
    )
    category_columns = ["category", "category_basis", "category_mapping_version", "feed_id", "feed_category_raw", "feed_set_version"]
    clean = clean.drop(*category_columns).join(category, ["dataset_id", "article_id"], "left")
    return clean, duplicate
