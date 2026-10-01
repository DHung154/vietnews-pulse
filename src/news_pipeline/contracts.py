"""Explicit Spark schemas for the raw and clean contracts."""

from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType


RAW_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("observation_id", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("dataset_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("feed_url", StringType(), True),
        StructField("feed_id", StringType(), True),
        StructField("feed_category_raw", StringType(), True),
        StructField("feed_canonical_category", StringType(), True),
        StructField("category_mapping_version", StringType(), True),
        StructField("feed_set_version", StringType(), True),
        StructField("source_record_id", StringType(), True),
        StructField("title_raw", StringType(), True),
        StructField("description_raw", StringType(), True),
        StructField("body_raw", StringType(), True),
        StructField("source_crawled_at_raw", StringType(), True),
        StructField("link_raw", StringType(), True),
        StructField("category_raw", StringType(), True),
        StructField("published_at_raw", StringType(), True),
        StructField("crawled_at", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("published_at_parse_ok", BooleanType(), True),
    ]
)


CLEAN_SCHEMA = StructType(
    [
        StructField("dataset_id", StringType(), True),
        StructField("article_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("canonical_url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("body_text", StringType(), True),
        StructField("model_input_text", StringType(), True),
        StructField("model_input_type", StringType(), True),
        StructField("clean_text", StringType(), True),
        StructField("category", StringType(), True),
        StructField("category_basis", StringType(), True),
        StructField("category_mapping_version", StringType(), True),
        StructField("feed_id", StringType(), True),
        StructField("feed_category_raw", StringType(), True),
        StructField("feed_set_version", StringType(), True),
        StructField("published_at", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_time_basis", StringType(), True),
        StructField("first_seen_at", StringType(), True),
        StructField("last_seen_at", StringType(), True),
        StructField("event_date", StringType(), True),
        StructField("quality_flags", ArrayType(StringType()), True),
        StructField("text_hash", StringType(), True),
        StructField("source_record_id", StringType(), True),
        StructField("observation_id", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("feed_url", StringType(), True),
        StructField("valid", BooleanType(), True),
        StructField("invalid_reason", StringType(), True),
    ]
)
