"""Spark SQL aggregates and deterministic zero-baseline growth rules."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .time_windows import completed_window_bounds


def resolve_as_of(explicit: datetime | None, dataset_ids: list[str], latest_event_time: str | None) -> tuple[datetime | None, str]:
    if explicit:
        return explicit, "explicit"
    if len(dataset_ids) != 1:
        raise ValueError("an explicit --as-of is required for mixed or empty dataset scope")
    dataset_id = dataset_ids[0]
    if dataset_id.startswith("historical_"):
        if not latest_event_time:
            raise ValueError("historical dataset has no event_time to anchor analytics")
        return datetime.fromisoformat(latest_event_time.replace("Z", "+00:00")), "historical_latest_event"
    return None, "live_now"


def growth_status(current: int, previous: int) -> dict:
    delta = current - previous
    if previous > 0:
        ratio = delta / previous
        status = "ok"
    elif current > 0:
        ratio = None
        status = "new"
    else:
        ratio = None
        status = "no_activity"
    return {"current_count": current, "previous_count": previous, "delta": delta, "growth_ratio": ratio, "growth_percent": ratio * 100 if ratio is not None else None, "status": status}


def volume_6h(df: DataFrame, start: str | None = None, end: str | None = None) -> DataFrame:
    timed = df.withColumn("_event_ts", F.to_timestamp("event_time"))
    shifted = timed.withColumn("_local_epoch", F.expr("_event_ts + INTERVAL 7 HOURS"))
    counts = shifted.filter(F.col("_local_epoch").isNotNull()).groupBy(F.window("_local_epoch", "6 hours"), "dataset_id", "source", "category").count().select("dataset_id", "source", "category", (F.col("window.start") - F.expr("INTERVAL 7 HOURS")).alias("window_start"), (F.col("window.end") - F.expr("INTERVAL 7 HOURS")).alias("window_end"), "count")
    if not start or not end:
        return counts
    dimensions = timed.select("dataset_id", "source", "category").distinct()
    spark = df.sparkSession
    starts = spark.range(1).select(F.explode(F.sequence(F.to_timestamp(F.lit(start)), F.to_timestamp(F.lit(end)) - F.expr("INTERVAL 6 HOURS"), F.expr("INTERVAL 6 HOURS"))).alias("window_start"))
    grid = dimensions.crossJoin(starts).withColumn("window_end", F.col("window_start") + F.expr("INTERVAL 6 HOURS"))
    return grid.join(counts, ["dataset_id", "source", "category", "window_start", "window_end"], "left").fillna(0, ["count"])


def sentiment_daily(df: DataFrame) -> DataFrame:
    return df.filter(F.col("sentiment_label").isNotNull()).groupBy("dataset_id", "event_date", "source", "category", "sentiment_label").count()


def sentiment_summary(df: DataFrame) -> DataFrame:
    return df.groupBy("dataset_id", "source", "category").agg(F.count("*").alias("article_count"), F.count("sentiment_label").alias("predicted_count")).withColumn("prediction_coverage", F.when(F.col("article_count") > 0, F.col("predicted_count") / F.col("article_count")).otherwise(F.lit(None)))


def category_trends(df: DataFrame, previous_start: str, current_start: str, window_end: str, min_current: int = 10, min_previous: int = 5) -> DataFrame:
    filtered = df.filter(F.col("event_time").isNotNull() & (F.col("event_time") >= F.to_timestamp(F.lit(previous_start))) & (F.col("event_time") < F.to_timestamp(F.lit(window_end))))
    categories = df.select("dataset_id", "source", "category").distinct()
    current = filtered.filter(F.col("event_time") >= F.to_timestamp(F.lit(current_start))).groupBy("dataset_id", "source", "category").count().withColumnRenamed("count", "current_count")
    previous = filtered.filter(F.col("event_time") < F.to_timestamp(F.lit(current_start))).groupBy("dataset_id", "source", "category").count().withColumnRenamed("count", "previous_count")
    result = categories.join(previous, ["dataset_id", "source", "category"], "left").join(current, ["dataset_id", "source", "category"], "left").fillna(0, ["previous_count", "current_count"])
    return result.withColumn("delta", F.col("current_count") - F.col("previous_count")).withColumn("growth_ratio", F.when(F.col("previous_count") > 0, F.col("delta") / F.col("previous_count"))).withColumn("growth_percent", F.col("growth_ratio") * 100).withColumn("eligible", (F.col("current_count") >= min_current) & (F.col("previous_count") >= min_previous)).withColumn("status", F.when((F.col("previous_count") == 0) & (F.col("current_count") > 0), "new").when((F.col("previous_count") == 0) & (F.col("current_count") == 0), "no_activity").otherwise("ok")).orderBy(F.col("growth_ratio").desc_nulls_last(), F.col("current_count").desc(), F.col("category"))


def keyword_document_frequency(df: DataFrame, stopwords: Iterable[str] = ()) -> DataFrame:
    tokens = df.select("dataset_id", "article_id", F.explode(F.split(F.lower(F.regexp_replace("clean_text", r"[^\p{L}\p{N}_]+", " ")), r"\s+")).alias("token"))
    filtered = tokens.filter((F.length("token") > 1) & ~F.col("token").isin(list(stopwords))).dropDuplicates(["dataset_id", "article_id", "token"])
    return filtered.groupBy("dataset_id", "token").count().withColumnRenamed("count", "document_frequency").orderBy(F.col("document_frequency").desc(), F.col("token"))
