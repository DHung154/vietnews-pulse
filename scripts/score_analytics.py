from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.analytics import category_trends, completed_window_bounds, keyword_document_frequency, resolve_as_of, sentiment_daily, sentiment_summary, volume_6h  # noqa: E402
from news_pipeline.normalization import clean_text  # noqa: E402
from news_pipeline.paths import validate_artifact_paths  # noqa: E402
from news_pipeline.rss import atomic_write_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean", type=Path, required=True)
    parser.add_argument("--model", type=Path)
    parser.add_argument("--dataset-id")
    parser.add_argument("--as-of", help="UTC ISO timestamp used for completed-window anchoring")
    parser.add_argument("--model-version")
    parser.add_argument("--output-root", type=Path, default=ROOT / "runtime/m05")
    parser.add_argument("--evidence", type=Path, default=ROOT / "evidence/m05/score_manifest.json")
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    validate_artifact_paths(inputs=(args.clean,) + ((args.model,) if args.model else ()), outputs=(args.output_root,), evidence=args.evidence)
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, DoubleType, StringType

    spark = SparkSession.builder.master(args.master).appName("news-m05-score").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    run_id = "score_" + uuid.uuid4().hex[:8]
    enriched_path = args.output_root / run_id / "enriched"
    gold_path = args.output_root / run_id / "gold"
    try:
        clean = spark.read.parquet(str(args.clean))
        if args.dataset_id:
            clean = clean.filter(F.col("dataset_id") == args.dataset_id)
        input_count = clean.count()
        resolved_model_version = args.model_version or (args.model.name if args.model else "unavailable")
        if args.model:
            from pyspark.ml import PipelineModel
            clean_text_udf = F.udf(clean_text, StringType())
            clean_for_model = clean.withColumn("text", clean_text_udf("clean_text"))
            model = PipelineModel.load(str(args.model))
            vector_to_list = F.udf(lambda value: value.toArray().tolist() if value is not None else None, ArrayType(DoubleType()))
            scored = model.transform(clean_for_model).withColumn("probability_array", vector_to_list("probability"))
            enriched = scored.withColumn("sentiment_label", F.when(F.col("prediction") == 0, "negative").when(F.col("prediction") == 1, "neutral").when(F.col("prediction") == 2, "positive")).withColumn("prob_negative", F.col("probability_array")[0]).withColumn("prob_neutral", F.col("probability_array")[1]).withColumn("prob_positive", F.col("probability_array")[2]).withColumn("predicted_class_probability", F.array_max("probability_array")).withColumn("model_version", F.lit(resolved_model_version)).withColumn("text_preprocess_version", F.lit("nfc_html_whitespace_regex_tokenizer_v1")).drop("label_id", "probability_array")
        else:
            enriched = clean.withColumn("sentiment_label", F.lit(None).cast(StringType())).withColumn("prob_negative", F.lit(None).cast(DoubleType())).withColumn("prob_neutral", F.lit(None).cast(DoubleType())).withColumn("prob_positive", F.lit(None).cast(DoubleType())).withColumn("predicted_class_probability", F.lit(None).cast(DoubleType())).withColumn("model_version", F.lit("unavailable")).withColumn("text_preprocess_version", F.lit("nfc_html_whitespace_regex_tokenizer_v1"))
        enriched.write.mode("errorifexists").parquet(str(enriched_path))
        dataset_ids = [row[0] for row in clean.select("dataset_id").distinct().collect()]
        latest_event_time = clean.select(F.max("event_time")).first()[0]
        explicit_as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00")) if args.as_of else None
        resolved_as_of, anchor_source = resolve_as_of(explicit_as_of, dataset_ids, latest_event_time)
        previous_start, current_start, window_end = completed_window_bounds(resolved_as_of)
        volume = volume_6h(enriched, previous_start, window_end)
        daily = sentiment_daily(enriched)
        summary = sentiment_summary(enriched)
        keywords = keyword_document_frequency(enriched)
        trend = category_trends(enriched, previous_start, current_start, window_end)
        for name, frame in (("volume_6h", volume), ("sentiment_daily", daily), ("sentiment_summary", summary), ("keywords_window", keywords), ("category_trends", trend)):
            frame.write.mode("errorifexists").parquet(str(gold_path / name))
        enriched_count = enriched.count()
        manifest = {"status": "ok", "run_id": run_id, "clean_input": str(args.clean.resolve()), "model_path": str(args.model.resolve()) if args.model else None, "enriched_path": str(enriched_path.resolve()), "gold_path": str(gold_path.resolve()), "input_count": input_count, "enriched_count": enriched_count, "dataset_ids": dataset_ids, "input_snapshot_ids": [row[0] for row in enriched.select("snapshot_id").distinct().collect()] if "snapshot_id" in enriched.columns else [], "model_versions": [row[0] for row in enriched.select("model_version").distinct().collect()], "gold_counts": {"volume_6h": volume.count(), "sentiment_daily": daily.count(), "sentiment_summary": summary.count(), "keywords_window": keywords.count(), "category_trends": trend.count()}, "window_bounds": {"previous_start": previous_start, "current_start": current_start, "window_end": window_end}, "anchor_source": anchor_source, "dataset_id_filter": args.dataset_id, "model_version": resolved_model_version, "note": "Model inference is unavailable when --model is omitted; sentiment remains null. Synthetic model inference is not a domain evaluation."}
        (args.output_root / run_id / "analytics_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        atomic_write_json(args.evidence, manifest)
        print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        atomic_write_json(args.evidence, {"status": "failed", "run_id": run_id, "error_type": type(exc).__name__, "error": str(exc)})
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
