"""Small MLlib baseline with an explicit, deterministic label map."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from .rss import atomic_write_json
from .normalization import clean_text


LABEL_MAP = {"negative": 0, "neutral": 1, "positive": 2}
TRAIN_SCHEMA = StructType([StructField("row_id", StringType(), False), StructField("text", StringType(), False), StructField("label", StringType(), False), StructField("split", StringType(), False)])


def _metrics(predictions: DataFrame, label_col: str = "label_id") -> dict[str, Any]:
    rows = predictions.groupBy(label_col, "prediction").count().collect()
    matrix = {(int(row[label_col]), int(row["prediction"])): int(row["count"]) for row in rows}
    per_class = []
    for label in range(3):
        tp = matrix.get((label, label), 0)
        fp = sum(matrix.get((other, label), 0) for other in range(3) if other != label)
        fn = sum(matrix.get((label, other), 0) for other in range(3) if other != label)
        support = tp + fn
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / support if support else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_class.append({"label": label, "support": support, "precision": precision, "recall": recall, "f1": f1})
    total = sum(item["support"] for item in per_class)
    return {"accuracy": sum(matrix.get((label, label), 0) for label in range(3)) / total if total else 0.0, "macro_precision": sum(item["precision"] for item in per_class) / 3, "macro_recall": sum(item["recall"] for item in per_class) / 3, "macro_f1": sum(item["f1"] for item in per_class) / 3, "per_class": per_class, "confusion_counts": [{"label": label, "prediction": prediction, "count": matrix.get((label, prediction), 0)} for label in range(3) for prediction in range(3)]}


def train_fixture(spark: SparkSession, input_path: Path, model_path: Path, evidence_path: Path, model_version: str = "tfidf_lr_demo_v2", dataset_id: str = "demo_sentiment", save_model: bool = True) -> dict[str, Any]:
    if save_model and model_path.exists():
        raise FileExistsError(f"refusing to overwrite model: {model_path}")
    df = spark.read.schema(TRAIN_SCHEMA).json(str(input_path))
    clean_text_udf = F.udf(clean_text, StringType())
    df = df.withColumn("text_normalized", clean_text_udf("text")).withColumn("text", F.col("text_normalized"))
    label_expr = F.when(F.col("label") == "negative", F.lit(LABEL_MAP["negative"])).when(F.col("label") == "neutral", F.lit(LABEL_MAP["neutral"])).when(F.col("label") == "positive", F.lit(LABEL_MAP["positive"])).otherwise(F.lit(None).cast(IntegerType()))
    df = df.withColumn("label_id", label_expr)
    train = df.filter(F.col("split") == "train")
    validation = df.filter(F.col("split") == "validation")
    test = df.filter(F.col("split") == "test")
    row_id_overlap = df.groupBy("row_id").agg(F.countDistinct("split").alias("split_count")).filter(F.col("split_count") > 1).count()
    text_split_overlap = df.groupBy("text_normalized").agg(F.countDistinct("split").alias("split_count")).filter(F.col("split_count") > 1).count()
    if row_id_overlap or text_split_overlap:
        raise ValueError(f"training leakage detected: row_id_overlap={row_id_overlap}, normalized_text_overlap={text_split_overlap}")
    tokenizer = RegexTokenizer(inputCol="text", outputCol="tokens", pattern="\\s+", minTokenLength=1)
    remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens", stopWords=["là", "và", "của", "cho", "một"])
    hashing = HashingTF(inputCol="filtered_tokens", outputCol="tf", numFeatures=128)
    idf = IDF(inputCol="tf", outputCol="features")
    best_model = None
    validation_results = []
    for reg_param in (0.01, 0.1):
        classifier = LogisticRegression(featuresCol="features", labelCol="label_id", predictionCol="prediction", probabilityCol="probability", rawPredictionCol="rawPrediction", maxIter=50, regParam=reg_param, family="multinomial")
        pipeline = Pipeline(stages=[tokenizer, remover, hashing, idf, classifier])
        candidate = pipeline.fit(train)
        candidate_metrics = _metrics(candidate.transform(validation))
        validation_results.append({"reg_param": reg_param, "macro_f1": candidate_metrics["macro_f1"]})
        if best_model is None or candidate_metrics["macro_f1"] > best_model[0] or (candidate_metrics["macro_f1"] == best_model[0] and reg_param < best_model[1]):
            best_model = (candidate_metrics["macro_f1"], reg_param, candidate)
    assert best_model is not None
    model = best_model[2]
    predictions = model.transform(test).cache()
    metrics = _metrics(predictions)
    train_counts = {row["label"]: row["count"] for row in train.groupBy("label").count().collect()}
    majority = max(train_counts, key=train_counts.get)
    majority_correct = test.filter(F.col("label") == majority).count()
    test_count = test.count()
    majority_metrics = {"label": majority, "accuracy": majority_correct / test_count if test_count else 0.0}
    overlap_count = row_id_overlap
    if save_model:
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model.write().overwrite().save(str(model_path))
        reloaded = PipelineModel.load(str(model_path))
        save_load_equal = reloaded.transform(test).select("row_id", "prediction").exceptAll(predictions.select("row_id", "prediction")).count() == 0
    else:
        save_load_equal = None
    note = "Synthetic fixture only; not a domain evaluation." if dataset_id == "demo_sentiment" else ("LLM weak-label experiment only; metrics measure agreement with held-out weak labels, not human accuracy." if dataset_id.startswith("llm_weak") else "Official labeled corpus training; metrics describe this corpus and are not a claim of news-domain accuracy.")
    metadata = {"status": "ok", "model_version": model_version, "dataset_id": dataset_id, "input_path": str(input_path.resolve()), "model_path": str(model_path.resolve()) if save_model else None, "model_persisted": save_model, "label_map": LABEL_MAP, "text_preprocess_version": "nfc_html_whitespace_regex_tokenizer_v1", "feature_config": {"num_features": 128, "reg_param_grid": [0.01, 0.1], "selected_reg_param": best_model[1], "max_iter": 50, "seed": None}, "train_count": train.count(), "validation_count": validation.count(), "test_count": test_count, "train_label_counts": train_counts, "validation_results": validation_results, "metrics": metrics, "majority_baseline": majority_metrics, "split_overlap_count": overlap_count, "normalized_text_overlap_count": text_split_overlap, "save_load_prediction_equal": save_load_equal, "trained_at": datetime.now(timezone.utc).isoformat(), "note": note}
    atomic_write_json(evidence_path, metadata)
    predictions.unpersist()
    return metadata
