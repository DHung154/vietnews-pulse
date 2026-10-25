"""Optional direct SQL / unsupervised clustering, never a sentiment model."""
from uuid import uuid4
from pyspark.sql import functions as F


def sql_tables(clean, dataset_id, bounds):
    view = 'news_lab_' + uuid4().hex
    scoped = clean.filter(F.col('dataset_id') == F.lit(dataset_id))
    scoped.withColumn('_previous', (F.to_timestamp('event_time') >= F.to_timestamp(F.lit(bounds[0]))) &
        (F.to_timestamp('event_time') < F.to_timestamp(F.lit(bounds[1])))).withColumn('_current',
        (F.to_timestamp('event_time') >= F.to_timestamp(F.lit(bounds[1]))) &
        (F.to_timestamp('event_time') < F.to_timestamp(F.lit(bounds[2])))).createOrReplaceTempView(view)
    spark = clean.sparkSession
    daily = spark.sql(f'''SELECT dataset_id,event_date,source,category,COUNT(*) AS count
        FROM {view} WHERE event_date IS NOT NULL GROUP BY dataset_id,event_date,source,category''')
    trend = spark.sql(f'''WITH counts AS (
        SELECT dataset_id,source,category,
          SUM(CASE WHEN _previous THEN 1 ELSE 0 END) AS previous_count,
          SUM(CASE WHEN _current THEN 1 ELSE 0 END) AS current_count
        FROM {view} GROUP BY dataset_id,source,category), deltas AS (
        SELECT *,current_count-previous_count AS delta FROM counts)
        SELECT *, CASE WHEN previous_count>0 THEN delta/previous_count END AS growth_ratio,
          CASE WHEN previous_count>0 THEN 100.0*delta/previous_count END AS growth_percent,
          current_count>=10 AND previous_count>=5 AS eligible,
          CASE WHEN previous_count=0 AND current_count>0 THEN 'new'
               WHEN previous_count=0 AND current_count=0 THEN 'no_activity' ELSE 'ok' END AS status
        FROM deltas''')
    return {'daily_volume_sql': daily, 'category_trends_sql': trend}


def cluster_news(clean, dataset_id, output_uri, k=5, seed=42, vocab_size=4096, pca_dim=None):
    from pyspark.ml import Pipeline
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.feature import CountVectorizer, IDF, Normalizer, RegexTokenizer, PCA
    from pyspark.sql import Window
    scoped = clean.filter(F.col('dataset_id') == F.lit(dataset_id))
    if scoped.select('dataset_id', 'article_id').distinct().count() != scoped.count():
        raise ValueError('clustering requires canonical article keys')
    count = scoped.count()
    if k < 2 or count <= k:
        raise ValueError('k must be >=2 and smaller than the article count')
    if vocab_size < 2 or (pca_dim is not None and not 1 <= pca_dim < vocab_size):
        raise ValueError('invalid vocabulary size or PCA dimension')
    stages = [RegexTokenizer(inputCol='clean_text', outputCol='tokens',
        pattern=r'[^\p{L}\p{N}_]+', minTokenLength=2),
        CountVectorizer(inputCol='tokens', outputCol='tf', vocabSize=vocab_size, minDF=2),
        IDF(inputCol='tf', outputCol='idf')]
    if pca_dim is not None:
        stages.append(PCA(k=pca_dim, inputCol='idf', outputCol='projected'))
    stages.extend([Normalizer(inputCol='projected' if pca_dim else 'idf', outputCol='features'),
        KMeans(k=k, seed=seed, maxIter=20, featuresCol='features', predictionCol='topic_cluster')])
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(scoped)
    model.write().save(output_uri + '/model')
    predicted = model.transform(scoped).cache()
    try:
        predicted.select('dataset_id', 'article_id', 'title', 'canonical_url', 'event_date', 'topic_cluster').write.mode('errorifexists').parquet(output_uri + '/assignments')
        sizes = [r.asDict() for r in predicted.groupBy('topic_cluster').count().orderBy('topic_cluster').collect()]
        examples = predicted.withColumn('_rn', F.row_number().over(Window.partitionBy('topic_cluster').orderBy('article_id'))).filter(F.col('_rn') <= 3)
        samples = [r.asDict() for r in examples.select('topic_cluster', 'article_id', 'title').collect()]
        vocabulary = model.stages[1].vocabulary
        # PCA coordinates are not vocabulary indices; do not invent word labels.
        terms = [] if pca_dim else [[vocabulary[i] for i in sorted(range(len(center)), key=lambda n: -center[n])[:10] if center[i] > 0] for center in model.stages[-1].clusterCenters()]
        silhouette = ClusteringEvaluator(predictionCol='topic_cluster').evaluate(predicted) if len(sizes) > 1 else None
        reloaded = type(model).load(output_uri + '/model').transform(scoped)
        left = predicted.select('article_id', F.col('topic_cluster').alias('_before'))
        right = reloaded.select('article_id', F.col('topic_cluster').alias('_after'))
        mismatch = left.join(right, 'article_id', 'full').filter(
            F.col('_before').isNull() | F.col('_after').isNull() |
            (F.col('_before') != F.col('_after'))).count()
        if mismatch:
            raise ValueError('saved clustering model predictions changed')
        return dict(input_count=count, k=k, seed=seed, sizes=sizes, top_terms=terms, examples=samples,
            feature_config=dict(vocab_size=vocab_size, pca_dim=pca_dim, normalization='L2'),
            training_cost=model.stages[-1].summary.trainingCost,
            pca_explained_variance=model.stages[3].explainedVariance.toArray().tolist() if pca_dim else None,
            silhouette=silhouette, save_reload_mismatches=mismatch,
            status='experimental_pending_semantic_review',
            limitations=['Unsupervised fit on this pool, not sentiment evaluation.',
                        'Clusters are lexical similarity, not proven events; IDs are model-version local.'])
    finally:
        predicted.unpersist()
