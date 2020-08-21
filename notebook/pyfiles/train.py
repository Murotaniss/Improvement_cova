#!/usr/bin/env python
# coding: utf-8

# In[7]:


import os
import click
import datetime
import math
import typing
import random
import functools
import numpy as np
import pyspark

from pyspark.sql import types as T, functions as F
from pyspark.sql.window import Window
from pyspark.ml.linalg import Vectors, VectorUDT
from spark_lib import ss_udf, ss_const, spark_utils, ss_common
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[9]:


def _zipindex_to_column(row_idx):
    row, idx = row_idx
    dic = row.asDict()
    dic['domain_id'] = idx
    return T.Row(**dic)


# In[10]:


# main部分
def main(date):
    """
    main部分動かしたら全部いい感じになるように実装
    """
    date = str(date)
    date_obj = datetime.datetime.strptime(date, '%Y%m%d')
    date_from = (date_obj - datetime.timedelta(days=10)).strftime('%Y%m%d')
    
    
    # ここの部分はdata_getに置き換えて取れるようにする
    posi_df = spark.read.parquet("posi_df")
    nega_df = spark.read.parquet("df_nega")

    # domain_idsをgetする
    domain_ids = get_domain_ids(date_from, date)
    
    bc_domain_ids = spark.sparkContext.broadcast(
        domain_ids.toPandas().set_index("domain").domain_id.to_dict()
    )
    
    
    #特徴の最大数を獲得
    feature_count = spark.table('mining.domain_id_mappings')     .where(F.col('dt') == date)     .select(F.max('domain_id').alias('max_id'))     .collect()[0].max_id + 1
    
    # モデル作成, 評価
    for ssp_page_id, page_ids in ss_const.TARGET_SSP_PAGE_ID.items():
        posi_feature_df,  nega_feature_df = feature_create(date_from, date, ssp_page_id, page_ids, bc_domain_ids)
        ### ここ修正必要あり(posi_feature, nega_feature→df) ###
        posi_feature_df.withColumn("")
        for i in ["cova", "gbdt"]:
            model, train_df, test_df = model_kind_choice(df, model_kind = str(i))
            model_evaluation(model, train_df, test_df,thresholds=[0.5])


# In[11]:


# def data_get(advertisers_id=None, order_id = None):
#     """
#     data取得part
#     """
#     spark.table
    


# In[12]:


def get_domain_ids(date_from, date):
    df = (
        spark.table("mining.uuid_weekly_domains")
        .where(
            F.col("dt").between(date_from, date)
            & F.col("uuid").isin(*ss_const.EXCLUDE_UUID)
            == False
        )
        .selectExpr("uuid", "map_keys(domains) AS domains")
        .groupBy("uuid")
        .agg(F.array_distinct(F.flatten(F.collect_list("domains"))).alias("domains"))
    )

    domain_ids = (
        df.select(F.explode("domains").alias("domain"))
        .groupby("domain")
        .agg(F.count(F.lit(1)).alias("domain_count"))
        .where(F.col("domain_count") >= 500)
        .rdd.zipWithIndex()
        .map(_zipindex_to_column)
        .toDF()
        .select("domain", "domain_id")
        .repartition(1000)
        .persist()
    )
    
    return domain_ids


# In[13]:


def feature_create(
    date_from: str,
    date_to: str,
    ssp_page_id: int,
    page_ids: typing.List[int],
    bc_domain_ids,
):
    """
    特徴量作成パート1
    """

    df = (
        spark.table("mining.uuid_weekly_domains")
        .where(
            F.col("dt").between(date_from, date_to)
            & F.col("ssp_page_id").isin(page_ids)
            & (F.col("uuid").isin(*ss_const.EXCLUDE_UUID) == False)
        )
        .select("uuid", "domains", "device_type")
    )

    rdd = df.rdd.mapPartitions(lambda rows: bc_map_join(rows, bc_domain_ids))
    # .toDF(['uuid', 'feature_ids'])
    schema = T.StructType(
        [
            T.StructField("uuid", T.StringType(), True),
            T.StructField("feature_ids", T.ArrayType(T.IntegerType()), True),
            T.StructField("device_type", T.StringType(), True),
        ]
    )
    df = (
        spark.createDataFrame(rdd, schema)
        .groupBy("uuid")
        .agg(
            F.when(F.array_contains(F.collect_set("device_type"), "app"), 1)
            .otherwise(0)
            .alias("has_app"),
            F.array_sort(
                F.array_distinct(F.flatten(F.collect_list("feature_ids")))
            ).alias("feature_ids"),
        )
        .where(F.size("feature_ids") >= 5)
        .persist()
    )
    
    posi_feature_df = df.join(posi_df)
    nega_feature_df = df.join(nega_df)
    
    return posi_feature_df, nega_feature_df


# In[14]:


def model_kind_choice(df, model_kind = "cova"):
    if model_kind == "cova":
        train_df, test_df = df.randomSplit([0.9, 0.1])
        lr = LogisticRegression(maxIter=100, regParam=0.05, elasticNetParam=0.1)
        model = lr.fit(train_df)
        
    elif model_kind == "gbdt":
        train_df, test_df = df.randomSplit([0.9, 0.1])
        labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df)
        featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df)
        gbt = GBTClassifier(labelcol = "indexedLabel", featurescol = "indexedFeatures")
        pipeline = Pipeline(stages=[labelIndexer, featureIndexer, gbt])
        model = pipeline.fit(train_df)
        
    else:
        raise ValueError("error : input is cova or lgbt only")
    
    return model, test_df, train_df


# In[15]:


def model_evaluation(model, train_df, test_df,thresholds=[0.5]):
    """
    モデルを評価するためのpart
    """
    prediction = model.transform(test_df)

    # metrics
    second_element = F.udf(lambda v: float(v[1]), T.DoubleType())
    prediction = prediction.withColumn('probability', second_element('probability'))         .withColumn('label', prediction.label.cast(T.DoubleType()))
    metrics = BinaryClassificationMetrics(prediction.select('probability', 'label').rdd)
    print("Area under PR = %s" % metrics.areaUnderPR)
    print("Area under ROC = %s" % metrics.areaUnderROC)

    # pr/rec
    cnt = prediction.count()
    for t in thresholds:
        record = {}
        record['threshold'] = t
        record['pr_auc'] = metrics.areaUnderPR
        record['roc_auc'] = metrics.areaUnderROC
        tp = prediction.where((F.col('label') == 1.0) & (F.col('probability') >= t)).count()
        fp = prediction.where((F.col('label') != 1.0) & (F.col('probability') >= t)).count()
        tn = prediction.where((F.col('label') != 1.0) & (F.col('probability') < t)).count()
        fn = prediction.where((F.col('label') == 1.0) & (F.col('probability') < t)).count()
        if tp + fp == 0:
            continue
        lift = (tp / (tp + fp)) / ((tp + fp) / (tp + fp + tn + fn))
        record['tp'] = tp
        record['fp'] = fp
        record['tn'] = tn
        record['fn'] = fn
        record['lift'] = lift
        if tp + fp == 0:
            record['pr'] = 0.0
            record['rec'] = 0.0
        else:
            record['pr'] = tp / (tp + fp)
            record['rec'] = tp / (tp + fn)
        result.append(record)
        print(record)

    return result


# In[ ]:


main(20200501)

