#!/usr/bin/env python
# coding: utf-8

# In[121]:


import pandas as pd
import os
import pandas as pd
import re
from datetime import datetime, timedelta
import requests


# In[2]:


# データ読み込み
get_ipython().run_line_magic('load_ext', 'nb_black')
df = pd.read_table("../data/imp_clk_cv.tsv")


# * sparkコード
# 
# `df_cl = suppon_df.filter(col('action') == 'click')`
# 
# `df_imp = suppon_df.filter(col('action') == 'impression')`

# In[3]:


# clickとimpressionでデータを分ける
df_cl = df[df["action"] == "click"]
df_imp = df[df["action"] == "impression"]


# In[4]:


# 数チェック
print("以前のノートブックdf_clのcount数 : 1236", "df_clのcount数 : ", len(df_cl))
print("以前のノートブックdf_impのcount数 : 1236", "df_impのcount数 : ", len(df_imp))


# * sparkコード
# 
# `df_nega = df_imp.join(df_cl, df_imp.uuid == df_cl.uuid , 'left_anti')`

# In[40]:


outer_df = pd.merge(df_cl, df_imp[["uuid"]], on="uuid", how="outer", indicator=True)
df_nega = outer_df[outer_df["_merge"] == "right_only"].drop("_merge", 1)


# In[29]:


len(outer_df[outer_df["_merge"] == "both"])


# In[30]:


len(outer_df[outer_df["_merge"] == "left_only"])


# In[31]:


len(outer_df[outer_df["_merge"] == "right_only"])


# * spark_code
# 
# `def make_data_for_autogenerate_segment(df_input):
#   df_cl = df_input.filter(col('action') == 'click')
#   df_imp = df_input.filter(col('action') == 'impression')
#   df_nega = df_imp.join(df_cl, df_imp.uuid == df_cl.uuid , 'left_anti')`
#   
#   `df_posi_renamed = df_cl.select('uuid','dt').distinct()\
#   .withColumn('label', lit('target')).withColumn('user_id', col('uuid')).withColumn('cookie_id', col('uuid')).withColumnRenamed('dt','target_date')
#   df_posi_for_dsic = df_posi_renamed.select('user_id','cookie_id','target_date','label')`
#   
#   `df_nega_renamed = df_nega.select('uuid','dt')\
#   .withColumn('user_id',col('uuid')).withColumn('cookie_id',col('uuid')).withColumn('label',lit('non_target')).withColumnRenamed('dt','target_date')
#   df_nega_for_dsic = df_nega_renamed.select('user_id','cookie_id','target_date','label')`
#   
#   `return df_posi_for_dsic,df_nega_for_dsic`

# In[105]:


def make_data_for_autogenerate_segment(df_input):
    df_cl = df_input[df_input["action"] == "click"]
    df_imp = df_input[df_input["action"] == "impression"]

    outer_df = pd.merge(df_cl["uuid"], df_imp, on="uuid", how="outer", indicator=True)
    df_nega = outer_df[outer_df["_merge"] == "right_only"].drop("_merge", 1)

    df_posi_for_dsic = (
        df_cl[~df_cl.duplicated()][["uuid", "dt"]]
        .assign(label="target", user_id=df_cl["uuid"])
        .rename(columns={"dt": "target_date", "uuid": "cookie_id"})[
            ["user_id", "cookie_id", "target_date", "label"]
        ]
    )
    df_nega_for_dsic = (
        df_nega[~df_nega.duplicated()][["uuid", "dt"]]
        .assign(label="non_target", user_id=df_nega["uuid"])
        .rename(columns={"dt": "target_date", "uuid": "cookie_id"})[
            ["user_id", "cookie_id", "target_date", "label"]
        ]
    )

    return df_posi_for_dsic, df_nega_for_dsic


# * spark_code
# 
# `df_posi,df_nega = make_data_for_autogenerate_segment(suppon_df)`

# In[106]:


df_posi, df_nega = make_data_for_autogenerate_segment(df)


# In[109]:


df_posi.head()


# In[110]:


df_nega.head()


# In[119]:


df_posi.isnull().any()


# In[120]:


df_nega.isnull().any()


# # for cova

# * spark_code
# 
# `fortuna_df = seed_df\
#                     .select('uuid')\
#                     .withColumnRenamed('uuid', 'so_uuid')\
#                     .withColumn('uuid', cut_id_type(col('so_uuid')))\
#                     .withColumn('id_type', 
#                                   when(
#                                       col('so_uuid').rlike('^idfa'), lit('idfa')
#                                   )\
#                                   .when(
#                                       col('so_uuid').rlike('^anid'), lit('anid')
#                                   )\
#                                   .otherwise(lit('cookie'))
#                                 )\
#                     .drop('so_uuid')`

# In[157]:


# idfaとanidを全て空白にかえるラムダ式
cut_id_type = lambda x: re.sub("(idfa:|anid:)", "", x)


# In[225]:


# カラム名を変更
seed_df = df_posi.rename(columns={"user_id": "uuid"})

# 定値cookieが入ったid_typeカラムを作成
seed_df["id_type"] = "cookie"

# so_uuid列を複製
seed_df["so_uuid"] = seed_df["uuid"]

# uuidからidfaとanidの冒頭を削除
seed_df["uuid"] = seed_df["uuid"].map(cut_id_type)

# 複製しておいたuuidからidfa, anid, cookieでidtypeを割り振る
seed_df.loc[seed_df["uuid"].str.contains("^idfa"), "id_type"] = "idfa"
seed_df.loc[seed_df["uuid"].str.contains("^anid"), "id_type"] = "anid"
fortuna_df = seed_df[["uuid", "id_type"]]


# In[226]:


# 確認
fortuna_df.head()

