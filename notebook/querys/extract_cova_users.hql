--------------------------------------------------
-- covaで指定された対象セグメントのuuidを抽出する
-- @param tmp_partition_id
-- @param target_cova_table
-- @param target_cova_id
-- @param target_cova_dt
-- @param target_cova_id_name
-- @param target_dt
--------------------------------------------------

SET mapred.job.name='similar_user_for_cova/extract_cova_users.hql?${tmp_partition_id}';

SET mapred.max.split.size=2048000000;
-- SET mapreduce.job.queuename=service_related;
SET mapreduce.job.queuename=default;

------------------------------
-- INSERT設定
------------------------------
INSERT OVERWRITE TABLE mining.cova_temporary_similar_user
PARTITION( pixel='${tmp_partition_id}' )

-- 対象ユーザー抽出
SELECT
  dmp.uuid,
  ARRAY() AS features
FROM
  (SELECT uuid FROM ${target_cova_table} WHERE ${target_cova_id_name} = ${target_cova_id} AND dt = ${target_cova_dt}) dmp
  JOIN (SELECT uuid FROM mining.uuid_four_week WHERE dt=${target_dt}) fw
  ON dmp.uuid = fw.uuid
ORDER BY
  RAND()
LIMIT
  1000000
;

