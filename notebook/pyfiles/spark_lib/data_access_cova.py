import sys, os, datetime

sys.path.append('{0}/dm_batch_pack/lib'.format(os.getenv("HOME")))
from logger import logger

from lib import data_access
from lib.const import *
from lib.db_const_cova import DB_ENGINE_COVA
import numpy as np, pandas as pd
import pandas.io.sql as psql


def select_active_pixels():
    ''' 現状activeなpixelの一覧を取得する
    '''
    return psql.read_sql( FIND_COVA_PIXELS, con=DB_ENGINE_COVA )


def update_pixels():
    ''' cova側テーブルの更新
    '''
    pixels = data_access.find_similar_user_pixels()
    with DB_ENGINE_COVA.begin() as conn:
        for idx, pixel in pixels.iterrows():
            if pixel.pixel_type != 'co':
                continue
            # 成功したレコードのみを抽出
            if pd.isnull(pixel.predicted_at) or pixel.state != STATE_SUCCESS:
                continue
            # レコード存在チェック
            query = 'SELECT COUNT(*) FROM user_expansion_jobs WHERE id=%s'
            ret = conn.execute(query, (pixel.pixel_id))
            cnt = ret.fetchone()[0]
            ret.close()
            # 存在しない場合はERROR
            if cnt == 0:
                raise ValueError('target job not found')
            # 存在する場合はUPDATE
            else:
                conn.execute( \
                        'UPDATE user_expansion_jobs SET state=%s, updated_at=CURRENT_TIMESTAMP ' + \
                        'WHERE id=%s', \
                        (COVA_STATE_FINISHED, pixel.pixel_id) )
            data_access.update_pixel_state(pixel, STATE_INACTIVE)


def update_pixel_state(pixel, state):
    ''' stateの更新
    '''
    with DB_ENGINE_COVA.begin() as conn:
        sql = 'UPDATE user_expansion_jobs SET state=%s, updated_at=current_timestamp WHERE id=%s'
        conn.execute(sql, (state, pixel.pixel_id))

def get_table_info(cova_job_id):
    with DB_ENGINE_COVA.begin() as conn:
        query = 'SELECT target_table, target_table_id_on_dm, target_table_dt FROM user_expansion_jobs WHERE id = %s'
        ret = conn.execute(query, (cova_job_id))
        record = ret.fetchone()
        ret.close()
        if record is None:
            raise ValueError('target job not found')
        target_cova_table = record[0]
        if target_cova_table  == 'private_segment_snapshots':
            return {
              'target_cova_table': 'cova.private_segment_snapshots',
              'target_cova_id_name': 'private_segment_id',
              'target_cova_id': record[1],
              'target_cova_dt': record[2] 
            }
        elif target_cova_table == 'external_segments':
            return {
              'target_cova_table': 'cova.external_segments',
              'target_cova_id_name': 'external_segment_id',
              'target_cova_id': record[1],
              'target_cova_dt': record[2] 
            }
        elif target_cova_table == 'public_segment_snapshots':
            return {
              'target_cova_table': 'cova.public_segment_snapshots',
              'target_cova_id_name': 'public_segment_id',
              'target_cova_id': record[1],
              'target_cova_dt': record[2]
            }
        elif target_cova_table == 'higashi_user_expansion_test_input':
            return {
              'target_cova_table': 'tmp.higashi_user_expansion_test_input',
              'target_cova_id_name': 'segment_id',
              'target_cova_id': record[1],
              'target_cova_dt': record[2] 
            }
        else:
            raise ValueError('target table is not defined')
         


FIND_COVA_PIXELS = '''
  SELECT 
    pixel_type,
    pixel_id,
    mobile_media_type
  FROM (
    SELECT
      'co' AS pixel_type,
      id AS pixel_id,
      COALESCE(mobile_media_type, 'all') AS mobile_media_type
    FROM
      user_expansion_jobs
    WHERE
      state = 'NEW'
    ORDER BY
      created_at
    LIMIT 4
  ) t1
'''
