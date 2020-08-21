import base64
import datetime
import urllib
import unittest

from spark_lib.ss_common import *
from pyspark.sql import types as T, functions as F

# cookie idの生成された時間を取得
get_birth_udf = F.udf(get_birth, T.IntegerType())

# device_typeをosから取得
get_device_from_os_udf = F.udf(get_device_from_os, T.StringType())

# device_typeをosとmedia_typeから取得
get_device_from_os_mediatype_udf = F.udf(get_device_from_os_mediatype, T.StringType())
get_device_udf = F.udf(get_device_from_os_mediatype, T.StringType())

# os_typeの取得
get_os_type_udf = F.udf(get_os_type, T.StringType())

# unixtimestampからhour(int)を返す
timestamp_to_hour_udf = F.udf(timestamp_to_hour, T.IntegerType())

# pmp判定
is_pmp_udf = F.udf(is_pmp, T.IntegerType())

# movie判定
is_movie_udf = F.udf(is_movie, T.IntegerType())

# 値をintに変換して取得（default=-1）
get_int_or_minus_one_udf = F.udf(get_int_or_default)

# 値をintに変換して取得（default=0）
get_int_or_zero_udf = F.udf(get_int_or_zero)

# media_typeに応じてbundle_id, track_id, domainのいずれかを返す
get_domain_or_app_udf = F.udf(get_domain_or_app, T.StringType())

# media_typeに応じてbundle_id, track_id, domainのいずれかを返す
get_domain_or_app_from_tp_udf = F.udf(get_domain_or_app_from_tp, T.StringType())

# media_typeに応じてbundle_id, track_id, domainのいずれかを返す
get_domain_or_app_from_domain_udf = F.udf(get_domain_or_app_from_domain, T.StringType())

# domain取得（サブ階層付き）
get_domain_udf = F.udf(get_domain, T.StringType())

# domainからspやwww等のprevixを削除する
domain_normalize_udf = F.udf(domain_normalize, T.StringType())

# tpからspやwww等のprefixを削除したdomainを返す
domain_normalize_from_tp_udf = F.udf(domain_normalize_from_tp, T.StringType())

# domainからwww等のprefixを削除
exclude_domain_prefix_udf = F.udf(exclude_domain_prefix, T.StringType())

# page_idをsspの企業ごとに集約したidを返す
get_ssp_page_id_udf = F.udf(get_ssp_page_id)

class TestSSUdf(unittest.TestCase):

    def test_get_os_type(self):
        self.assertEqual(get_os_type('iPhone OS 9', ''), 'iOS')
        self.assertEqual(get_os_type('iPod', ''), 'iOS')
        self.assertEqual(get_os_type('iPad', ''), 'iOS')
        self.assertEqual(get_os_type('Android 4.0', ''), 'Android')
        self.assertEqual(get_os_type('Linux', ''), 'Linux')
        self.assertEqual(get_os_type('Mac OS X', ''), 'Mac')
        self.assertEqual(get_os_type('Windows 8', ''), 'Windows')
        self.assertEqual(get_os_type('Windows Phone 7', ''), 'Windows')
        self.assertEqual(get_os_type('something', ''), 'Unknown')
        self.assertEqual(get_os_type('something', 'Mozilla/5.0 (PlayStation 4 6.71) AppleWebKit/605.1.15 (KHTML, like Gecko)'), 'PlayStation')
        self.assertEqual(get_os_type('Unknown', 'Mozilla/5.0 (Nintendo WiiU) AppleWebKit/536.30 (KHTML, like Gecko) NX/3.0.4.2.13 NintendoBrowser/4.3.2.11274.JP'), 'Nintendo')

if __name__ == '__main__':
    # /opt/spark-2.4.3/bin/spark-submit ss_udf.py
    unittest.main()
