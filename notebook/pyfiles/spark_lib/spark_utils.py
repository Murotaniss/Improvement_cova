''' Sparkからファイルベースでテーブルの情報を読込む
'''

import sys
import random

import pyspark
from pyspark.sql import SparkSession, types as T, functions as F

from spark_lib import ss_udf

def query(sql, params=None, spark=None, appName=None, output_type=None, output_path=None, max_rows=100000):
    ''' クエリ実行関数
        @param output_type
            pandas : max_rows件分pandasのDataFrameに値を格納して返す
            spark  : sparkのDataFrameを返す
            csv    : output_pathに指定したパスにmax_rows件までcsvで出力する
            tsv    : output_pathに指定したパスにmax_rows件までtsvで出力する
            json   : output_pathに指定したパスにmax_rows件までjsonで出力する
            None   : 最大1000件まで標準出力にtsvで出力する
    '''
    if appName is None:
        appName = 'dm.query.' + sql[0:30]
    if spark is None:
        spark = build_spark_session(appName)

    spark.udf.register('get_birth', ss_udf.get_birth_udf)
    spark.udf.register('get_device_from_os', ss_udf.get_device_from_os_udf)
    spark.udf.register('get_device_from_os_mediatype', ss_udf.get_device_from_os_mediatype_udf)
    spark.udf.register('timestamp_to_hour', ss_udf.timestamp_to_hour_udf)
    spark.udf.register('is_pmp', ss_udf.is_pmp_udf)
    spark.udf.register('is_movie', ss_udf.is_movie_udf)
    spark.udf.register('get_int_or_minus_one', ss_udf.get_int_or_minus_one_udf)
    spark.udf.register('get_int_or_zero', ss_udf.get_int_or_zero_udf)
    spark.udf.register('get_domain', ss_udf.get_domain_udf)
    spark.udf.register('domain_normalize', ss_udf.domain_normalize_udf)
    spark.udf.register('get_domain_or_app', ss_udf.get_domain_or_app_udf)
    spark.udf.register('get_os_type', ss_udf.get_os_type_udf)
    spark.udf.register('exclude_domain_prefix', ss_udf.exclude_domain_prefix_udf)
    spark.udf.register('get_ssp_page_id', ss_udf.get_ssp_page_id_udf)

    if params is not None:
        sql = sql.format(**params)
    df = spark.sql(sql)

    if output_type == 'pandas':
        return df.limit(max_rows).toPandas()
    elif output_type == 'spark':
        return df
    elif output_type == 'csv':
        if output_path is None:
            raise ValueError('csv output_type required output_path parameter')
        df.limit(max_rows).toPandas().to_csv(output_path, index=False)
    elif output_type == 'tsv':
        if output_path is None:
            raise ValueError('tsv output_type required output_path parameter')
        df.limit(max_rows).toPandas().to_csv(output_path, sep='\t', index=False)
    elif output_type == 'json':
        if output_path is None:
            raise ValueError('json output_type required output_path parameter')
        df.limit(max_rows).toPandas().to_json(output_path)
    else:
        df.limit(1000).toPandas().to_csv(sys.stdout, sep='\t', index=False)

def build_spark_session(name, shuffle_partitions=3000):
    ''' sparkのsession生成
    '''
    return SparkSession.builder \
        .appName("dm.%s" % name) \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .enableHiveSupport() \
        .getOrCreate()

def insert_hbase_table(spark, df, tmp_query, query, tmp_table_name, partition_size=3000000):
    ''' レコード数に応じてpartition数を変動させてINSERTを実行する
        partition_sizeは1度にinsertするレコード数の目安。
    '''
    cnt = df.count()
    df = with_row_number(df)
    for i in range(1000):
        start = i * partition_size
        end = (i + 1) * partition_size
        print(start, end)
        sub_df = df.filter((F.col('rownum') > start) & (F.col('rownum') < end))
        if sub_df.count() == 0:
            break
        sub_df.repartition(1).registerTempTable(tmp_table_name)
        spark.sql(query)
        if end > cnt:
            break

def insert_hive_to_hbase(query):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = proc.communicate()
    if proc.returncode != 0:
        raise ChildProcessError('hive failure : ' + query)

def insert_table(spark, df, query, tmp_table_name, partition_size=3000000):
    ''' レコード数に応じてpartition数を変動させてINSERTを実行する
        partition_sizeは1ファイルに対して保存するレコード数の目安。
        テーブルに応じてpartition_sizeを変更しファイルサイズが128MB〜256MB程度になるよう調整する。
    '''
    cnt = df.count()
    repartition = max(1, int(df.count() / partition_size))
    df.repartition(repartition).registerTempTable(tmp_table_name)
    spark.sql(query)

def with_row_number(df, col_name='rownum'):
    ''' DataFrameに連番カラムを加える
    '''
    def new_row(row_idx):
        row, idx = row_idx
        dic = row.asDict()
        dic[col_name] = idx
        return T.Row(**dic)
    return df.rdd.zipWithIndex().map(new_row).toDF()

def get_hive_table_partitions(spark, table_name):
    ''' 指定テーブルのpartition一覧を取得する
    '''
    def split_partition(s):
        partitions = s.split('/')
        return dict([p.split('=') for p in partitions])

    partitions = spark.sql('show partitions %s' % table_name).toPandas()
    return list(map(split_partition, partitions.partition))

def get_recent_hive_table_partitions(spark, table_name, partition_key='dt'):
    ''' 指定テーブルのpartition一覧を取得する
    '''
    partitions = get_hive_table_partitions(spark, table_name)
    return max([p[partition_key] for p in partitions])

def _read_orc_table(spark, table_name, location, partition_cols=[]):
    ''' ORCファイルのtableを読み込みスキーマを整えて返す
    '''
    df = spark.read.orc(location)
    return _change_column_names(spark, df, table_name, partition_cols)

def _read_tsv_table(spark, table_name, location, partition_cols):
    ''' tsvファイルのtableを読み込みスキーマを整えて返す
    '''
    df = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").load(location)
    return _change_column_names(spark, df, table_name, partition_cols)

def _change_column_names(spark, df, table_name, partition_cols):
    ''' カラム名を整える
    '''
    old_cols = df.schema.names
    new_cols = list(filter(lambda x: not x in partition_cols, spark.table(table_name).schema.names))
    for idx in range(len(new_cols)):
        df = df.withColumnRenamed(old_cols[idx], new_cols[idx])
    return df

def read_partitioned_custom_report(spark, date, term_of_data=None, partition_key=None):
    ''' partitioned_custom_reportテーブルの読込み
    '''
    # create location
    path = '/hive/warehouse/partitioned_custom_report/dt={}'.format(date)
    if term_of_data is not None:
        path += '/term_of_data={:02d}'.format(term_of_data)
    if term_of_data is not None and partition_key is not None:
        path += '/partition_key={}'.format(partition_key)

    # read table
    df = _read_orc_table(spark, 'partitioned_custom_report', path, ['dt', 'term_of_data', 'partition_key'])
    return df

def read_custom_report_conversion(spark, date, term_of_data=None):
    ''' custom_report_conversionテーブルの読込み
    '''
    # create location
    path = '/hive/warehouse/custom_report_conversion/dt={}'.format(date)
    if term_of_data is not None:
        path += '/term_of_data={:02d}'.format(term_of_data)

    # read table
    df = _read_tsv_table(spark, 'custom_report_conversion', path, ['dt', 'term_of_data'])
    return df

def read_bid_hourly(spark, date, hour=None):
    ''' bid_hourlyテーブルの読込
    '''
    # create location
    path = '/data/log-compatible/bid/dt={}'.format(date)
    if hour is not None:
        path += '/hour={:02d}'.format(hour)

    # read table
    df = _read_orc_table(spark, 'access_log.bid_hourly', path, ['dt', 'hour'])
    return df

def rm_hadoop_directory(spark, dir_path):
    ''' hadoop上のディレクトリ削除（1階層のみ対応。再起は事故ると怖いのでしてない）
    '''
    fileSystem = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
    path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
    fs = fileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

    # ファイルが存在しない場合はExceptionは発生させずTrueを返す
    if not fs.exists(path(dir_path)):
        return True

    for status in fs.listStatus(path(dir_path)):
        fs.delete(status.getPath())
    return fs.delete(path(dir_path))

def domain_list_normalize(org_domains):
    ''' ドメイン正規化
        sp, pc, wwwなどのサブドメインの除去や不要サイトの除去を行う
    '''
    domains = set()
    for domain in org_domains:
        domain = ss_udf.domain_normalize(domain)
        if not domain is None:
            domains.add(domain)
    if len(domains) > 100:
        domains = list(domains)
        random.shuffle(domains)
        domains = domains[0:100]
    else:
        domains = list(domains)
    return domains

def short_os_name_and_device(org_os, org_user_agent):
    ''' osの名称を短縮名に変換する
        iPhone, Android, Windows Phone, Windows, Mac, Linux, iPod, iPad, PlayStation, Nintendo, Unknown
    '''
    result_os = 'Unknown'
    result_device = 'Unknown'

    os1 = org_os.split(' ')[0]
    os1_to_device = {
        'iPhone': 'sp', 'Android': 'sp', 'Mac': 'pc', 'Linux': 'pc', 'iPod': 'sp', 'iPad': 'sp', 'PlayStation': 'game', 'Nintendo': 'game'
    }

    if org_os == 'Unknown':
        if 'PlayStation' in org_user_agent:
            os1 = 'PlayStation'
        elif 'Nintendo' in org_user_agent:
            os1 = 'Nintendo'

    if os1 in os1_to_device:
        result_device = os1_to_device[os1]
        result_os = os1
    elif org_os.startswith('Windows Phone'):
        result_device = 'sp'
        result_os = 'Windows Phone'
    elif org_os.startswith('Windows'):
        result_device = 'pc'
        result_os = 'Windows'

    return result_os, result_device
