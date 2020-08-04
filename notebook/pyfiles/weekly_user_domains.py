''' uuid_weekly_domainsテーブルの更新
'''

import os
import click
import datetime
import collections

import pyspark
from pyspark.sql import types as T, functions as F

from spark_lib import spark_utils

spark = spark_utils.build_spark_session(os.path.basename(__file__))

@click.command()
@click.option('--date', required=True, type=int)
def main(date):
    ''' uuid_weekly_domainsテーブルへのデータ投入
    '''
    date_obj = datetime.datetime.strptime(str(date), '%Y%m%d')
    date_from = (date_obj - datetime.timedelta(days=date_obj.weekday())).strftime('%Y%m%d')
    date_from = int(date_from)
    print(date_from, date)
    df = spark.table('mining.uuid_daily_domains') \
        .where(F.col('dt').between(date_from, date)) \
        .groupby('uuid', 'ssp_page_id') \
        .agg(
            F.collect_set('device_type')[0].alias('device_type'), \
            F.collect_set('os_type')[0].alias('os_type'), \
            F.collect_list('domains').alias('domains'), \
            F.collect_list('active_hours').alias('hours') \
        ) \
        .select('uuid', 'device_type', 'os_type', 'domains', 'hours', 'ssp_page_id') \
        .rdd.map(_format_row) \
        .toDF() \
        .cache()
    ssp_page_ids = df.select('ssp_page_id').distinct().toPandas().ssp_page_id
    for ssp_page_id in ssp_page_ids:
        tmp_df = df.where(df.ssp_page_id == ssp_page_id)
        spark_utils.insert_table(spark, tmp_df, INSERT_QUERY.format(date_from), 'temp_dm_uuid_weekly_domains', partition_size=5000000)

def _format_row(row):
    ''' domain, hourのカウント
    '''
    dic = row.asDict()
    # domainのカウント
    domain_counter = collections.Counter()
    for domains in dic['domains']:
        domain_counter += collections.Counter(domains)
    dic['domains'] = dict(domain_counter)
    # hourのカウント
    hour_counter = collections.Counter()
    for hours in dic['hours']:
        hour_counter += collections.Counter(hours)
    dic['hours'] = dict(hour_counter)
    # active_days
    dic['active_days'] = len(dic['domains'])

    return pyspark.sql.types.Row(**dic) 

INSERT_QUERY = '''
INSERT OVERWRITE TABLE mining.uuid_weekly_domains
PARTITION(dt={0}, ssp_page_id)
SELECT uuid, device_type, os_type, domains, hours, active_days, ssp_page_id
FROM temp_dm_uuid_weekly_domains
'''

if __name__ == '__main__':
    main()
