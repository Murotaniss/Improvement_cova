''' tasks共通処理
'''

import os
import json
import argparse
import click

from pyspark.sql import SparkSession

def create_work_dir(dir, work='tmp'):
    ''' 作業ディレクトリの作成
    '''
    path = os.path.join(dir, work)
    if not os.path.exists(path):
        os.mkdir(path)
    return path

class JsonFileParam(click.File):
    ''' jsonファイルを引数に受け取りパース済みで返すclick param type
    '''
    name = 'json file param'

    def convert(self, value, param, ctx):
        try:
            return json.load(super().convert(value, param, ctx))
        except (IOError, OSError) as e:
            self.fail('Could not parse file: %s: %s' % (
                click.types.filename_to_ui(value),
                click.types.get_streerror(e),
            ), param, ctx)

    def __repr__(self):
        return 'JsonFileParam(%r)' % (self.pattern)

#def date_argument_parser():
#    ''' 日付コマンドライン引数のパース
#    '''
#    parser = argparse.ArgumentParser()
#    parser.add_argument("--date", nargs='?', required=True, help='処理対象日(例: 20181119)')
#    args = parser.parse_args()
#
#    return args
#
#def date_hour_argument_parser(hour_require=True):
#    ''' 日時コマンドライン引数のパース
#    '''
#    parser = argparse.ArgumentParser()
#    parser.add_argument("--date", nargs='?', required=True, help='処理対象日(例: 20181119)')
#    parser.add_argument("--hour", nargs='?', required=hour_require, help='処理対象時(例: 1)')
#    args = parser.parse_args()
#
#    return args

#class SsTask:
#    ''' Task用共通処理
#    '''
#    def __init__(self, work_dir=None, spark=True):
#        if spark:
#            self.spark = SparkSession.builder.appName("dm.%s" % os.path.basename(__file__)).enableHiveSupport().getOrCreate()
#        self.arg_parser = argparse.ArgumentParser()
#        self.work_dirs = []
#        if not work_dir is None:
#            self.work_dirs.append(work_dir)
#
#    def set_argument(self):
#        pass
#
#    def make_work_dirs(self):
#        ''' 作業ディレクトリの生成
#        '''
#        for d in self.work_dirs:
#            if not os.path.exists(d):
#                os.mkdir(d)
#
#    def run(self):
#        self.set_argument()
#        args = self.arg_parser.parse_args()
#        self.make_work_dirs()
#        self.main(args)

#class SsDailyTask(SsTask):
#    def set_argument(self):
#        self.arg_parser.add_argument("--date", nargs='?', required=True, help='処理対象日(例: 20181119)')
#
#class SsDateHourTask(SsTask):
#    def __init__(self, work_dir=None, spark=True, hour_require=True):
#        super().__init__(work_dir, spark)
#        self.hour_require = hour_require
#
#    def set_argument(self):
#        self.arg_parser.add_argument("--date", nargs='?', required=True, help='処理対象日(例: 20181119)')
#        self.arg_parser.add_argument("--hour", nargs='?', required=self.hour_require, help='処理対象時(例: 1)')
