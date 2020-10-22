# -*- coding:utf-8 -*-
import time
import pandas as pd
import numpy as np
from utils.py4jhdfs import Py4jHdfs
from pyspark.sql import HiveContext, SQLContext
from pyspark.sql.types import *


def run_time_count(func):
    """
    计算函数运行时间
    装饰器:@run_time_count
    """

    def run(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        print("[Info]: Function [{0}] run time is {1} second(s).".format(func.__name__, round(time.time() - start, 4)))
        print('')
        return result

    return run


def df_head(hc_df, lines=5):
    if hc_df:
        df = hc_df.toPandas()
        return df.head(lines)
    else:
        return None

def to_str(int_or_unicode):
    if int_or_unicode in (None, '', np.nan, 'None', 'nan'):
        return ''
    try:
        return str(int_or_unicode)
    except:
        return int_or_unicode


def to_int(str_or_int):
    if str_or_int in (None, '', np.nan, 'None', 'nan'):
        return None
    try:
        return int(float(str_or_int))
    except:
        return str_or_int


def to_float(int_or_float):
    if int_or_float in (None, '', np.nan, 'None', 'nan'):
        return np.nan
    try:
        return float(int_or_float)
    except:
        return int_or_float
    
    
@run_time_count
def csv_writer(sc, spark_df, save_path, sep=',', with_header=True, n_partitions=False, mode='overwrite', **kwargs):
    """
    write spark dataframe to csv files with one line header.
    
    exampe:
    data = hc.read.csv('/kt/lem/calc_input/cn/special/src/homb_satisfy/homb_investigate_label_%s.tsv' % 202008,
                       sep='\t',
                       header=True,
                      ).limit(50).repartition(10)
    csv_writer(sc, data, save_path='/kt/lem/calc_input/cn/special/tmp/test.csv', sep=',', n_partitions=None)
    """
    ph = Py4jHdfs(sc)
    if mode == 'overwrite':
        try:
            ph.rm(save_path)
        except Exception as e:
            print('[Warning]: %s dose not exist!' % save_path)
    df = spark_df
    df_header = df.columns
    df_header = df_header[:-1]+[df_header[-1]+'\n']
    df_header = sep.join(df_header)
    ph.write(path=save_path+'/header.csv', contents=df_header, encode='utf-8', overwrite_or_append='overwrite')
    if n_partitions:
        df.coalesce(n_partitions).write.csv(save_path, sep=sep, header=False, mode='append', **kwargs)
    else:
        df.write.csv(save_path, sep=sep, header=False, mode='append', **kwargs)
    print('[Info]: File Save Success!')
    return True


@run_time_count
def csv_reader(sc, file_dir, sep=',', header_path='/header.csv', **kwargs):
    """
    read csv files to spark dataframe with one line header.
    
    exampe:
    df = csv_reader(sc, '/kt/lem/calc_input/cn/special/tmp/test.csv')
    df.show(100)
    """
    hc = HiveContext(sc)
    ph = Py4jHdfs(sc)
    files = ph.ls(file_dir,is_return=True)
    files = [file_dir+x[0] for x in files]
    files = list(filter(lambda x: '/part-' in x, files))
    header = sc.textFile(file_dir+header_path).collect()[0].split(sep)
    df = hc.read.csv(files, sep=sep, header=False, **kwargs)
    for old_col,new_col in zip(df.columns,header):
        df = df.withColumnRenamed(old_col,new_col)
    return df


def save_dataframe_by_rdd(sc, data_df, save_path, with_header=True, sep='\t', n_partitions=10):
    ph = Py4jHdfs(sc)
    if not data_df:
        print('[Warning]: There Is No Data To Save!')
        return None
    header = data_df.columns
    header = sc.parallelize([sep.join(header)])
    #     print(header.collect())
    print(save_path)
    data_df = data_df.rdd.map(tuple).map(lambda r: tuple([to_str(r[i]) for i in range(len(r))])).map(
        lambda r: sep.join(r))
    if with_header:
        data_df = header.union(data_df)
    ph.rm(save_path)
    #     print(data_df.take(2))
    data_df.coalesce(n_partitions).saveAsTextFile(save_path)
    print('File Saved！')


def save_pandas_df(sc, pd_df, path, sep='\t'):
    """
    把pandas DtaFrame存到HDFS
    建议存储较小的文件100w行以内，否则可能会很慢
    """
    ph = Py4jHdfs(sc)
    if not isinstance(pd_df, pd.DataFrame) or pd_df.shape[0]<1:
        print('[Warning]: There is no data to save.')
        return False
    for col in pd_df.columns:
        pd_df[col] = pd_df[col].apply(lambda x: to_str(x))
    header = pd_df.columns.tolist()
    pd_df = pd_df.values.tolist()
    pd_df = [header] + pd_df
    pd_df = list(map(lambda x: sep.join(x), pd_df))
    print('[Path]: %s' % path)
    ph.write(path=path, contents=pd_df, encode='utf-8', overwrite_or_append='overwrite')
    print('[Info]: File Saved!')
    return True


def save_pandas_df_to_hive(sc, pd_df, table_name, mode='append'):
    """
    把pandas.DtaFrame存到Hive表
    """
    hc = HiveContext(sc)
    if not isinstance(pd_df, pd.DataFrame):
        print('[Warning]: Input data type is not pd.DataFrame.')
        return False
    hc_df = hc.createDataFrame(pd_df)
    print(table_name)
    hc_df.write.saveAsTable(name=table_name, mode=mode)
    print('Table Saved!')
    return True


def save_rdd_to_hdfs(sc, input_rdd, save_path, to_distinct=True, sep='\t'):
    ph = Py4jHdfs(sc)
    #     print(type(input_rdd))
    if not input_rdd:
        print('[Warning]: There is no data to save!')
        return False
    rdd = input_rdd
    if to_distinct:
        rdd = rdd.distinct()
    rdd = rdd.map(lambda r: tuple([to_str(r[i]) for i in range(len(r))])).map(lambda r: sep.join(r))
    print(rdd.take(3))
    print(rdd.count())
    output_path = save_path
    print('output_path:', output_path)
    ph.rm(output_path)
    rdd.saveAsTextFile(output_path)
    print('File Saved!')
    return True

def save_hive_data_to_hdfs(sc, select_sql, output_path, with_header=True, sep='\t', n_partitions=10, mode='overwrite',is_deduplication=False):
    hc = HiveContext(sc)
    data = hc.sql(select_sql)
    if is_deduplication:
        data = data.drop_duplicates()
    print('[Path]: %s' % output_path)
    csv_writer(sc, data, save_path=output_path, sep=sep, n_partitions=n_partitions, with_header=with_header)
#     data.coalesce(n_partitions).write.csv(output_path,sep=sep,header=with_header,mode=mode)
    print('[Info]: File saved!')
    return True


DICT_SCHEMA = {'str': StringType(),
               'object': StringType(),
               'int': IntegerType(),
               'int32': IntegerType(),
               'int64': IntegerType(),
               'long': LongType(),
               'float': FloatType(),
               'float32': FloatType(),
               'float64': FloatType(),
               }
DICT_DTYPE = {'str': to_str,
              'object': to_str,
              'int': to_int,
              'int32': to_int,
              'int64': to_int,
              'long': to_int,
              'float': to_float,
              'float32': to_float,
              'float64': to_float,
              }


def create_schema_from_field_and_dtype(field_list, dtype_list):
    schema = StructType([StructField(field, DICT_SCHEMA.get(dtype, StringType()), True) for field, dtype in zip(field_list, dtype_list)])
    return schema


def transform_dtype(input_rdd_row, input_dtype):
    return tuple([DICT_DTYPE[d](r) for r, d in zip(input_rdd_row, input_dtype)])


def replace_nans(input_rdd_row, to_replace=None):
    def replace_nan(x, to_replace):
        if x not in ('nan', 'None', 'NaN', ''):
            return x
        return to_replace

    return tuple([replace_nan(i, to_replace) for i in input_rdd_row])


def write_rdd_to_hive_table_by_partition(hc, input_rdd, field_list, dtype_list, table_name, partition_by, mode='append', sep='\t'):
    mode_map = {'append': 'into', 'overwrite': 'overwrite'}
    schema = create_schema_from_field_and_dtype(field_list, dtype_list)
    data = input_rdd
    data = data.map(lambda r: replace_nans(r, None))
    data = data.map(lambda r: transform_dtype(r, dtype_list))
    #     print(data.take(3))
    data = hc.createDataFrame(data, schema=schema)
    #     print(['len(data.columns)',len(data.columns)])
    data.registerTempTable("table_temp")  # 创建临时表
    #     print(hc.sql('select * from table_temp limit 2').show())
    insert_sql = " insert %s %s partition(%s=%s) select * from %s " % (mode_map[mode], table_name, partition_by['key'], partition_by['value'], 'table_temp')
    #     print(insert_sql)
    hc.sql(insert_sql)  # 插入数据
    #     data.write.mode(mode).format(format).partitionBy([partition_by['key']]).saveAsTable(table_name) # 有BUG无法使用
    print('[Info]: Partition: %s=%s' % (partition_by['key'], partition_by['value']))
    print('[Info]: Save Table Success!')
    return True


def write_pd_dataframe_to_hive_table_by_partition(hc, input_df, field_list, dtype_list, table_name, partition_by, mode='append', sep='\t'):
    if not isinstance(input_df,pd.DataFrame):
        print('[Warning]: There is no data for date %s to save!' % partition_by['value'])
        return False
    mode_map = {'append': 'into', 'overwrite': 'overwrite'}
    schema = create_schema_from_field_and_dtype(field_list, dtype_list)
    data = input_df
    for field,dtype in zip(field_list, dtype_list):
        try:
            data[field] = data[field].apply(lambda x: DICT_DTYPE[dtype](x))
        except Exception as e:
            print('[Error]: %s' % e)
    data = hc.createDataFrame(data, schema=schema).drop('stat_day')
    #     print(['len(data.columns)',len(data.columns)])
    data.registerTempTable("table_temp")  # 创建临时表
    #     print(hc.sql('select * from table_temp limit 2').show())
    insert_sql = " insert %s %s partition(%s=%s) select * from %s " % (mode_map[mode], table_name, partition_by['key'], partition_by['value'], 'table_temp')
    #     print(insert_sql)
    hc.sql(insert_sql)  # 插入数据
    #     data.write.mode(mode).format(format).partitionBy([partition_by['key']]).saveAsTable(table_name) # 有BUG无法使用
    print('[Info]: Partition: %s=%s' % (partition_by['key'], partition_by['value']))
    print('[Info]: Save Table Success!')
    return True


if __name__ == '__main__':
    print(to_float(None))
