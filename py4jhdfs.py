# -*- coding:utf-8 -*-
import sys
import time
import datetime
import pandas as pd
import numpy as np
import logging

# 显示小数位数
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# 显示所有列
pd.set_option('display.max_columns', 1000)
# 显示所有行
pd.set_option('display.max_rows', 1000)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)
# 当console中输出的列数超过1000的时候才会换行
pd.set_option('display.width', 1000)


def quiet_logs(sc):
    # 控制台不打印警告信息
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    logger_py4j = logging.getLogger('py4j')
    logger_py4j.setLevel(logging.ERROR)


def df_head(hc_df, lines=5):
    if hc_df:
        df = hc_df.toPandas()
        return df.head(lines)
    else:
        return None


class Py4jHdfs:
    """
    python操作HDFS
    """

    def __init__(self, sc):
        self.sc = sc
        self.filesystem = self.get_file_system()

    def path(self, file_path):
        """
        创建hadoop path对象
        :param sc sparkContext对象
        :param file_path 文件绝对路径
        :return org.apache.hadoop.fs.Path对象
        """
        path_class = self.sc._gateway.jvm.org.apache.hadoop.fs.Path
        return path_class(file_path)

    def get_file_system(self):
        """
        创建FileSystem
        :param sc SparkContext
        :return FileSystem对象
        """
        filesystem_class = self.sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        hadoop_configuration = self.sc._jsc.hadoopConfiguration()
        return filesystem_class.get(hadoop_configuration)

    def ls(self, path, is_return=False):
        """
        读取文件列表,相当于hadoop fs -ls命令
        :param path hdfs绝对路径
        :return file_list 文件列表
        """

        def file_or_dir(is_file, is_dir):
            if is_file:
                return 'is_file:True'
            elif is_dir:
                return 'is_directory:True'
            else:
                return 'unknow'

        filesystem = self.get_file_system()
        status = filesystem.listStatus(self.path(path))
        try:
            file_index = str(status[0].getPath()).index(path) + len(path)
        except:
            print([])
        file_list = [(str(m.getPath())[file_index:],
                      str(round(m.getLen() / 1024.0 / 1024.0, 2)) + ' MB',
                      str(datetime.datetime.fromtimestamp(m.getModificationTime() / 1000)),
                      str(file_or_dir(m.isFile(), m.isDirectory()))) for m in status]
        if file_list and not is_return:
            for f in file_list:
                print(f)
        if not file_list:
            print([])
        if is_return:
            return file_list

    def exists(self, path):
        return self.filesystem.exists(self.path(path))

    def mkdir(self, path):
        return self.filesystem.mkdirs(self.path(path))

    def mkdirs(self, path, mode="755"):
        return self.filesystem.mkdirs(self.path(path))

    def set_replication(self, path, replication):
        return self.filesystem.setReplication(self.path(path), replication)

    def mv(self, path1, path2):
        return self.filesystem.rename(self.path(path1), self.path(path2))

    def rm(self, path, recursive=True, print_info=True):
        """
        直接删除文件，不可恢复！
        :param path 文件或文件夹
        :param recursive 是否递归删除，默认为True
        """
        try:
            result = self.filesystem.delete(self.path(path), recursive)
            if result:
                if print_info:
                    print('[Info]: Remove File Successful!')
                    return True
            else:
                if print_info:
                    print('[Error]: Remove File Failed!')
                return result
        except Exception as e:
            if print_info:
                print('[Error]: %s' % e)
            

    def safe_rm(self, path, trash_path='.Trash/Current'):
        """
        删除文件，可恢复
        可删除文件/文件夹
        :path 需删除的文件的绝对路径
        """
        try:
            self.filesystem.rename(self.path(path), self.path(trash_path + path))
            print('[Info]: Safe Remove File Successful!')
        except:
            try:
                self.rm(self.path(trash_path + path))
                self.filesystem.rename(self.path(path), self.path(trash_path + path))
                print('[Info]: Safe Remove File Successful!')
            except Exception as e:
                print('[Error]: %s' % e)
                print('[Error]: Remove File Failed!')
        return True

    def exists(self, path):
        return self.filesystem.exists(self.path(path))

    def chmod(self, path, mode):
        self.filesystem.setPermission(self.path(path), mode)

    def chown(self, path, owner, group):
        self.filesystem.setOwner(self.path(path), owner, group)

    #     def get(self, src, dst, del_src=False,use_raw_local_file_system=True):
    #         self.filesystem.copyToLocalFile(del_src, self.path(src), dst, use_raw_local_file_system)

    #     def put(self, src, dst, del_src=False, overwrite=True):
    #         self.filesystem.copyFromLocalFile(del_src, src, dst, overwrite)

    def run_time_count(func):
        """
        计算函数运行时间
        装饰器:@run_time_count
        """

        def run(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            print("Function [{0}] run time is {1} second(s).".format(func.__name__, round(time.time() - start, 4)))
            return result

        return run

    @run_time_count
    def write(self, path, contents, encode='utf-8', overwrite_or_append='overwrite'):
        """
        写内容到hdfs文件
        :param sc SparkContext
        :param path 绝对路径
        :param contents 文件内容 字符串或字符串列表 例如：rdd.collect() 形如:['str0,str1,str2','str3,str4,str5']
        :param encode 输出编码格式
        :param overwrite_or_append 写模式：覆盖或追加
        """
        try:
            filesystem = self.get_file_system()
            if overwrite_or_append == 'overwrite':
                out = filesystem.create(self.path(path), True)
            elif overwrite_or_append == 'append':
                out = filesystem.append(self.path(path))
            if isinstance(contents, list):
                for content in contents:
                    out.write(bytearray(content + '\r\n', encode))
            elif sys.version_info.major == 3 and isinstance(contents, str):
                out.write(bytearray(contents, encode))
            elif sys.version_info.major == 2 and (isinstance(contents, str) or isinstance(contents, unicode)):
                out.write(bytearray(contents, encode))
            else:
                print('[Error]: Input data format is not right!')
                return False
            out.flush()
            out.close()
            print('[Path]: %s' % path)
            print('[Info]: File Saved!')
            return True
        except Exception as e:
            print('[Error]: %s' % e)
            return False

    @run_time_count
    def read(self, path, sep=',', header=None, nrows=None):
        """
        读取hdfs上存储的utf-8编码的单个csv,txt文件，输出为pandas.DataFrame
        :param path 文件所在hdfs路径
        :param sep 文本分隔符
        :param header 设为0时表示第一行作为列名
        :param nrows 读取行数
        """
        filesystem = self.get_file_system()
        file = filesystem.open(self.path(path))
        # print(file)
        data = []
        line = True
        nrow = 0
        if not nrows:
            nrows_ = np.inf
        else:
            nrows_ = nrows
        while line and nrow <= nrows_:
            try:
                nrow = nrow + 1
                line = file.readLine()
                data.append(line.encode('raw_unicode_escape').decode('utf-8').split(sep))
            except Exception as e:
                print('[Info]: %s' % str(e))
                break
        file.close()
        if header == 0:
            data = pd.DataFrame(data[1:], columns=data[0])
        elif header:
            data = pd.DataFrame(data, columns=header)
        else:
            data = pd.DataFrame(data)
        return data

    @run_time_count
    def read_hdfs(self, path, sep=',', header=None, nrows=None):
        """
        读取hdfs上存储的文件，输出为pandas.DataFrame
        :param path 文件所在hdfs路径
        :param sep 文本分隔符
        :param header 设为0时表示第一行作为列名
        :param nrows 读取行数
        """
        filesystem = self.get_file_system()
        files = self.ls(path, is_return=True)
        files = list(map(lambda x: x[0], files))
        file_flag = '/_SUCCESS'
        files = list(filter(lambda x: x != file_flag, files))
        files = list(map(lambda x: path + x, files))
        print('[Info]: Num of need to read files is %s' % len(files))
        # if file_flag in files:
        #     files = list(filter(lambda x: x != file_flag, files))
        #     files = list(map(lambda x: path + x, files))
        #     print('[Info]: Num of need to read files is %s' % len(files))
        # else:
        #     print("[Error]: File format is incorrect! Try to use 'read()' replace 'read_hdfs()'.")
        #     return False
        data = []
        for file_path in files:
            file = filesystem.open(self.path(file_path))
            print('[Info]: Reading file %s' % file_path)
            line = True
            nrow = 0
            if not nrows:
                nrows_ = np.inf
            else:
                nrows_ = nrows
            while line and nrow <= nrows_:
                try:
                    nrow = nrow + 1
                    line = file.readLine()
                    if line is not None:
                        data.append(line.encode('raw_unicode_escape').decode('utf-8').split(sep))
                except Exception as e:
                    print('[Error]: %s' % str(e))
                    break
            file.close()
        if header == 0:
            data = pd.DataFrame(data[1:], columns=data[0])
        elif header:
            data = pd.DataFrame(data, columns=header)
        else:
            data = pd.DataFrame(data)
        return data



if __name__ == '__main__':
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import HiveContext, SQLContext

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    hc = HiveContext(sc)
    sqlContext = SQLContext(sc)
    quiet_logs(sc)
    sc.setLogLevel('Error')
    ph = Py4jHdfs(sc)
    # data = ph.read_hdfs('/hdfs/example.csv', sep=',')
