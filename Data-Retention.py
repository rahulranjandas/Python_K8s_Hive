# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 15:49:41 2021

@author: rahul.das
"""

##importing libraries
from datetime import datetime, timedelta, date
from pyhive import hive
import subprocess
import psycopg2 as pg

def postgres_connect():
    try:
        conn = pg.connect(database = "dataingest_testing", user = "dataingest_testing", password = "dataingest_testing", host = "10.113.144.152", port = "5432")
        print("Opened Postgres database successfully")
        cur=conn.cursor()
        cur.execute("select table_name, hdfs_path, retention_period, partition_col from data_retention_test")
        tblrows=cur.fetchall()
        print("Closing Postgres connection!!")
        conn.close()
        for row in tblrows:
            tablename.append(row[0])
            hdfspath.append(row[1])
            retention_duration.append(row[2])
            partition_col.append(row[3])
        for row in tablename:
            database.append(row.split('.')[0])
            table.append(row.split('.')[1])
    except Exception as err:
        print("Connection error: Postgres DB Connection not established!!\n",err)
        conn.close()
        print("Closing Postgres connection!!")
        exit()

def hive_connection():
    for i in range(len(tablename)):
        try:
            conn = hive.Connection(host="bspc00c76e1751-001.app.org", port=10000, auth='KERBEROS', database=database[i],  kerberos_service_name='hive', configuration={"hive.server2.authentication.kerberos.principal" : "hive/bspc00c76e1751-001.app.org@APP.ORG", "hive.server2.authentication.kerberos.keytab" : "/home/subexuser/nimesh/hive.service.keytab"})
            print("connected to Hive DB>")
            cursor = conn.cursor()
            print(1)
            sql='show partitions '+table[i]
            cursor.execute(sql)
            result=cursor.fetchall()
            extract_partitions(result,i)
            dropsql='alter table '+database[i]+'.'+table[i]+' drop if exists partition('+partition_col[i]+'='
            for x in trunc_date:
                # hdfs_trunc=''
                hdfs_trunc=hdfspath[i]+'/'+partition_col[i]+'='+x
                (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-rm','-r',hdfs_trunc])
                print(str(out)+str('\n')+str(err)+str('\n')+str(ret))

            for x in trunc_date:
                sql=dropsql+'\''+x+'\')'
                try:
                    cursor.execute(sql)
                    print("Partition dropped -> "+x)
                except:
                    print("Partition does not exist -> ",x)
                sql=''

            # trunc_date.clear()
            # print("printing truncate list",trunc_date)
            sql='msck repair table '+database[i]+'.'+table[i]
            trunc_date.clear()
            cursor.execute(sql)
            conn.close()
            print("Hive Connection closed")
        except Exception as err:
            print("Connection failed.\nError:",err)
            conn.close()
            print("Hive Connection closed")

def extract_partitions(result,i):
    parsedate=[]
    strdate=[]
    partitions=list(zip(*result))[0]
    dates = list(map(lambda dat: dat.split('/')[0],list(map(lambda res: res.split('=')[1], partitions))))
    for d in dates:
        res=parsing_date(d)
        parsedate,formatdate=res[0],res[1]
        strdate.append(parsedate.strftime(formatdate))

    strdate.sort(key = lambda date: datetime.strptime(date, formatdate))
    # print(strdate)
    curdate=date.today()
    retentiond=datetime.strftime(curdate-timedelta(retention_duration[i]),formatdate)
    trunc_date.clear()
    for dat in strdate:
        if dat<retentiond:
            trunc_date.append(dat)

def parsing_date(text):
    for fmt in ('%d-%m-%Y', '%Y%m%d', '%d/%m/%Y','%d%m%Y'):
        try:
            return datetime.strptime(text, fmt), fmt
        except ValueError:
            pass
    raise ValueError('Invalid date format found -> '+text)

def run_hdfs_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    try:
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err
    except Exception(FileNotFoundError()):
        pass
    raise  print("Error:")


if __name__=="__main__":
   tablename=[]
   database=[]
   table=[]
   hdfspath=[]
   retention_duration=[]
   partition_col=[]
   trunc_date=[]

   postgres_connect()

   print(database)
   hive_connection()
