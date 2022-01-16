# -*- coding: utf-8 -*-
"""
Created on Mon Jun 21 16:12:28 2021

@author: rahul.das
"""

# importing libraries
import subprocess
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import psycopg2 as pg
# import sys

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

def postgres_connect():
    try:
        conn = pg.connect(database = "rahuldb", user = "rahuldb", password = "rahuldb", host = "10.113.144.181", port = "5432")
        print("Opened Postgres database successfully")
        cur=conn.cursor()
        cur.execute("select  feed_name, hdfs_path, retention_period from data_retention_merge where rawzone_prune_fl=true order by id")
        tblrows=cur.fetchall()
        print("Closing Postgres connection!!")
        conn.close()
        for row in tblrows:
            feedname.append(row[0])
            hdfspath.append(row[1])
            retention_duration.append(row[2])
    except Exception as err:
        print("Connection error: Postgres DB Connection not established!!\n",err)
        conn.close()
        print("Closing Postgres connection!!")
        exit()

def purge_raw_zone():
   for i in range(len(hdfspath)):
      print("Retention for feed: "+feedname[i])
      path="/".join(hdfspath[i].split("/")[:len(hdfs_raw_zone.split("/")[:-1])])+"/"
      if ((len(hdfspath[i])>len(hdfs_raw_zone)) and (hdfs_raw_zone == path) and len(path)==len(hdfs_raw_zone)):
            print('Valid raw zone path')
      else:
            print('Invalid raw zone path!! Please check your path')
            continue


      (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-ls',hdfspath[i]])
      # print(str(out)+str('\n')+str(err)+str('\n')+str(ret))
      paths=out
      formatd='%Y-%m-%d %H:%M'
      curdate=datetime.today()
      print("Current DateTime:"+str(curdate))
      retentiond=datetime.strftime(curdate-timedelta(retention_duration[i]),formatd)
      print("Retention Date:"+str(retentiond))
      test=paths.decode()

      # table paths
      all_paths=[x for x in test.split() if x.startswith('/')]
      print("\nAll paths found in source path:")
      print(str(all_paths)+'\n')

      for x in all_paths:
         # checking if inside table path directory exists or files
         (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-test','-d',x])
         # 1 - file; 0-directory
         if ret==1:
            (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-stat','%y',x])
            # print(str(out)+str('\n')+str(err)+str('\n')+str(ret))
            datezone=':'.join((out.decode().split('\n')[0]).split(':')[0:2])
            pdate=datetime.strptime(datezone, formatd)+relativedelta(hours=5,minutes=30)
            date=datetime.strftime(pdate,formatd)
            if datetime.strptime(date, formatd) < datetime.strptime(retentiond, formatd):
               #print("xp")
               #print(date)
               (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-rm','-r',x])
               print(str(out)+str('\n')+str(err)+str('\n')+str(ret))
               print("File is deleted")
               print(ret)
            else:
               print("File is retained")
         else:
            # Inside table path for all partitions
            partdate=x.split("/")[-1]
            print(partdate)
            retention=datetime.strftime(datetime.strptime(retentiond, formatd),'%Y-%m-%d')
            try:
                if datetime.strptime(partdate, '%Y-%m-%d') < datetime.strptime(retention, '%Y-%m-%d'):
                   print("Partition to be dropped -> "+str(partdate))
                   (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-rm','-r',x])
                   print(str(out)+str('\n')+str(err)+str('\n')+str(ret))
                   #print(ret)
                else:
                   print("Partition retained -> "+str(partdate)+"\n")
            except:
                print("Invalid date found")
            
            
            # (retp,outp,errp)=run_hdfs_cmd(['hdfs','dfs','-ls','-t',x])
            # # print(str(outp)+str('\n')+str(errp)+str('\n')+str(retp))
            # # print(str(outp)+'\n')
            # dpaths=outp.decode()
            # pathPartitions=[x for x in dpaths.split() if x.startswith('/')]
            # print("\nAll paths found in table "+(x.split('/')[-1])+":")
            # print(pathPartitions)
            # print('\n')
            # for p in pathPartitions:
            #    (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-test','-d',p])
            #    # 1 - file; 0-directory
            #    if ret==1:
            #       print("Checking the timestamp for file:"+p)
            #       (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-stat','%y',p])
            #       # print(str(ret)+str('\n')+str(err)+str('\n'))
            #       print("Timestamp of file :"+str(out)+"\n")
            #       datezone=':'.join((out.decode().split('\n')[0]).split(':')[0:2])
            #       pdate=datetime.strptime(datezone, formatd)+relativedelta(hours=hours,minutes=minutes)
            #       date=datetime.strftime(pdate,formatd)
            #       if datetime.strptime(date, formatd) < datetime.strptime(retentiond, formatd):
            #          print("Purging Files")
            #          print("Partition to be dropped -> "+str(date)+'\n')
            #          (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-rm','-r',p])
            #          print(str(out)+str('\n')+str(err)+str('\n')+str(ret))
            #          print(ret)
            #       else:
            #          print("File is retained:"+str(p.split('/')[-1])+'\n')
            #    elif ret==0:
            #       # Inside partition paths
            #       partdate=p.split("/")[-1]
            #       print(partdate)
            #       retention=datetime.strftime(datetime.strptime(retentiond, formatd),'%Y-%m-%d')
            #       try:
            #           if datetime.strptime(partdate, '%Y-%m-%d') < datetime.strptime(retention, '%Y-%m-%d'):
            #              print("Partition to be dropped -> "+str(partdate))
            #              (ret,out,err)=run_hdfs_cmd(['hdfs','dfs','-rm','-r',p])
            #              print(str(out)+str('\n')+str(err)+str('\n')+str(ret))
            #              #print(ret)
            #           else:
            #              print("Partition retained -> "+str(partdate)+"\n")
            #       except:
            #           print("Invalid date found")

if __name__=="__main__":
   database=[]
   feedname=[]
   table=[]
   hdfspath=[]
   retention_duration=[]
   # default raw zone path
   hdfs_raw_zone="/home/nifi/edl/raw_zone/"
   # Timezone
   hours=4
   minutes=00
#   t1l058.mtnirancell.ir
#   log_path='/home/subexuser/rahul/'
#   filename=log_path+'raw_data_retention_'+datetime.now().strftime('%Y%m%d')+'.log'
#   log_file=open(filename,"at",encoding="utf-8")
#   sys.stdout = log_file
   print("Run commenced at:"+str(datetime.now()))
   postgres_connect()
   purge_raw_zone()
   print("Run terminated at:"+str(datetime.now()))
   print("<--------------------------------------->\n")
#   sys.stdout = old_stdout
#   log_file.close()


