'''
Copyright (c) 2016, Aurelien Galicher
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES 
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''


from pyspark import SparkConf, SparkContext
from boto.s3.connection import S3Connection
import boto
from boto.s3.key import Key
import boto.s3.connection
import gzip
import io
import pandas as pd
from pyspark.sql.types import *
import math
from pyspark.sql import  SQLContext
from pyspark.sql.functions import  lit
import matplotlib.pyplot as plt
import datetime as dt

plt.style.use('ggplot')

AWS_ACCESS_KEY_ID = '<your public key>'
AWS_SECRET_ACCESS_KEY = '<your private key>'
Bucketname = 'wikitrendsmsbgd16' 

keyspace = "wikistats"

''' connection to aws s3'''

def retrieve_bucket():
	''' connection to aws s3'''
  conn = boto.s3.connect_to_region('us-east-1',
       aws_access_key_id=AWS_ACCESS_KEY_ID,
       aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
		#       is_secure=True,               # uncommmnt if you are not using ssl
       calling_format = boto.s3.connection.OrdinaryCallingFormat(),
       )
	bucket = conn.get_bucket(Bucketname)
	return bucket

def retrieve_file_list_dict(mybucket):
    ''' storing file names in a dictionary '''
    page_count_file_dict = {}
    for key in mybucket.list(prefix="wikistats/"):
        filename = key.name
        m = filename.split('/')[1].split('-')
        year = m[1][:4]
        month = m[1][4:6]
        day = m[1][-2:]
        hour = m[2][:2]
        date = year+'-'+month+'-'+day
        event_time = date+' '+hour+':00:00		'
        page_count_file_dict.update({ filename.split('/')[1] : { 'year': year,
                                                                'month' : month,
                                                                'day' : day,
                                                                'hour' : hour,
                                                                'date' : date,
                                                                'event_time' : event_time }
                                    })
    return page_count_file_dict

def setting_aws_context():
	''' setting aws configuration in current Spark Context '''
  hadoopConf = sc._jsc.hadoopConfiguration()
	hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
	hadoopConf.set("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY_ID)
	hadoopConf.set("fs.s3.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
  return

def filter_df(df, language='en', column= 'pageviews'):
   ''' filter apply to ingested files '''
  threshold = 50
   rdf = df.filter("project_code = '%s'" % language).filter('%s > %d' % (column,threshold))\
              .filter(~df.title.startswith('Main_Page')).filter(~df.title.startswith('Special:'))
  return rdf

def preprossessing(file_dict, paging=250):
  ''' ingestion of logfiles stored in s3
      files are read from s3 (sc.textFile)
      the rdd is transformed in a Spark DataFrame
      resulting DaatFrame are stacked and written to cassandra in a batch 
      of size set by the parameter paging (default = 250)
  '''
  counter = 0
  df = None
  sortedfiles = file_dict.keys()
  sortedfiles.sort()
  #sortedfiles = sortedfiles[500:]
  for myfile in sortedfiles:
		pagecountRDD = sc.textFile("s3://wikitrendsmsbgd16/wikistats/%s" % myfile)
		pagemapDF = pagecountRDD.map(lambda x: x.split(" ")).toDF(["project_code","title","pageviews","bytes"])
		pagemapDF = filter_df(pagemapDF)
		pagemapDF = pagemapDF.withColumn('event_time', lit(file_dict[myfile]['event_time']))
		pagemapDF = pagemapDF.select(['event_time','title','pageviews'])
		
		print "file n: %d, %s " % (counter, myfile)
		counter +=1
		if df is None:
			df = pagemapDF
		else:
			df = df.unionAll(pagemapDF)
		if counter > paging:
			print "writing df to cassandra: %d" % counter
			df.write\
    		.format("org.apache.spark.sql.cassandra")\
   			.options(keyspace="wikistats", table="hrpageviews")\
   			.save(mode="append")
   			df=None
   			counter = 0
  if df is not None:
    df.write\
    	.format("org.apache.spark.sql.cassandra")\
   		.options(keyspace="wikistats", table="hrpageviews")\
   		.save(mode="append")
  return

def top100rising(event_time, time_delta=24, df=df): 
  ''' 
      compute for the timestamp event_time the top 100 pageviews 
      in the timeframe set by parameter time_delta
      and write the results to cassandra (table top100rising)
  '''
  delta = datetime.timedelta(hours=time_delta)
  df.filter(df.event_time <= event_time)\
        .filter(df.event_time > (event_time - delta))\
        .filter(~df.title.startswith('Main_Page')).filter(~df.title.startswith('Special:'))\
        .groupby('title').sum('pageviews').sort('SUM(pageviews)',ascending=False)\
        .withColumn("event_time", lit(event_time))\
        .withColumnRenamed('SUM(pageviews)','pageviews').limit(100)\
        .select(["event_time","pageviews","title"]).write\
                                              .format("org.apache.spark.sql.cassandra")\
                                              .options(keyspace="wikistats", table="top100rising")\
                                              .save(mode="append")
  return

#CREATE TABLE wikistats.top100trending ( event_time timestamp, pageviews bigint, title text, PRIMARY KEY ((event_time), pageviews));

def top100trending(event_time, time_delta=30, df=df): 
  ''' 
      compute for the date event_time the top 100 pageviews 
      in the timeframe set by parameter time_delta (default = 30 days)
      and write the results to cassandra (table top100rising)
  '''
  delta = datetime.timedelta(days=time_delta)
  print "top100trending %s" % event_time
  df.filter(df.event_time <= event_time)\
        .filter(df.event_time > (event_time - delta))\
        .groupby('title').sum('pageviews').sort('SUM(pageviews)',ascending=False)\
        .filter(~df.title.startswith('Main_Page')).filter(~df.title.startswith('Special:'))\
        .withColumn("event_time", lit(event_time))\
        .withColumnRenamed('SUM(pageviews)','pageviews').limit(100)\
        .select(["event_time","pageviews","title"]).write\
                                              .format("org.apache.spark.sql.cassandra")\
                                              .options(keyspace="wikistats", table="top100trending")\
                                              .save(mode="append")
  return

def createTop100rising():
  ''' 
      apply the function top100rising to all distinct event_time found 
      in table hrpageviews
  '''
  df = sqlContext.sql('select * from wikistats.hrpageviews')
  df.cache()
  df6 = df.select(df.event_time).distinct()
  p_df = df6.toPandas()
  p_df.event_time.apply(lambda x: top100rising(pd.to_datetime(x).to_datetime(), 24, df))
  return
#CREATE TABLE wikistats.dapageviews ( event_time timestamp, title text, pageviews int, PRIMARY KEY ((event_time), title);

def populateDapageviews():
  ''' 
      compute the aggregate per day of table hrpageviews 
      and store the result in table dapageviews 
  '''
  df = sqlContext.sql('select * from wikistats.hrpageviews')
  rdd = df.map(lambda x: (x.event_time.date(), x.title, x.pageviews))
  df_t = sqlContext.createDataFrame(rdd, ['event_time', 'title', 'pageviews'])
  df_t = df_t.groupby(['event_time','title']).sum('pageviews').withColumnRenamed('SUM(pageviews)','pageviews')
  df_t.write\
            .format("org.apache.spark.sql.cassandra")\
            .options(keyspace="wikistats", table="dapageviews")\
            .save(mode="append")
  return

#CREATE TABLE wikistats.top100rising ( event_time timestamp, pageviews bigint, title text, PRIMARY KEY ((event_time), title));
def createTop100trending():
  ''' 
      apply the function top100trending to all distinct date found 
      in table dapageviews
  '''
  df = sqlContext.sql('select * from wikistats.dapageviews')
  df.cache()
  df6 = df.select(df.event_time).distinct()
  p_df = df6.toPandas()
  p_df.event_time.apply(lambda x: top100trending(pd.to_datetime(x).to_datetime(), 30, df))
  return

def write_results_to_s3():
    top100risingdf = sqlContext.sql('select * from wikistats.top100rising')
    top100risingdf.toPandas().to_csv('top100rising.csv')
    top100trendingdf = sqlContext.sql('select * from wikistats.top100trending')
    top100trendingdf.toPandas().to_csv('top100trending.csv')
    top100risinghistdf = sqlContext.sql('select * from wikistats.top100risinghist')
    top100risinghistdf.toPandas().to_csv('top100risinghist.csv')
    top100trendinghistdf.toPandas = sqlContext.sql('select * from wikistats.top100trendinghist')
    top100trendinghistdf.toPandas().to_csv('top100trendinghist.csv')
    return

def top100risinghist(event_time, time_delta, df, df_top100): 
  ''' 
      retrieving past values in range time_delta of items present in top100rising
      writing result to top100risinghist
  '''
  delta = datetime.timedelta(hours=time_delta+1)
  event_time_inf = event_time - delta
  df_windows = df.filter(df.event_time <= event_time).filter(df.event_time >= event_time_inf)
  df_windows_history = df_windows.withColumn("refhour", lit(event_time))
  dwh =  df_windows_history.alias('dwh')
  dt1 =  df_top100.alias('dt1').filter(df_top100.event_time ==  event_time).select('title')
  df_top100_hist = dwh.join(df_top100, dwh.title == dt1.title, 'inner')\
                      .select(["refhour", "dwh.title","dwh.event_time", "dwh.pageviews"])\
                      .distinct().sort('title')
  df_top100_hist.write\
                  .format("org.apache.spark.sql.cassandra")\
                  .options(keyspace="wikistats", table="top100risinghist")\
                  .save(mode="append")

def createTop100risinghist():
  ''' 
      apply the function top100risinghist to all distinct event_time found 
      in table hrpageviews
  '''
  df = sqlContext.sql('select * from wikistats.hrpageviews')
  df_top100 = sqlContext.sql('select * from wikistats.top100rising')
  df.cache()
  df_top100.cache()
  df6 = df.select(df.event_time).distinct().sort(df.event_time, ascending=False)
  p_df = df6.toPandas()
  p_df.event_time.apply(lambda x: top100risinghist(pd.to_datetime(x).to_datetime(), 24, df, df_top100))
  return

def top100trendinghist(event_time, time_delta, df, df_top100): 
  ''' 
      retrieving past values in range time_delta of items present in top100trending
      writing result to top100trendinghist
  '''
  delta = datetime.timedelta(days=time_delta+1)
  event_time_inf = event_time - delta
  df_windows = df.filter(df.event_time <= event_time).filter(df.event_time >= event_time_inf)
  df_windows_history = df_windows.withColumn("refhour", lit(event_time))
  dwh =  df_windows_history.alias('dwh')
  dt1 =  df_top100.alias('dt1').filter(df_top100.event_time ==  event_time).select('title')
  df_top100_hist = dwh.join(df_top100, dwh.title == dt1.title, 'inner')\
                      .select(["refhour", "dwh.title","dwh.event_time", "dwh.pageviews"])\
                      .distinct().sort('title')
  df_top100_hist.write\
                  .format("org.apache.spark.sql.cassandra")\
                  .options(keyspace="wikistats", table="top100trendinghist")\
                  .save(mode="append")
  return

def createTop100trendinghist():
  ''' 
      apply the function top100trendinghist to all distinct dates found 
      in table dapageviews
  '''
  df = sqlContext.sql('select * from wikistats.dapageviews')
  df_top100 = sqlContext.sql('select * from wikistats.top100trending')
  df.cache()
  df_top100.cache()
  df6 = df.select(df.event_time).distinct().sort(df.event_time, ascending=False)
  p_df = df6.toPandas()
  p_df.event_time.apply(lambda x: top100trendinghist(pd.to_datetime(x).to_datetime(), 30, df, df_top100))
  return

def write_results_to_csv():
    top100risingdf = sqlContext.sql('select * from wikistats.top100rising')
    top100risingdf.toPandas().to_csv('top100rising.csv')
    top100trendingdf = sqlContext.sql('select * from wikistats.top100trending')
    top100trendingdf.toPandas().to_csv('top100trending.csv')
    top100risinghistdf = sqlContext.sql('select * from wikistats.top100risinghist')
    top100risinghistdf.toPandas().to_csv('top100risinghist.csv')
    top100trendinghistdf.toPandas = sqlContext.sql('select * from wikistats.top100trendinghist')
    top100trendinghistdf.toPandas().to_csv('top100trendinghist.csv')
    return


def dataviz_rising(str_time, n=100):
  '''
      vizualition in matplotlib to TOP100 rising for a given hour
      on the left panel to the top n values as bart chart
      on the right panel n charts of historical values for the last 24 hours 
  '''
  df = sqlContext.sql("select * from wikistats.top100rising")
  pdf = df.toPandas()
    #print (pdf)
  pdf_event_time = pdf[pdf.event_time == str_time]\
                      .sort_values('pageviews', ascending = False).head(100)

  df = sqlContext.sql("select * from wikistats.top100risinghist")
  times_deriesdf = df.filter(df.refhour == str_time)
  times_deriesdf_pd = times_deriesdf.toPandas()

  #print (pdf_event_time)
  pdf_event_time= pdf[pdf.event_time == '%s' % str_time].groupby('title').pageviews.sum().sort_values(ascending=True)

  #n = 100
  plt.close('all')

  fig = plt.figure(figsize=(16,n/20*32))
  plt.title('TOP %d Rising %s' % (n, str_time))

  ax = fig.add_subplot(1,2,1)
  x = pdf_event_time.index.values
  y = pdf_event_time.values
  height = 0.75
  ax.barh(np.arange(len(y[:n])), y[:n], height , color = 'red', label = x)
  
  ax.set_yticks(np.arange(len(y[:n])))
  labels = [ x[i] + ': %d views' % y[i] for i in range(n)]
  rects = ax.patches
  for rect, label in zip(rects, labels):
      width = rect.get_width()
      height = rect.get_height()
      ax.text(width/10, rect.get_y() + height / 2, label, ha='left', va='center')

  for i in range(0,n):
      ax = fig.add_subplot(n,2,2+i*2)
      y = times_deriesdf_pd[times_deriesdf_pd.title == x[i]].sort_values('event_time', ascending=True)['pageviews'].values
      plt.fill_between(range(len(y)), 0, y, facecolor='orange', alpha=0.5)
      ax.xaxis.set_ticklabels([])
      ax.yaxis.set_ticklabels([])

  plt.tight_layout()
  plt.savefig('TOP_%d_Rising_%s.png' % (n,str_time))
  plt.show()

  return

def dataviz_trending(str_time, n=100):
  '''
      vizualition in matplotlib to TOP100 trending for a given date
      on the left panel to the top n values as bart chart
      on the right panel n charts of historical values for the last 30 days 
  '''
  df = sqlContext.sql("select * from wikistats.top100trending")
  pdf = df.toPandas()
    #print (pdf)
  pdf_event_time = pdf[pdf.event_time == str_time]\
                      .sort_values('pageviews', ascending = False).head(100)

  df = sqlContext.sql("select * from wikistats.top100trendinghist")
  times_deriesdf = df.filter(df.refhour == pd.to_datetime(str_time))
  times_deriesdf_pd = times_deriesdf.toPandas()
  pdf_event_time= pdf[pdf.event_time == '%s' % str_time].groupby('title').pageviews.sum().sort_values(ascending=True)

  plt.close('all')
  fig = plt.figure(figsize=(16,n/20*32))
  plt.title('TOP %d Trending %s' % (n, str_time))

  ax = fig.add_subplot(1,2,1)
  x = pdf_event_time.index.values
  y = pdf_event_time.values
  height = 0.75
  ax.barh(np.arange(len(y[:n])), y[:n], height , color = 'red', label = x)
  ax.set_yticks(np.arange(len(y[:n])))
  labels = [ x[i] + ': %d views' % y[i] for i in range(n)]
  rects = ax.patches
  for rect, label in zip(rects, labels):
      width = rect.get_width()
      height = rect.get_height()
      ax.text(width/10, rect.get_y() + height / 2, label, ha='left', va='center')

  for i in range(0,n):
      ax = fig.add_subplot(n,2,2+i*2)
      y = times_deriesdf_pd[times_deriesdf_pd.title == x[i]].sort_values('event_time', ascending=True)['pageviews'].values
      plt.fill_between(range(len(y)), 0, y, facecolor='orange', alpha=0.5)
      ax.xaxis.set_ticklabels([])
      ax.yaxis.set_ticklabels([])

  plt.tight_layout()
  plt.savefig('TOP_%d_Trending_%s.png' % (n,str_time))
  plt.show()

  return

''' main sequence'''
#1
bucket = retrieve_bucket()
#2
page_count_file_dict = retrieve_file_list_dict(bucket)

#3
setting_aws_context()
preprossessing(page_count_file_dict)
#4
populateDapageviews()
#5
createTop100rising()
#6
createTop100trending()
#7
createTop100risinghist()
#8
createTop100trendinghist()
#9
write_results_to_csv()
