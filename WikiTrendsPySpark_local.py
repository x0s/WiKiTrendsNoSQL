'''
PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0,TargetHolding:pyspark-cassandra:0.2.4,datastax:spark-cassandra-connector:1.5.0-RC1-s_2.11 
--jars spark-cassandra-connector-1.5.0-RC1-s_2.11.jar,pyspark_cassandra-0.2.4.jar --driver-class-path pyspark_cassandra-0.2.4.jar 

PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0,TargetHolding:pyspark-cassandra:0.2.4,datastax:spark-cassandra-connector:1.4.1-s_2.10

PYSPARK_DRIVER_PYTHON=ipython dse pyspark --executor-memory 12G --total-executor-cores 36

##pagecount_table
CREATE TABLE wikistats.hrpageviews2 ( title text, year int, month int, day int, hour int, pageviews int, PRIMARY KEY (title, year, month, day, hour)) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);
CREATE TABLE wikistats.dapageviews2 ( title text, year int, month int, day int, agg_pageviews int, PRIMARY KEY (title, year, month, day)) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC);

https://github.com/rustyrazorblade/spark-training/blob/master/exercises-solutions.ipynb

once connected to the master:
initiate bash
	bash
start PYSPARK 
	PYSPARK_DRIVER_PYTHON=ipython dse pyspark --executor-memory 4G --total-executor-cores 36

use %cpaste to paste python code in the interactive python shell

PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark --packages datastax:spark-cassandra-connector:1.5.0-RC1-s_2.10,TargetHolding:pyspark-cassandra:0.2.4 
--jars spark-cassandra-connector-1.5.0-RC1-s_2.11.jar
--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0,TargetHolding:pyspark-cassandra:0.2.4,
datastax:spark-cassandra-connector:1.5.0-RC1-s_2.11


PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark --driver-class-path $(echo /Users/galicher/Documents/NoSQL/spark-1.6.0-bin-hadoop2.6/*.jar |sed 's/ /:/g')

mdp: azer1234

Master ec2-54-175-192-111.compute-1.amazonaws.com
Login: ubuntu

'''
# final static variables 
# stocker les noms des colonnes en static 

from pyspark import SparkConf, SparkContext
from boto.s3.connection import S3Connection
import boto
from boto.s3.key import Key
import boto.s3.connection
import gzip
import io
##from cassandra.cluster import Cluster
import pandas as pd
from pyspark.sql.types import *
import math
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql import  SQLContext
#from pyspark_cassandra import CassandraSparkContext





keyspace = "wikistats"
table = "pagecounts"

''' connection to aws s3'''

def retrieve_bucket():
	conn = boto.s3.connect_to_region('us-east-1',
       aws_access_key_id=AWS_ACCESS_KEY_ID,
       aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
		#       is_secure=True,               # uncommmnt if you are not using ssl
       calling_format = boto.s3.connection.OrdinaryCallingFormat(),
       )
	bucket = conn.get_bucket(Bucketname)
	return bucket

def retrieve_file_list_dict(mybucket):
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
	hadoopConf = sc._jsc.hadoopConfiguration()
	hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
	hadoopConf.set("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY_ID)
	hadoopConf.set("fs.s3.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)

def filter_df(df, language='en', column= 'pageviews'):
   		#stats = df.filter("project_code = '%s'" % language).describe('%s' % column).collect()
   		#threshold = int(round(float(stats[1][1])+3.0*math.sqrt(float(stats[2][1]))))
   		threshold = 100
   		rdf = df.filter("project_code = '%s'" % language).filter('%s > %d' % (column,threshold))
   		return rdf

def preprossessing(file_dict, key_space, name_table):
  counter = 0
  df = None
  sortedfiles = file_dict.keys()
  sortedfiles.sort()
  sortedfiles = sortedfiles[:80]
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
		if counter > 10:
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

def last24hours(df):
  # Defines partitioning specification and ordering specification.
  #windowSpec = \
  #  Window \
  #    .partitionBy(...) \
  #    .orderBy(...)
  # Defines a Window Specification with a ROW frame.
  #windowSpec.rowsBetween(start, end)
  # Defines a Window Specification with a RANGE frame.
  #windowSpec.rangeBetween(start, end)
  #  dataFrame = sqlContext.table("productRevenue")
  #  revenue_difference = \
  #    (func.max(dataFrame['revenue']).over(windowSpec) - dataFrame['revenue'])
  #  dataFrame.select(
  #    dataFrame['product'],
  #    dataFrame['category'],
  #    dataFrame['revenue'],
  #   revenue_difference.alias("revenue_difference"))  

  return


''' main '''
sc.stop()
conf = (SparkConf(True)
         .setMaster("spark://127.0.0.1:7077")
         .setAppName("My app")
         .set("spark.executor.memory", "2g")
         .set("spark.cassandra.connection.host", "localhost"))
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
bucket = retrieve_bucket()
page_count_file_dict = retrieve_file_list_dict(bucket)
setting_aws_context()
#sc._jsc.getConf().set("default:wikistats/spark.cassandra.input.split.size_in_mb","128")
preprossessing(page_count_file_dict, keyspace, table)


sqlContext.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="hrpageviews2", keyspace="wikistats").load().show()


sqlContext.range

dday = datetime.datetime(2011,1,2,4)
delta = datetime.timedelta(hours=-24)
df1 = df.filter(df.event_time <= dday).filter(df.event_time > (dday + delta))
df2 = df1.groupby('title').sum('pageviews').sort('SUM(pageviews)',ascending=False)
df2.take(100)
