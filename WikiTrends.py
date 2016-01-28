import gzip
import io

filePath = 'pagecounts-20110101-000000.gz'
#with io.TextIOWrapper(io.BufferedReader(gzip.open(filePath)), encoding='utf8', errors='ignore') as file:
#    for line in file:
#        print line

from cassandra.cluster import Cluster
#cluster = Cluster(contact_points=['127.0.0.1','127.0.0.2','127.0.0.3'])
master= '54.210.161.212'
cluster = Cluster(contact_points=[master])
session = cluster.connect()

str_cmd = 'CREATE KEYSPACE Excelsior WITH REPLICATION = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 };'

str_cmd = 'CREATE TABLE wikitrends ( text, jour timestamp, heure text, temperature int, PRIMARY KEY ((ville, jour), heure);'
rows = session.execute(str_cmd)

session = cluster.connect('excelsior')
table_str = 'CREATE TABLE pageviews_by_day ( title text, date text, event_time timestamp, pageviews int, PRIMARY KEY ((title, date), event_time));'
rows = session.execute(table_str)

##INSERT
#INSERT INTO temperature_by_day(weatherstation_id,date,event_time,temperature)
#VALUES (’1234ABCD’,’2013-04-03′,’2013-04-03 07:01:00′,’72F’);
m = filePath.split('-')
year = m[1][:4]
month = m[1][4:6]
day = m[1][-2:]
hour = m[2][:2]
date = year+'-'+month+'-'+day
event_time = date+' '+hour+':00:00'

filePath = 'pagecounts-20110101-000000.gz'
insert_req_prefix = 'INSERT INTO pageviews_by_day ( title, date, event_time, pageviews)'


with io.TextIOWrapper(io.BufferedReader(gzip.open(filePath)), encoding='utf8', errors='ignore') as file:
    for line in file:
        words = line.split('\s+')
        title = words[0]+';'+words[1]
        pageviews = int(words[2])
        insert_req_suffix = ' VALUES(%s, %s, %s, %d);' % (title, date, event_time, pageviews)
        insert_req = insert_req_prefix + insert_req_suffix
        print session.execute(insert_req)
## projectcode, pagename, pageviews, bytes
## ${YEAR}/${YEAR}-${MONTH}/pagecounts-${YEAR}${MONTH}${DAY}-${HOUR}0000.gz
## variables:
## YEAR, MONTH, DAY, HOUR, project code, pagename, pageviews, bytes
## objective 1 TOP 100 LAST 30 DAYS
## objective 2 TOP 30 LAST 24 hours

### line MONTH
### line HOUR
### pagename value page view
