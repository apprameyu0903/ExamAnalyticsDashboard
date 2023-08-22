#!/usr/bin/env python
# coding: utf-8

# In[1]:


from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


# In[2]:


import sys
# sys.path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
##!jupyter nbconvert <notebook name>.ipynb --to html

#!pip3 install pandas==0.20.3
import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns


import pymongo
import json
from pymongo import MongoClient
import time

pd.options.display.max_colwidth = 500
pd.options.display.max_columns = 51


# In[3]:


# ###sudo update-ca-certificates
ES_CERTIFICATES = {'ca_certs':'/usr/local/share/ca-certificates/elastic1/ca.crt','client_certs':'/usr/local/share/ca-certificates/elastic1/logclient.crt','client_key':'/usr/local/share/ca-certificates/elastic1/logclient.key'}

elastic_client = Elasticsearch(host='iafems.cdacchn.in', port=9200, verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=('elastic','34ERdfcv@34'),timeout=300)


# In[4]:


type(elastic_client)


# In[53]:


##gaierror: [Errno -2] Name or service not known /etc/hosts
###10.184.53.57    iafems.cdacchn.in
alias = []
# alias.extend(elastic_client.indices.get_alias('iafstar-june-testing'))

# alias.extend(elastic_client.indices.get_alias('iafstar-june-test-ans'))
alias.extend(elastic_client.indices.get_alias('iafafcat-aug-ans-test-20210829'))

alias


# In[52]:


alias_ans = []
# alias_ans.extend(elastic_client.indices.get_alias('iafstar-june-test-ans'))
alias_ans.extend(elastic_client.indices.get_alias('iafafcat-aug-ans-test-20210829'))

alias_ans


# In[7]:


from urllib.parse import quote
################ MONGODB conaction details
# mongod --bind_ip 10.184.51.194 --dbpath /var/lib/mongodb/

# connection = MongoClient("mongodb://10.184.51.194:27017")
connection = MongoClient("mongodb://10.184.61.202:27017")


# mongo_uri = "mongodb://iaf:" + quote("cdac123!@#") + "@10.184.61.202:27017"
# # connection = MongoClient('mongodb://iaf:cdac123!@#@10.184.51.194:27017')
# connection = MongoClient(mongo_uri, authSource="admin")
# db=connection['star']
db=connection['afcat21Aug']


# In[8]:


db


# ## 1. #### getting minimum time  to Query
# 
# #### points to obe noted
# - 1. minmum time of each use case need to recorded 
# - 2. query to fetch data from ELK shoud be based on each use-case "string patern" and "minimum time" noted above
# - 3. **EXECUTE** only required block for every minutes may be use-casess block

# In[30]:


##List of region and centre associated with it
Jaipur = [
  '143', '123', '128',
  '202', '203', '204',
  '214', '215', '216', '259'
]

## list all the centre here, execute one by one
center_list =[]


# In[49]:


# print("Unique Center : ", len(results_df['centercode'].unique() ))
# print("UNIQUE CANDIDATE : ", len(results_df['candidateid'].unique() ))
# results_df.shape


# In[24]:


1000 * 20
ans_df['anomaly'].uniqueque()


# ## TEST wit ROLLING

# In[77]:


# import numpy as np
# # results_df.head(1)
# # ans_df.head(1)
# from dateutil.parser import parse
# eventedate_date = [parse(d) for d in results_df['eventdate'] ]
# results_df['eventdate_date'] = eventedate_date
# results_df = results_df.sort_values(['candidateid', 'eventdate'], ascending=True)

# # results_df.groupby('column8')['eventdate_date'].diff()

# results_df['timeToAns'] = results_df.groupby('column8')['eventdate_date'].diff() /  np.timedelta64(1, 's')


# In[57]:


######################### EXAM TIME for EACH SLOT
import pytz
from datetime import datetime
from pytz import timezone

stime = time.time()

print(" STRAT TIME ::", time.ctime())
    
    
# ## yaer, month, date, hour, min, sec, timezone
# unaware = datetime(2020, 10, 5, 14, 45, 0, 0)
# # exam_time = pytz.utc.localize(unaware)
# # exam_time
# localtz = timezone('Asia/Kolkata')
# exam_time = localtz.localize(unaware)
# # exam_time

#1 PM IST = 7:30 Z time
#1:30 IST = 8:00 Z time

#4:15 PM = 10:45 Z time 
#4:45 PM = 11:15 Z time 

        
######################### DATA LOG CRAWLING DATE & TIME
                         
delyaed_min_login_time = '2021-08-29T09:40:35.000Z'
delyaed_login_time_min = '2021-08-29T10:59:35.000Z'

# delyaed_min_login_time = '2021-08-29T04:15:35.000Z'
# delyaed_login_time_min = '2021-08-29T04:30:35.000Z'


# delyaed_min_login_time = '2021-02-10T10:00:00.000Z'
### First time it will start from user given TIME, if empty current time will be taken
if delyaed_min_login_time == '':
    now = datetime.now()
    delyaed_login_time = now.strftime("%d/%m/%Y %H:%M:%S")
else:
    delyaed_login_time = delyaed_min_login_time
print(delyaed_login_time)
############## Extract Center-wise make paper available data from ELK
ress= Search(using = elastic_client,  index=alias )
# ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
# ress = ress.query('range', **{'eventdate': {'gte': '2020-03-09T00:00:00.000Z'}})

ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time, 'lte':  delyaed_login_time_min }})
print( "data ",  ress.count())
##change 1 #added
# delyaed_min_login_time = '2020-11-04T3:50:00.000Z'
# delyaed_min_login_time_l = '2020-11-04T11:00:00.000Z'
# ress = ress.query('range', **{'eventdate': {'lte':  delyaed_min_login_time_l}})

ress = ress.query('match', **{'msg':'submitAnswer'})
# ress = ress.query('match', **{'papercode':'XY'})

print( "message ", ress.count())
#     res = ress.query('match', **{'makePaperSectionAvailable'})
ress = ress.query('match', **{'examslot':'D'})
# ress = ress.query('match', **{'msg':'submitAnswer'})
print( "slot ", ress.count())

# ress = ress.query('match', **{'centercode':'400'})
# ress = ress.query('match', **{'papercode':'XY'})
print( "paper code ", ress.count())
# res = ress


# res = res.query('match', **{'examslot':'C'})
cnt = ress.count()
rst = ress.execute()
print("COUNT", cnt)


############# Converting into DATAFRAME
type(rst.hits.hits)
# rst_df = pd.io.json.json_normalize(rst.hits.hits)


import datetime as dt
from dateutil.parser import parse


import time
start_time = time.time()
print("start time : ", start_time)
results_df = pd.DataFrame(d.to_dict() for d in ress.scan())
print(results_df.shape)
print("time for completion : ",  time.time() - start_time )
print("Data Frame ", results_df.head())

print("Unique Center : ", len(results_df['centercode'].unique() ))
print("UNIQUE CANDIDATE : ", len(results_df['candidateid'].unique() ))



results_df.columns
results_df.head(1)
#### set Minimum Time to crawl NEXT time
delyaed_min_login_time = results_df['eventdate'].min()


ans_df = results_df.copy()


print("ans_df : ", ans_df.shape)
print("ans_df : ", ans_df['centercode'].unique() )



import numpy as np
# results_df.head(1)
# ans_df.head(1)
from dateutil.parser import parse
eventedate_date = [parse(d) for d in results_df['eventdate'] ]
results_df['eventdate_date'] = eventedate_date
results_df = results_df.sort_values(['candidateid', 'eventdate'], ascending=True)

results_df = results_df.loc[(results_df['msg'].notnull() & results_df['msg'].str.contains("submitAnswer") ) ]#, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]


# results_df.groupby('column8')['eventdate_date'].diff()

# results_df['timeToAns'] = results_df.groupby('column8')['eventdate_date'].diff() /  np.timedelta64(1, 's')
results_df['timeToAns'] = results_df.groupby('candidateid')['eventdate_date'].diff() /  np.timedelta64(1, 's')

ans_df = results_df.copy()

ans_df = ans_df[['papercode','column6', 'candidateid', 'sessionid',  'question_new', 'questionset','examslot','centercode','eventdate','timeToAns']].sort_values(by=['candidateid','eventdate'], ascending=True)

## find the avge time for any 10 guestions, in this case first 9 question mean/average time would be null. 
# df[df['b'].notnull()]
ans_df['avgTime']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(15)['timeToAns'].mean().droplevel(level='candidateid')

ans_df['avgTime_ten']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(10)['timeToAns'].mean().droplevel(level='candidateid')

# df['moving'] = df.groupby(['col_1', 'col_2', 'col_3']).rolling(10)['value'].mean().droplevel(level=[0,1,2])
# rdf.groupby(['no']).rolling(5)['s'].mean()

## 15 questoin answerd with in 3 min(180 sec) / mean time is 1 sec., then i considered as a ANAMOLY 
ans_df.loc[ (ans_df.avgTime <= 12),  'anomaly' ] = 1
ans_df.loc[ (ans_df.avgTime > 12),  'anomaly' ] = 0

## 10 questoin answerd with in 2 min(120 sec) / mean time is 1 sec., then i considered as a ANAMOLY 
ans_df.loc[ (ans_df.avgTime_ten <= 12),  'anomaly_ten' ] = 1
ans_df.loc[ (ans_df.avgTime_ten > 12),  'anomaly_ten' ] = 0

## Convert UST to IST
isttime = [ date.astimezone(timezone('Asia/Kolkata')) for date in  pd.to_datetime(ans_df['eventdate'])]
ans_df['eventdate_ist'] = isttime
st = time.time()
ans_df.eventdate_ist =  ans_df.eventdate_ist.astype(str)


# ########### inserting into MongoDB database
# record1 = db['qpattern']
# # ##drop
# db.anomaly.drop()
# # ##Insert
records = json.loads(ans_df.T.to_json()).values()
db.anomaly.insert_many(records)
print("ANOMALY DETECTION : DONE")
print("time to complete function : ", time.time() - stime)

consolidate_df = ans_df.loc[ans_df.anomaly==1, ('examslot','centercode','candidateid') ]
consolidate_df['status'] = 'fast answering'
consolidate_df = consolidate_df[['examslot','centercode','candidateid','status']]
records = json.loads(consolidate_df.T.to_json()).values()
db.consolidate.insert_many(records)
print("CONSOLIDATE ::: ANOMALY DETECTION : DONE")
print("time to complete function : ", time.time() - stime)


# In[ ]:



consolidate_df = ans_df.loc[ans_df.anomaly==1, ('examslot','centercode','candidateid') ]
consolidate_df['status'] = 'fast answering'
consolidate_df = consolidate_df[['examslot','centercode','candidateid','status']]
records = json.loads(consolidate_df.T.to_json()).values()
db.consolidate.insert_many(records)
print("CONSOLIDATE ::: ANOMALY DETECTION : DONE")
print("time to complete function : ", time.time() - stime)


# In[29]:


ans_df


# In[83]:


pd.set_option("display.max_columns", 1000)

ans_df.head(2)

results_df = results_df.loc[(results_df['msg'].notnull() & results_df['msg'].str.contains("submitAnswer") ) ]#, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]


# In[69]:




results_df.columns
results_df.head(1)
#### set Minimum Time to crawl NEXT time
delyaed_min_login_time = results_df['eventdate'].min()


ans_df = results_df.copy()


print("ans_df : ", ans_df.shape)
print("ans_df : ", ans_df['centercode'].unique() )



import numpy as np
# results_df.head(1)
# ans_df.head(1)
from dateutil.parser import parse
eventedate_date = [parse(d) for d in results_df['eventdate'] ]
results_df['eventdate_date'] = eventedate_date
results_df = results_df.sort_values(['candidateid', 'eventdate'], ascending=True)

results_df = results_df.loc[(results_df['msg'].notnull() & results_df['msg'].str.contains("submitAnswer") ) ]#, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]


# results_df.groupby('column8')['eventdate_date'].diff()

# results_df['timeToAns'] = results_df.groupby('column8')['eventdate_date'].diff() /  np.timedelta64(1, 's')
results_df['timeToAns'] = results_df.groupby('candidateid')['eventdate_date'].diff() /  np.timedelta64(1, 's')

ans_df = results_df.copy()

ans_df = ans_df[['papercode','column6', 'candidateid', 'sessionid',  'question_new', 'questionset','examslot','centercode','eventdate','timeToAns']].sort_values(by=['candidateid','eventdate'], ascending=True)

## find the avge time for any 10 guestions, in this case first 9 question mean/average time would be null. 
# df[df['b'].notnull()]
ans_df['avgTime']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(15)['timeToAns'].mean().droplevel(level='candidateid')

ans_df['avgTime_ten']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(10)['timeToAns'].mean().droplevel(level='candidateid')

# df['moving'] = df.groupby(['col_1', 'col_2', 'col_3']).rolling(10)['value'].mean().droplevel(level=[0,1,2])
# rdf.groupby(['no']).rolling(5)['s'].mean()

## 15 questoin answerd with in 3 min(180 sec) / mean time is 1 sec., then i considered as a ANAMOLY 
ans_df.loc[ (ans_df.avgTime <= 12),  'anomaly' ] = 1
ans_df.loc[ (ans_df.avgTime > 12),  'anomaly' ] = 0

## 10 questoin answerd with in 2 min(120 sec) / mean time is 1 sec., then i considered as a ANAMOLY 
ans_df.loc[ (ans_df.avgTime_ten <= 12),  'anomaly_ten' ] = 1
ans_df.loc[ (ans_df.avgTime_ten > 12),  'anomaly_ten' ] = 0

## Convert UST to IST
isttime = [ date.astimezone(timezone('Asia/Kolkata')) for date in  pd.to_datetime(ans_df['eventdate'])]
ans_df['eventdate_ist'] = isttime
st = time.time()
ans_df.eventdate_ist =  ans_df.eventdate_ist.astype(str)


# ########### inserting into MongoDB database
# record1 = db['qpattern']
# # ##drop
# db.anomaly.drop()
# # ##Insert
records = json.loads(ans_df.T.to_json()).values()
db.anomaly.insert_many(records)
print("ANOMALY DETECTION : DONE")
print("time to complete function : ", time.time() - stime)

consolidate_df = ans_df.loc[ans_df.anomaly==1, ('examslot','centercode','candidateid') ]
consolidate_df['status'] = 'fast answering'
consolidate_df = consolidate_df[['examslot','centercode','candidateid','status']]
records = json.loads(consolidate_df.T.to_json()).values()
db.consolidate.insert_many(records)
print("CONSOLIDATE ::: ANOMALY DETECTION : DONE")
print("time to complete function : ", time.time() - stime)


# In[75]:


ans_df.head()


# In[47]:


######################### EXAM TIME for EACH SLOT
import pytz
from datetime import datetime
from pytz import timezone

stime = time.time()

print(" STRAT TIME ::", time.ctime())
    
    
# ## yaer, month, date, hour, min, sec, timezone
# unaware = datetime(2020, 10, 5, 14, 45, 0, 0)
# # exam_time = pytz.utc.localize(unaware)
# # exam_time
# localtz = timezone('Asia/Kolkata')
# exam_time = localtz.localize(unaware)
# # exam_time

#1 PM IST = 7:30 Z time
#1:30 IST = 8:00 Z time

#4:15 PM = 10:45 Z time 
#4:45 PM = 11:15 Z time 

        
######################### DATA LOG CRAWLING DATE & TIME
                         
delyaed_min_login_time = '2021-07-10T03:45:35.000Z'
delyaed_login_time_min = '2021-07-09T07:00:35.000Z'


# delyaed_min_login_time = '2021-02-10T10:00:00.000Z'
### First time it will start from user given TIME, if empty current time will be taken
if delyaed_min_login_time == '':
    now = datetime.now()
    delyaed_login_time = now.strftime("%d/%m/%Y %H:%M:%S")
else:
    delyaed_login_time = delyaed_min_login_time
print(delyaed_login_time)
############## Extract Center-wise make paper available data from ELK
ress= Search(using = elastic_client,  index=alias )
# ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
# ress = ress.query('range', **{'eventdate': {'gte': '2020-03-09T00:00:00.000Z'}})

ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time, 'lte':  delyaed_login_time_min }})

##change 1 #added
# delyaed_min_login_time = '2020-11-04T3:50:00.000Z'
# delyaed_min_login_time_l = '2020-11-04T11:00:00.000Z'
# ress = ress.query('range', **{'eventdate': {'lte':  delyaed_min_login_time_l}})

# ress = ress.query('match', **{'msg':'submitAnswer'})

#     res = ress.query('match', **{'makePaperSectionAvailable'})
ress = ress.query('match', **{'examslot':'A'})
# ress = ress.query('match', **{'msg':'submitAnswer'})

# ress = ress.query('match', **{'centercode':'400'})
# res = ress.query('match', **{'papercode':'YY'})

res = ress


# res = res.query('match', **{'examslot':'C'})
cnt = res.count()
rst = res.execute()
print("COUNT", cnt)


############# Converting into DATAFRAME
type(rst.hits.hits)
# rst_df = pd.io.json.json_normalize(rst.hits.hits)


import datetime as dt
from dateutil.parser import parse


import time
start_time = time.time()
print("start time : ", start_time)
results_df = pd.DataFrame(d.to_dict() for d in res.scan())
print(results_df.shape)
print("time for completion : ",  time.time() - start_time )
print("Data Frame ", results_df.head())

print("Unique Center : ", len(results_df['centercode'].unique() ))
print("UNIQUE CANDIDATE : ", len(results_df['candidateid'].unique() ))

results_df.columns
results_df.head(1)
#### set Minimum Time to crawl NEXT time
delyaed_min_login_time = results_df['eventdate'].min()


ans_df = results_df.copy()


print("ans_df : ", ans_df.shape)
print("ans_df : ", ans_df['centercode'].unique() )



import numpy as np
# results_df.head(1)
# ans_df.head(1)
from dateutil.parser import parse
eventedate_date = [parse(d) for d in results_df['eventdate'] ]
results_df['eventdate_date'] = eventedate_date
results_df = results_df.sort_values(['candidateid', 'eventdate'], ascending=True)

results_df = results_df.loc[(results_df['msg'].notnull() & results_df['msg'].str.contains("submitAnswer") ) ]#, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]


# results_df.groupby('column8')['eventdate_date'].diff()

# results_df['timeToAns'] = results_df.groupby('column8')['eventdate_date'].diff() /  np.timedelta64(1, 's')
results_df['timeToAns'] = results_df.groupby('candidateid')['eventdate_date'].diff() /  np.timedelta64(1, 's')

ans_df = results_df.copy()

ans_df = ans_df[['papercode','column6', 'candidateid', 'sessionid',  'question_new', 'questionset','examslot','centercode','eventdate','timeToAns']].sort_values(by=['candidateid','eventdate'], ascending=True)

## find the avge time for any 10 guestions, in this case first 9 question mean/average time would be null. 
# df[df['b'].notnull()]
ans_df['avgTime']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(15)['timeToAns'].mean().droplevel(level='candidateid')

ans_df['avgTime_ten']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(10)['timeToAns'].mean().droplevel(level='candidateid')

# df['moving'] = df.groupby(['col_1', 'col_2', 'col_3']).rolling(10)['value'].mean().droplevel(level=[0,1,2])
# rdf.groupby(['no']).rolling(5)['s'].mean()

## 15 questoin answerd with in 3 min(180 sec) / mean time is 1 sec., then i considered as a ANAMOLY 
ans_df.loc[ (ans_df.avgTime <= 12),  'anomaly' ] = 1
ans_df.loc[ (ans_df.avgTime > 12),  'anomaly' ] = 0

## 10 questoin answerd with in 2 min(120 sec) / mean time is 1 sec., then i considered as a ANAMOLY 
ans_df.loc[ (ans_df.avgTime_ten <= 12),  'anomaly_ten' ] = 1
ans_df.loc[ (ans_df.avgTime_ten > 12),  'anomaly_ten' ] = 0

## Convert UST to IST
isttime = [ date.astimezone(timezone('Asia/Kolkata')) for date in  pd.to_datetime(ans_df['eventdate'])]
ans_df['eventdate_ist'] = isttime
st = time.time()
ans_df.eventdate_ist =  ans_df.eventdate_ist.astype(str)


# ########### inserting into MongoDB database
# record1 = db['qpattern']
# # ##drop
# db.anomaly.drop()
# # ##Insert
records = json.loads(ans_df.T.to_json()).values()
db.anomaly.insert_many(records)
print("ANOMALY DETECTION : DONE")
print("time to complete function : ", time.time() - stime)

consolidate_df = ans_df.loc[ans_df.anomaly==1, ('examslot','centercode','candidateid') ]
consolidate_df['status'] = 'fast answering'
consolidate_df = consolidate_df[['examslot','centercode','candidateid','status']]
records = json.loads(consolidate_df.T.to_json()).values()
db.consolidate.insert_many(records)
print("CONSOLIDATE ::: ANOMALY DETECTION : DONE")
print("time to complete function : ", time.time() - stime)


# In[39]:


ans_df.sort_index(axis=1).columns
# results_df.sort_index(axis=1).columns


# In[52]:


# consolidate_df.candidateid.value_counts()


# In[84]:


db.anomaly.insert_many(records)
db


# In[ ]:


results_df.head(2)


# In[143]:


# from pandas.io.json import json_normalize
# results_df = pd.json_normalize(d.from_dict() for d in res.scan())
# # json_normalize(r)
# # from pandas.io.json import json_normalize
# # json_normalize(sample_object)
# # res.scan()


# In[135]:


print("1")
ans_df['avgTime']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(15)['timeToAns'].mean().droplevel(level='candidateid')
print("2")
ans_df['sumTime']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(15)['timeToAns'].sum().droplevel(level='candidateid')
print("3")
# df['moving'] = df.groupby(['col_1', 'col_2', 'col_3']).rolling(10)['value'].mean().droplevel(level=[0,1,2])
# rdf.groupby(['no']).rolling(5)['s'].mean()

## 10 questoin answerd with in 4min(240 sec) / mean time is 24 sec., then i considered as a ANAMOLY 
ans_df.loc[ (ans_df.avgTime <= 12),  'anomaly' ] = 1
ans_df.loc[ (ans_df.avgTime > 12),  'anomaly' ] = 0
print("4")

## Convert UST to IST
isttime = [ date.astimezone(timezone('Asia/Kolkata')) for date in  pd.to_datetime(ans_df['eventdate'])]
ans_df['eventdate_ist'] = isttime
st = time.time()
ans_df.eventdate_ist =  ans_df.eventdate_ist.astype(str)
print("6")

# # ########### inserting into MongoDB database
# # record1 = db['qpattern']
# # # ##drop
# # db.anomaly_a.drop()
# # # ##Insert
# records = json.loads(ans_df.T.to_json()).values()
# db.anomaly_a.insert_many(records)


# In[136]:



# ########### inserting into MongoDB database
# record1 = db['qpattern']
# # ##drop
db.anomaly.drop()
# # ##Insert
records = json.loads(ans_df.T.to_json()).values()
print("ok")
db.anomaly.insert_many(records)


# In[132]:


# ans_df.loc[ (ans_df.avgTime <= 12),]

ans_df.loc[ans_df.anomaly == 1,]

# ans_df.drop_duplicates(subset = ["candidateid"] ).loc[ (ans_df.avgTime <= 12),]


# In[119]:



# db.anomaly.drop()
# # # ##Insert
# records = json.loads(ans_df.T.to_json()).values()
# db.anomaly.insert_many(records)


# In[121]:


# %%time
# ans_df.to_dict()


# In[107]:


## Convert UST to IST
# mpa_df['eventdate_d'] = pd.to_datetime(mpa_df.eventdate)
from pytz import timezone

isttime = [ date.astimezone(timezone('Asia/Kolkata')) for date in  pd.to_datetime(ans_df['eventdate'])]
ans_df['eventdate_ist'] = isttime
st = time.time()
ans_df.eventdate_ist =  ans_df.eventdate_ist.astype(str)

db.anomaly.drop()
# # ##Insert
records = json.loads(ans_df.T.to_json()).values()
db.anomaly.insert_many(records)
print("ANOMALY DETECTION : DONE")
time.time() - st

# print("time to complete function : ", time.time() - stime)

# st = time.time()
# isttime = []
# for date in  pd.to_datetime(ans_df['eventdate']):
# #     print(date.astimezone(timezone('Asia/Kolkata')))
#     isttime.append(date.astimezone(timezone('Asia/Kolkata')))
# st - time.time()    

# st = time.time()
# isttime = [ date.astimezone(timezone('Asia/Kolkata')) for date in  pd.to_datetime(ans_df['eventdate'])]
# st - time.time()    
# # isttime
# # mpa_df['eventdate_ist'] = isttime



# In[99]:


pd.set_option("display.max_rows", 101)


#results_df.groupby('column8')['eventdate_date'].diff() /  np.timedelta64(1, 's')
# ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid'])['timeToAns'].sum()[1:100]



# In[88]:


# ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(10)['timeToAns'].sum().droplevel(level='candidateid')


# In[ ]:





# In[ ]:





# In[59]:


# results_df[['papercode','column6', 'candidateid', 'sessionid',  'question_new', 'questionset','examslot','centercode','eventdate','timeToAns']].sort_values(by=['candidateid','eventdate'], ascending=True).head(2)
# tt.seconds
ans_df = ans_df[['papercode','column6', 'candidateid', 'sessionid',  'question_new', 'questionset','examslot','centercode','eventdate','timeToAns']].sort_values(by=['candidateid','eventdate'], ascending=True)

## find the avge time for any 10 guestions, in this case first 9 question mean/average time would be null. 
# df[df['b'].notnull()]
ans_df['avgTime']= ans_df[ans_df['timeToAns'].notnull()].groupby(['candidateid']).rolling(10)['timeToAns'].mean().droplevel(level='candidateid')

# df['moving'] = df.groupby(['col_1', 'col_2', 'col_3']).rolling(10)['value'].mean().droplevel(level=[0,1,2])
# rdf.groupby(['no']).rolling(5)['s'].mean()


# In[60]:


## 10 questoin answerd with in 4min(240 sec) / mean time is 24 sec., then i considered as a ANAMOLY 
ans_df.loc[ (ans_df.avgTime <= 24),  'anamoly' ] = 1
ans_df.loc[ (ans_df.avgTime > 24),  'anamoly' ] = 0

# ans_df[['papercode','column6', 'candidateid', 'sessionid',  'question_new', 'questionset','examslot','centercode','eventdate','timeToAns', 'avgTime', 'anamoly']].head(10)
# ans_df.loc[(ans_df.anamoly ==1), ]
ans_df.loc[ans_df.candidateid=='JA2003JOD087XYA0002',]['eventdate']


# In[ ]:





# In[78]:


mpa_df.dtypes
# mpa_df['eventdate_d'] = mpa_df.eventdate_ist.str.slice(stop=18)
#  mpa_df["eventdate_nt"] = mpa_df.eventdate.str.slice(stop=19)


# In[77]:


## Convert UST to IST
mpa_df['eventdate_d'] = pd.to_datetime(mpa_df.eventdate)
import datetime
from pytz import timezone
isttime = []
for date in mpa_df['eventdate_d']:
#     print(date.astimezone(timezone('Asia/Kolkata')))
    isttime.append(date.astimezone(timezone('Asia/Kolkata')))
    
mpa_df['eventdate_ist'] = isttime
mpa_df.head(4)


# In[89]:


import pytz
from datetime import datetime
## yaer, month, date, hour, min, sec, timezone
unaware = datetime(2020, 10, 5, 14, 45, 0, 0)
exam_time = pytz.utc.localize(unaware)
exam_time
localtz = timezone('Asia/Kolkata')
exam_time = localtz.localize(unaware)
exam_time


# In[94]:


# exam_time = '2020-10-05 14:45:00+05:30' 
# exam_time  = datetime(2020, 10, 5, 14, 45)
# exam_time = datetime.strptime(exam_time, '%Y-%m-%d %H:%M:%S+05:30')
mpa_df['centre_delay']=mpa_df['eventdate_ist'].rsub( exam_time).astype('timedelta64[m]')
mpa_df['centre_delay']= [-i for i in mpa_df['centre_delay']]


# In[95]:


mpa_df.head(4)


# In[91]:


### USER Needs to give time to crawl the data
delyaed_min_login_time = '2020-07-16T00:00:00.000Z'
from datetime import datetime


# In[194]:


global delyaed_min_login_time
def startExamDelay():

    delyaed_min_login_time = '2020-10-05T00:00:00.000Z'
    ### First time it will start from user given TIME, if empty current time will be taken
    if delyaed_min_login_time == '':
        now = datetime.now()
        delyaed_login_time = now.strftime("%d/%m/%Y %H:%M:%S")
    else:
        delyaed_login_time = delyaed_min_login_time

    ############## Extract Center-wise make paper available data from ELK
    ress= Search(using = elastic_client,  index=alias )
    # ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
    # ress = ress.query('range', **{'eventdate': {'gte': '2020-03-09T00:00:00.000Z'}})
    ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time}})
    ##change 1 #added
    res = ress.query('match', **{'msg':'SUCCESS: Make Paper Section Available'})
    #     res = ress.query('match', **{'makePaperSectionAvailable'})
    res = res.query('match', **{'examslot':'F'})



    # res = res.query('match', **{'examslot':'C'})
    cnt = res.count()
    rst = res.execute()
    cnt


    ############# Converting into DATAFRAME
    type(rst.hits.hits)
    # rst_df = pd.io.json.json_normalize(rst.hits.hits)




    import time
    start_time = time.time()
    results_df = pd.DataFrame(d.to_dict() for d in res.scan())
    results_df.shape
    print("time for completion : ",  time.time() - start_time )
    len( results_df['centercode'].unique() )
    if(len(results_df) != 0):

        #### set Minimum Time to crawl NEXT time
        delyaed_min_login_time = results_df['eventdate'].min()


        mpa_df = results_df.loc[(results_df['msg'].notnull() & results_df['msg'].str.contains("SUCCESS: Make Paper Available") ) ]#, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]
        mpa_df = mpa_df[['msg','centercode','eventdate', 'papercode', 'examslot']]



        # results_df.head(1)
        ############## Data Cleansing and Transformation.
        mpa_df.head(1)
        mpa_df['papercode'] = results_df.column4.str[-3:-1]
        print(mpa_df.head(3))
        #     mpa_df['column7'] = results_df.column7.str[:-1]



        ############EACH CENTER has multiple make paper available for SAME paper/slot
        ## SO Ihave grouped and takem=n MAX time-- may be its test data not in actual I GUESS
        idx = mpa_df.groupby(['centercode', 'papercode','examslot'])['eventdate'].transform(max) == mpa_df['eventdate']
        mpa_df = mpa_df[idx]
        mpa_df.head(5)

        ################## Extract Candidate Exam START time
        res1 = ress.query('match', **{'msg':'startExamination'})
        # res1 = res1.query('match', **{'examslot':'C'})
        cnt = res1.count()
        # rst_df = pd.io.json.json_normalize(rst.hits.hits)
        stime = time.time()
        start_results_df = pd.DataFrame(d.to_dict() for d in res1.scan())
        print("time : ", time.time() - stime)
        start_results_df.shape
        print(start_results_df.columns)

        ######## Info / Extrcat candidate LOGIN details
        clogin_df =  start_results_df.loc[( start_results_df['candidateid'].notnull() & start_results_df['msg'].str.contains("startExamination") ) ,['candidateid', 'eventdate', 'msg', 'examslot','centercode', 'papercode', 'column5','slot']]
        clogin_df.shape

        ####### take first login row - to avaoid repeat candidateid - if Multiple logins
        #     clogin_df = clogin_df.groupby('candidateid').first().reset_index()
#         clogin_df = clogin_df.groupby('candidateid').last().reset_index()
        clogin_df = clogin_df.groupby('candidateid').first().reset_index()
        clogin_df.shape

        ##merge mpa_df with clogin_df based on slot+papercode+centrecode
        sexm_tdiff_df = pd.merge(left=mpa_df, right=clogin_df,  how='left',suffixes=('_mpa', '_clog'),
                                  on= ['papercode','examslot','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
        sexm_tdiff_df.shape
        # sexm_tdiff_df.head(3)
        sexm_tdiff_df = sexm_tdiff_df.loc[sexm_tdiff_df['candidateid'].notnull(),]


        ################ get ACTUALL TIME DIFFERENCE for Candidate LOGIN
        ##astype('timedelta64[s] -> s : return as SECONDS , m: return as Minuts
        sexm_tdiff_df['start_td']=pd.to_datetime(sexm_tdiff_df['eventdate_mpa']).rsub( pd.to_datetime(sexm_tdiff_df['eventdate_clog']) ).astype('timedelta64[m]')
        sexm_tdiff_df[['start_td', 'eventdate_clog', 'eventdate_mpa', 'candidateid', 'centercode', 'examslot', 'papercode']][1:3]
        print(sexm_tdiff_df.head(5))

        ############ Time cluster 
        sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td  == 0) ,'start_td_c' ] = "< 1 min"

        sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td   == 1) ,'start_td_c' ] = "1- 2 min"
        sexm_tdiff_df.loc[ sexm_tdiff_df.start_td == 2,'start_td_c'] = '2-3 min'
        sexm_tdiff_df.loc[(sexm_tdiff_df.start_td ==3 ) | (sexm_tdiff_df.start_td   == 4 ), 'start_td_c' ] ="3-5 min"
        sexm_tdiff_df.loc[sexm_tdiff_df.start_td.isnull(), 'start_td_c' ] = 'Not started'
        # sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td < 0 ),  'start_td_c' ] = "Issues"
        sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td < 0 ),  'start_td_c' ] = "< 1 min"
        sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td >= 5 ),  'start_td_c' ] = "> 5 min"




        # sexm_tdiff_df


        ########### inserting into MongoDB database
        # record1 = db['qpattern']
        ##drop
        db.starttime.drop()
        ##Insert
        records = json.loads(sexm_tdiff_df.T.to_json()).values()
        db.starttime.insert_many(records)
        print("START TIME DELAY : DONE")
        print("time to complete function : ", time.time() - stime)
        time.sleep(30)
    else:
        print("EXAM NOT STARTED EXAM")


# In[195]:


# startExamDelay()
##1. how ti run #1
while True:
    startExamDelay()


# ## 2. Super Finisher 
# #### candidate who finish exam early
# 
# - only few candidates are submitted exam (only 14K in all 4 slots)
#     - candidate_id is missing in the Log
# - mostly exam ended by CI, 
#     - candidate_id is missing in the Log

# In[186]:


##changes ##cell added
def quickFinisher():
    
    ############## Extract Center-wise make paper available data from ELK
    ress= Search(using = elastic_client,  index=alias )
    # ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
    ress = ress.query('range', **{'eventdate': {'gte': '2020-10-03T09:00:00.000Z'}})
    # ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time}})
    ##change 1 #added
    ress = ress.query('match', **{'examslot':'A'})

    # ress.count()
    res2 = ress.query('match', **{'message':'Candidate Submitted Examination'})

    # ress1= Search(using = elastic_client,  index=alias )
    # ress1 = ress1.query('range', **{'@timestamp': {'gte': '2020-02-23'}})
    # ress1.count()
    # res2 = ress.query('match', **{'msg':'endExamination'})
#     res2 = res2.query('match', **{'examslot':'C'})
    cnt = res2.count()
    # rst_df = pd.io.json.json_normalize(rst.hits.hits)
    stime = time.time()
    cend_df = pd.DataFrame(d.to_dict() for d in res2.scan())
    print("time : ", time.time() - stime)
    cend_df.shape

    if(len(cend_df) != 0):
        ######## Data cleansing and Transformation
        # cend_df =  cend_df.loc[(cend_df['message'].str.contains("Candidate Submitted Examination") ) ,['candidateid', 'eventdate', 'msg', 'examslot','slot','centercode', 'papercode', 'column5']]
        print(len(cend_df))
        cend_df =  cend_df.loc[: ,['eventdate', 'msg', 'examslot','centercode', 'papercode', 'column5']]
        cend_df['column5'] = cend_df['column5'].str[:-1]
        # cend_df['column7'] = cend_df['column7'].str[:-1]

        cend_df.head(1)


        ################## Extract Candidate Exam START time
        res1 = ress.query('match', **{'msg':'startExamination'})
        # res1 = res1.query('match', **{'examslot':'C'})
        cnt = res1.count()
        # rst_df = pd.io.json.json_normalize(rst.hits.hits)
        stime = time.time()
        start_results_df = pd.DataFrame(d.to_dict() for d in res1.scan())
        print("time : ", time.time() - stime)
        start_results_df.shape

        ######## Info / Extrcat candidate LOGIN details
        clogin_df =  start_results_df.loc[( start_results_df['candidateid'].notnull() & start_results_df['msg'].str.contains("startExamination") ) ,['candidateid', 'eventdate', 'msg', 'examslot','centercode', 'papercode', 'column5','slot']]
        clogin_df.shape

        ####### take first login row - to avaoid repeat candidateid - if Multiple logins
        clogin_df = clogin_df.groupby('candidateid').first().reset_index()

        print( "clogin ",  clogin_df.shape)


        ##merge clogin_df with cend_df based on slot+papercode+centrecode+sessionID
        superFinisher_df = pd.merge(left=clogin_df, right=cend_df,  how='right',suffixes=('_clog', '_cend'),
                                  on= ['papercode','examslot','column5', 'centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']

        superFinisher_df.head(2)
        ##get SUPER/QUICK FINISHER TIME 
        ##astype('timedelta64[s] -> s : return as SECONDS , m: return as Minuts
        superFinisher_df['end_td']=pd.to_datetime(superFinisher_df['eventdate_clog']).rsub( pd.to_datetime(superFinisher_df['eventdate_cend']) ).astype('timedelta64[m]')
        # data['Aging'] = data['Created_Date'].rsub(today, axis=0).dt.days
        # superFinisher_df[['end_td', 'eventdate_clog', 'eventdate_cend', 'candidateid_clog', 'centercode', 'examslot', 'papercode']][1:3]


        ##VISUVALIZATION SUPER/QUICK FINISHER
        superFinisher_df.loc[ superFinisher_df.end_td <15,'end_td_c'] = "<15 min"
        superFinisher_df.loc[ (superFinisher_df.end_td >= 15) & (superFinisher_df.end_td < 30 ),'end_td_c' ] = "15-30 min"
        superFinisher_df.loc[(superFinisher_df.end_td >=30 ) & (superFinisher_df.end_td <=45 ), 'end_td_c' ] ="30-45 min"
        superFinisher_df.loc[(superFinisher_df.end_td >=45 ) & (superFinisher_df.end_td <=60 ), 'end_td_c' ] ="45-60 min"
        superFinisher_df.loc[ superFinisher_df.end_td >60,'end_td_c'] = "Beyond 1 Hr"
        superFinisher_df.loc[superFinisher_df.end_td.isnull(), 'end_td_c' ] = "Not Finished"


        print(superFinisher_df.head(2))

        ########### inserting into MongoDB database
        # record1 = db['qpattern']
        ##drop
        db.qfinisher.drop()
        ##Insert
        records = json.loads(superFinisher_df.T.to_json()).values()
        db.qfinisher.insert_many(records)
        print("QUICK FINISHER - DONE")
        print("time QUICK FINISHER : ", time.time() - stime)
        time.sleep(30)
    else:
        print("NO ONE SUBMITED EXAM")


# In[187]:


quickFinisher()
# while True:
#     quickFinisher()

