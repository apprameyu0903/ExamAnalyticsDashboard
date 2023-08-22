#!/usr/bin/env python
# coding: utf-8

# In[1]:


from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


# STAR - SUCCESS: Make Paper Section Available
#     
# AFCAT - SUCCESS: Make Paper Available

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
ES_CERTIFICATES = {'ca_certs':'/usr/local/share/ca-certificates/elastic/ca.crt','client_certs':'/usr/local/share/ca-certificates/elastic/logclient.crt','client_key':'/usr/local/share/ca-certificates/elastic/logclient.key'}

elastic_client = Elasticsearch(host='iafems.cdacchn.in', port=9200, verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=('elastic','34ERdfcv@34'),timeout=300)


# In[29]:


##gaierror: [Errno -2] Name or service not known /etc/hosts
###10.184.53.57    iafems.cdacchn.in
alias = []
# alias.extend(elastic_client.indices.get_alias('iafstar-june-testing'))
alias.extend(elastic_client.indices.get_alias('iafafcat-aug-test-20210829'))


alias


# In[5]:


from urllib.parse import quote
################ MONGODB conaction details
# mongod --bind_ip 10.184.51.194 --dbpath /var/lib/mongodb/

# connection = MongoClient("mongodb://10.184.51.194:27017")
connection = MongoClient("mongodb://10.184.61.202:27017")

db=connection['afcat21Aug']


# In[6]:


db


# ## 1. #### getting minimum time  to Query
# 
# #### points to obe noted
# - 1. minmum time of each use case need to recorded 
# - 2. query to fetch data from ELK shoud be based on each use-case "string patern" and "minimum time" noted above
# - 3. **EXECUTE** only required block for every minutes may be use-casess block

# In[16]:


pd.set_option('display.max_rows', 500)


# In[44]:


# results_df['msg'][1:50]
# results_df[ results_df['msg'].str.contains("SUCCESS: Make Paper") ]  #, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]


# In[17]:


# results_df


# In[19]:


### USER Needs to give time to crawl the data
# delyaed_min_login_time = '2021-03-15T00:00:00.000Z'
from datetime import datetime


# In[33]:


global delyaed_min_login_time
# def startExamDelay():

#     delyaed_min_login_time = '2021-02-12T00:00:00.000Z'

delyaed_min_login_time = '2021-08-29T09:00:00.000Z'
delyaed_login_time_min = '2021-08-29T11:30:00.000Z'

#     delyaed_min_login_time = '2021-08-29T04:00:00.000Z'
#     delyaed_login_time_min = '2021-08-29T06:00:00.000Z'


### First time it will start from user given TIME, if empty current time will be taken
if delyaed_min_login_time == '':
    now = datetime.now()
    delyaed_login_time = now.strftime("%d/%m/%Y %H:%M:%S")
else:
    delyaed_login_time = delyaed_min_login_time

print("delyaed_login_time : ", delyaed_login_time)    
############## Extract Center-wise make paper available data from ELK
ress= Search(using = elastic_client,  index=alias )
# ress = ress.query('range', **{'eventdate': {'gte': '09/03/2020 00:00:00'}})
#     ress = ress.query('range', **{'eventdate': {'gte': '2021-03-15T010:15:00.00'}})
ress = ress.query('range', **{'eventdate': {'gte':  delyaed_login_time, 'lte':  delyaed_login_time_min, }})
##change 1 #added
#     res = ress.query('match', **{'msg':'SUCCESS: Make Paper Section Available'})
res = ress.query('match', **{'msg':'candidateLogin'})
#     res = ress.query('fuzzy', **{'msg':{'value':"SUCCESS: Make Paper Available"}})

#     res = ress.query('match', **{'makePaperSectionAvailable'})
res = res.query('match', **{'examslot':'C'})
#     res = res.query('match', **{'papercode':'XY'})



# res = res.query('match', **{'examslot':'C'})
cnt = res.count()
rst = res.execute()
cnt


############# Converting into DATAFRAME
type(rst.hits.hits)
# rst_df = pd.io.json.json_normalize(rst.hits.hits)


import time
start_time = time.time()
print("make a scan " )
results_df = pd.DataFrame(d.to_dict() for d in res.scan())
print("shape:: ", results_df.shape)

print("time for completion : ",  time.time() - start_time )
print( "length is", len( results_df['centercode'].unique() ))
print( " Data frame ", results_df.head(2) )
#     if(len(results_df) != 0):

#### set Minimum Time to crawl NEXT time
delyaed_min_login_time = results_df['eventdate'].min()


mpa_df = results_df.loc[(results_df['msg'].notnull() & results_df['msg'].str.contains("candidateLogin") ) ]#, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]
if(len(mpa_df) != 0):
    mpa_df = mpa_df[['msg','centercode','eventdate', 'papercode', 'examslot', 'candidateid']]

    print("RESULTS DF ")
    print(results_df.head(1))

    # results_df.head(1)
    ############## Data Cleansing and Transformation.
    mpa_df.head(1)
#     mpa_df['papercode'] = results_df.column4.str[-3:-1]
    print(mpa_df.head(3))
    #     mpa_df['column7'] = results_df.column7.str[:-1]



    ############EACH CENTER has multiple make paper available for SAME paper/slot
    ## SO Ihave grouped and takem=n MAX time-- may be its test data not in actual I GUESS
#         idx = mpa_df.groupby(['centercode', 'papercode','examslot'])['eventdate'].transform(max) == mpa_df['eventdate']
    count_df = mpa_df.groupby(['examslot','centercode','candidateid']).size().reset_index(name='counts')
    
#         mpa_df = mpa_df[idx]
    print("head 2")
    print(count_df.head(2))
    print('Make paper centercode')
#         print(mpa_df['centercode'])


#         ################## Extract Candidate Exam START time
 
    stime = time.time()
 
    ########### inserting into MongoDB database
    # record1 = db['qpattern']
    ##drop
#         db.starttime.drop()
    ##Insert
    records = json.loads(count_df.T.to_json()).values()
    db.multiplelogin.insert_many(records)
    print("START TIME DELAY : DONE")
    print("time to complete function : ", time.time() - stime)
    consolidate_df = count_df.loc[count_df.counts >=2,]
    consolidate_df['status']  = 'multiple login'
    consolidate_df = consolidate_df[['examslot','centercode','candidateid','status']]
    records = json.loads(consolidate_df.T.to_json()).values()
    db.consolidate.insert_many(records)
    print(" CONSIOLIDATE DONE")
    print("time to complete function : ", time.time() - stime)
#         time.sleep(3000)
else:
    print("EXAM NOT STARTED EXAM")


# In[ ]:





# In[ ]:





# In[92]:


# mpa_df[['centercode', 'papercode','examslot']]    


# #### Run this in MongoDB

# In[ ]:



var duplicates = [];
db.multiplelogin.aggregate([
 
  { $group: { 
    _id: { name: "$candidateid",  examsolt: "$examslot"}, 
    dups: { "$addToSet": "$_id" }, 
    count: { "$sum": 1 } 
  }}, 
  { $match: { 
    count: { "$gt": 1 } 
  }}
],
{allowDiskUse: true}    
).forEach(function(doc) {
    doc.dups.shift();      
    doc.dups.forEach( function(dupId){ 
        duplicates.push(dupId);
        }
    )    
})

(/, If, you, want, to, Check, all, "_id", which, you, are, deleting, else, print, statement, not, needed)
printjson(duplicates);     


(/, Remove, all, duplicates, in, one, go)
db.multiplelogin.remove({_id:{$in:duplicates}}) 

