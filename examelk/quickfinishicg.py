import sys
# sys.path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import pandas as pd
import pymongo
import json
from pymongo import MongoClient
import time

pd.options.display.max_colwidth = 500
pd.options.display.max_columns = 51

# ES_CERTIFICATES = {'ca_certs':'/usr/local/share/ca-certificates/icgelastic/ca.crt'}
# elastic_client = Elasticsearch(host='icgems.cdacchn.in', port=9200, verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],http_auth=('elastic','34ERdfcv@34'),timeout=300)



from urllib.parse import quote

def quickfinishicgfn(starttime,endtime,slot,papercode):
    execution = False
    ################ MONGODB conaction details
    connection = MongoClient("mongodb://10.184.61.202:27017")
    db=connection['icg2021Sep']
    
    ES_CERTIFICATES = {'ca_certs':'/usr/local/share/ca-certificates/elastic/ca.crt'}
    elastic_client = Elasticsearch(host='icgems.cdacchn.in', port=9200, verify_certs=False,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],http_auth=('elastic','34ERdfcv@34'),timeout=300)
    
    alias = []
    alias.extend(elastic_client.indices.get_alias('icgmain-20210928'))


    ##changes ##cell added
    print(" STRAT TIME ::", time.ctime())
    ress= Search(using = elastic_client,  index=alias )

    ############## Extract Center-wise make paper available data from ELK  
    ress = ress.query('range', **{'eventdate': {'gte': starttime}})
    ress = ress.query('match', **{'examslot':slot})
    res2 = ress.query('match', **{'message':'Candidate Submitted Examination'})
    cnt = res2.count()
    print( "COUNT : ", cnt)
    stime = time.time()
    cend_df = pd.DataFrame(d.to_dict() for d in res2.scan())
    print("time : ", time.time() - stime)
    print("SHAPE", cend_df.shape)
    print( cend_df.head(1))
    if(len(cend_df) != 0):
        cend_df =  cend_df.loc[(cend_df['message'].str.contains("Candidate Submitted Examination") ) ,]
        cend_df = cend_df[cend_df['candidateid'].notna()]
        ######## Data cleansing and Transformation
        cend_df =  cend_df.loc[: ,['eventdate', 'message', 'examslot','centercode', 'papercode', 'column5', 'column6', 'sessionid']]
        cend_df['column5'] = cend_df['column5'].str[:-1]        
        cend_df.head(1)

        ################## Extract Candidate Exam START time
        res1 = ress.query('match', **{'message':'startExamination'})
        # res1 = res1.query('match', **{'examslot':'C'})
        cnt = res1.count()
        # rst_df = pd.io.json.json_normalize(rst.hits.hits)
        stime = time.time()
        start_results_df = pd.DataFrame(d.to_dict() for d in res1.scan())
        print("time : ", time.time() - stime)
        start_results_df.shape
        
        
        ######## Info / Extrcat candidate LOGIN details
        clogin_df =  start_results_df.loc[( start_results_df['candidateid'].notnull() & start_results_df['message'].str.contains("startExamination") ) ,['candidateid', 'eventdate', 'message', 'examslot','centercode', 'papercode', 'column5','slot', 'column6', 'sessionid']]
        clogin_df.shape    
        
        ####### take first login row - to avaoid repeat candidateid - if Multiple logins
        clogin_df = clogin_df.groupby(['centercode', 'slot', 'sessionid']).first().reset_index()

        ##merge clogin_df with cend_df based on slot+papercode+centrecode+sessionID
        superFinisher_df = pd.merge(left=clogin_df, right=cend_df,  how='right',suffixes=('_clog', '_cend'),
                                    on= ['sessionid','papercode','examslot', 'centercode'])

        ##get SUPER/QUICK FINISHER TIME 
        ##astype('timedelta64[s] -> s : return as SECONDS , m: return as Minuts
        superFinisher_df['end_td']=pd.to_datetime(superFinisher_df['eventdate_clog']).rsub( pd.to_datetime(superFinisher_df['eventdate_cend']) ).astype('timedelta64[m]')

        ##VISUVALIZATION SUPER/QUICK FINISHER
        superFinisher_df.loc[ superFinisher_df.end_td <15,'end_td_c'] = "quick"
        superFinisher_df.loc[ (superFinisher_df.end_td >= 15) & (superFinisher_df.end_td <= 30 ),'end_td_c' ] = "fast"
        superFinisher_df.loc[superFinisher_df.end_td >30 , 'end_td_c' ] ="ontime"       
        superFinisher_df.loc[superFinisher_df.end_td.isnull(), 'end_td_c' ] = "Not Finished"

        ########### inserting into MongoDB database
        # record1 = db['qpattern']
        ##drop
    #         db.qfinisher.drop()
        ##Insert
        records = json.loads(superFinisher_df.T.to_json()).values()
        db.qfinisher.insert_many(records)
        print("QUICK FINISHER - DONE")
        print("time QUICK FINISHER : ", time.time() - stime)

        consolidate_df = superFinisher_df.loc[superFinisher_df.end_td_c=='quick', ('examslot','centercode','candidateid') ]
        consolidate_df['status'] = 'quick finish'
        consolidate_df = consolidate_df[['examslot','centercode','candidateid','status']]
        records = json.loads(consolidate_df.T.to_json()).values()
        db.consolidate.insert_many(records)
        print("CONSOLIDATE - DONE")
        print("time CONSOLIDATE QUICK FINISHER : ", time.time() - stime)
        execution = True

    else:
        print("NO ONE SUBMITED EXAM")
        time.sleep(20)
        execution = True
        
    return execution