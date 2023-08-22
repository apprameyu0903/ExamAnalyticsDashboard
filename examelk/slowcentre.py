#!/usr/bin/env python
# coding: utf-8

# In[1]:


from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


# In[5]:


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


# In[6]:


# ###sudo update-ca-certificates
ES_CERTIFICATES = {'ca_certs':'/usr/local/share/ca-certificates/elastic/ca.crt','client_certs':'/usr/local/share/ca-certificates/elastic/logclient.crt','client_key':'/usr/local/share/ca-certificates/elastic/logclient.key'}

elastic_client = Elasticsearch(host='iafems.cdacchn.in', port=9200, verify_certs=True,use_ssl=True,ca_certs=ES_CERTIFICATES['ca_certs'],client_certs=ES_CERTIFICATES['client_certs'],client_key=ES_CERTIFICATES['client_key'],http_auth=('elastic','34ERdfcv@34'),timeout=300)


# In[17]:


# dir(elastic_client)


# In[18]:


# elastic_client.search()


# In[38]:


##gaierror: [Errno -2] Name or service not known /etc/hosts
###10.184.53.57    iafems.cdacchn.in
alias = []
# alias.extend(elastic_client.indices.get_alias('iafstar-june-testing'))

alias.extend(elastic_client.indices.get_alias('iafafcat-aug-test-20210829'))

alias


# In[8]:


from urllib.parse import quote
################ MONGODB conaction details
# mongod --bind_ip 10.184.51.194 --dbpath /var/lib/mongodb/

# connection = MongoClient("mongodb://10.184.51.194:27017")
connection = MongoClient("mongodb://10.184.61.202:27017")

# mongo_uri = "mongodb://iaf:" + quote("cdac123!@#") + "@10.184.61.202:27017"

db=connection['afcat21Aug']


# In[9]:


db


# ## 1. #### getting minimum time  to Query
# 
# #### points to obe noted
# - 1. minmum time of each use case need to recorded 
# - 2. query to fetch data from ELK shoud be based on each use-case "string patern" and "minimum time" noted above
# - 3. **EXECUTE** only required block for every minutes may be use-casess block
# - 4. GET THE TIME SLOT FOR EACH EXAM SLOT AND STORE IT

# In[43]:


# actualExamTime = 'November 4, 2020, 09:00:00 AM'
from datetime import datetime

##ACTUAL EXAM START TIME
### 24 HOUR FORMAt
## yaer, month, date, hour, min, sec, timezone

# unaware = datetime(2021, 3, 22, 9, 15, 0, 0)

# unaware = datetime(2021, 7, 18, 9, 0, 0, 0)
# unaware = datetime(2021, 7, 18, 13, 0, 0, 0)
# unaware = datetime(2021, 8, 29, 9, 45, 0, 0)
unaware = datetime(2021, 8, 29, 14, 45, 0, 0)

### 12 HOUR FORMAT

# actualExamTime = 'July 18, 2021, 09:00:00 AM'
# actualExamTime = 'July 18, 2021, 01:00:00 PM'
# actualExamTime = 'August 29, 2021, 09:45:00 AM'
actualExamTime = 'August 29, 2021, 02:45:00 PM'

 


# In[44]:


######################### EXAM TIME for EACH SLOT
import pytz
from datetime import datetime

from pytz import timezone

stime = time.time()
    


## yaer, month, date, hour, min, sec, timezone
# unaware = datetime(2020, 11, 4, 10, 30, 0, 0)
# exam_time = pytz.utc.localize(unaware)
# exam_time
localtz = timezone('Asia/Kolkata')
exam_time = localtz.localize(unaware)
print(exam_time)



######################### DATA LOG CRAWLING DATE & TIME
delyaed_min_login_time = '2021-08-29T08:30:00.000Z'
# delyaed_min_login_time = '2021-08-29T03:30:00.000Z'

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
ress.count()

##change 1 #added
# res = ress.query('match', **{'msg':'SUCCESS: Make Paper Section Available'})
res = ress.query('match', **{'msg':'SUCCESS: Make Paper Available'})
res.count()

#     res = ress.query('match', **{'makePaperSectionAvailable'})
res = res.query('match', **{'examslot':'D'})
# res = res.query('match', **{'papercode':'XY'})
res.count()


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
# len( results_df['centercode'].unique() )
print(results_df.head(1))

#### set Minimum Time to crawl NEXT time
delyaed_min_login_time = results_df['eventdate'].min()


mpa_df = results_df.loc[(results_df['msg'].notnull() & results_df['msg'].str.contains("SUCCESS: Make Paper Available") ) ]#, ['msg','centercode','eventdate', 'papercode', 'examslot', 'column7'] ]
mpa_df = mpa_df[['msg','centercode','eventdate', 'papercode', 'examslot']]



# results_df.head(1)
############## Data Cleansing and Transformation.
# mpa_df['papercode'] = results_df.column4.str[-3:-1]
print(mpa_df.head(1))
#     mpa_df['column7'] = results_df.column7.str[:-1]



############EACH CENTER has multiple make paper available for SAME paper/slot
## SO Ihave grouped and takem=n MIN time-- may be its test data not in actual I GUESS
## so that we can know its started on time or not. 
idx = mpa_df.groupby(['centercode', 'papercode','examslot'])['eventdate'].transform(min) == mpa_df['eventdate']
mpa_df = mpa_df[idx]
print("MPA : ")
# print(mpa_df.head(1))


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

mpa_df['centre_delay']=mpa_df['eventdate_ist'].rsub( exam_time).astype('timedelta64[m]')
mpa_df['centre_delay']= [-i for i in mpa_df['centre_delay']]

mpa_df.loc[ (mpa_df.centre_delay >= -3) & (mpa_df.centre_delay <=3 ),  'centre_delay_c' ] = "ontime"

mpa_df.loc[ (mpa_df.centre_delay <= -4 ),  'centre_delay_c' ] = "early"
mpa_df.loc[ (mpa_df.centre_delay >= 4) & (mpa_df.centre_delay <=10 ),  'centre_delay_c' ] = "slow"
mpa_df.loc[ (mpa_df.centre_delay >= 11),  'centre_delay_c' ] = "delayed"


mpa_df.eventdate_ist =  mpa_df.eventdate_ist.astype(str)
mpa_df.eventdate_d = mpa_df.eventdate_d.astype(str)
mpa_df['actual_exam_time']=actualExamTime
mpa_df[['centercode', 'eventdate_ist', 'centre_delay', 'centre_delay_c']].head(20)


# ################## Extract Candidate Exam START time
# res1 = ress.query('match', **{'msg':'startExamination'})
# # res1 = res1.query('match', **{'examslot':'C'})
# cnt = res1.count()
# # rst_df = pd.io.json.json_normalize(rst.hits.hits)
# stime = time.time()
# start_results_df = pd.DataFrame(d.to_dict() for d in res1.scan())
# print("time : ", time.time() - stime)
# start_results_df.shape


# ######## Info / Extrcat candidate LOGIN details
# clogin_df =  start_results_df.loc[( start_results_df['candidateid'].notnull() & start_results_df['msg'].str.contains("startExamination") ) ,['candidateid', 'eventdate', 'msg', 'examslot','centercode', 'papercode', 'column5','slot']]
# clogin_df.shape

# ####### take first login row - to avaoid repeat candidateid - if Multiple logins
# #     clogin_df = clogin_df.groupby('candidateid').first().reset_index()
# clogin_df = clogin_df.groupby('candidateid').last().reset_index()
# clogin_df.shape

# ##merge mpa_df with clogin_df based on slot+papercode+centrecode
# sexm_tdiff_df = pd.merge(left=mpa_df, right=clogin_df,  how='left',suffixes=('_mpa', '_clog'),
#                           on= ['papercode','examslot','centercode'])#left_on=['A_c1','c2'], right_on = ['B_c1','c2']
# sexm_tdiff_df.shape
# # sexm_tdiff_df.head(3)
# sexm_tdiff_df = sexm_tdiff_df.loc[sexm_tdiff_df['candidateid'].notnull(),]


# ################ get ACTUALL TIME DIFFERENCE for Candidate LOGIN
# ##astype('timedelta64[s] -> s : return as SECONDS , m: return as Minuts
# sexm_tdiff_df['start_td']=pd.to_datetime(sexm_tdiff_df['eventdate_mpa']).rsub( pd.to_datetime(sexm_tdiff_df['eventdate_clog']) ).astype('timedelta64[m]')
# sexm_tdiff_df[['start_td', 'eventdate_clog', 'eventdate_mpa', 'candidateid', 'centercode', 'examslot', 'papercode']][1:3]
# print(sexm_tdiff_df.head(5))

# ############ Time cluster 
# sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td  == 0) ,'start_td_c' ] = "< 1 min"

# sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td   == 1) ,'start_td_c' ] = "1- 2 min"
# sexm_tdiff_df.loc[ sexm_tdiff_df.start_td == 2,'start_td_c'] = '2-3 min'
# sexm_tdiff_df.loc[(sexm_tdiff_df.start_td ==3 ) | (sexm_tdiff_df.start_td   == 4 ), 'start_td_c' ] ="3-5 min"
# sexm_tdiff_df.loc[sexm_tdiff_df.start_td.isnull(), 'start_td_c' ] = 'Not started'
# # sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td < 0 ),  'start_td_c' ] = "Issues"
# sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td < 0 ),  'start_td_c' ] = "< 1 min"
# sexm_tdiff_df.loc[ (sexm_tdiff_df.start_td >= 5 ),  'start_td_c' ] = "> 5 min"




# # sexm_tdiff_df


# ########### inserting into MongoDB database
# # record1 = db['qpattern']
# ##drop
# db.centerdelay.drop()
# ##Insert
records = json.loads(mpa_df.T.to_json()).values()
db.centerdelay.insert_many(records)
print("CENTRE DELAY : DONE")
print("time to complete function : ", time.time() - stime)


# In[23]:


mpa_df


# In[139]:


# for d in res.scan():
#     d

# print(res.do_dict())

# pd.json_normalize(rst.to_dict())


# In[16]:


# mpa_df[['eventdate_ist','centre_delay']]
mpa_df['exam_time']=exam_time
mpa_df[['exam_time', 'eventdate_ist']][1:10]

# pd.to_datetime(mpa_df['eventdate_ist']).rsub( pd.to_datetime(mpa_df['exam_time']) ).astype('timedelta64[m]')


# In[40]:


# isttime


# In[ ]:





# In[ ]:





# In[17]:


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


# #### Run this in MongoDB

# In[ ]:



var duplicates = [];
db.centerdelay.aggregate([
 
  { $group: { 
    _id: { name: "$centercode", examsolt: "$examslot"}, 
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
db.centerdelay.remove({_id:{$in:duplicates}}) 


# In[27]:


exp = [1,5,3,10, 8, 9]

salary = [25000, 38000, 30000, 120000, 100000, 980000]


# ## Liner regression

# In[65]:


import pandas as pd
import requests

# url = 'https://raw.githubusercontent.com/vinotharjun/ml-workshop/master/Linear%20Regression/salarydata.csv'
# s=requests.get(url).content
# c=pd.read_csv(io.StringIO(s.decode('utf-8')))

df = pd.read_csv('/home/cloud/salarydata.csv')
x = df['YearsExperience'].values
y = df['Salary'].values


# In[ ]:


#y =  a * x +b OR y =  b1 * x +b0
# b = b0

# B1 = sum((x(i) - mean(x)) * (y(i) - mean(y))) / sum( (x(i) - mean(x))^2 )
# B0 = mean(y) - B1 * mean(x)


# a = sum((exp(i) - mean(exp)) * (salary(i) - mean(salary))) / sum( (exp(i) - mean(exp))^2 )
# 
# 
# b = mean(salary) - a * mean(exp) 
# 
# 
# Tutorial
# 
# This tutorial is broken down into five parts:
# 
#     Calculate Mean and Variance.
#     Calculate Covariance.
#     Estimate Coefficients.
#     Make Predictions.
#     Predict Insurance.
# 
# 

# #### 1 Mean and Varience
# 
# 
# 

# In[30]:


##1. mean and varience

# Calculate the mean value of a list of numbers
def mean(values):
	return sum(values) / float(len(values))

# Calculate the variance of a list of numbers
def variance(values, mean):
	return sum([(x-mean)**2 for x in values])


# In[66]:


mean(exp)
variance(exp, mean(exp))

# x = exp
# y= salary

mean_x, mean_y = mean(x), mean(y)
var_x, var_y = variance(x, mean_x), variance(y, mean_y)
print('x stats: mean=%.3f bvariance=%.3f' % (mean_x, var_x))
print('y stats: mean=%.3f variance=%.3f' % (mean_y, var_y))


# #### Covarience
# 
# - The covariance of two groups of numbers describes how those numbers change together.
# 
# covariance = sum((x(i) - mean(x)) * (y(i) - mean(y)))
# 
# 

# In[67]:


##2. Calculate Covariance
# Calculate covariance between x and y
def covariance(x, mean_x, y, mean_y):
	covar = 0.0
	for i in range(len(x)):
		covar += (x[i] - mean_x) * (y[i] - mean_y)
	return covar


# In[68]:


covar = covariance(x, mean_x, y, mean_y)
print('Covariance: %.3f' % (covar))


#  #### Estimate Coefficients & intercept
#  
# - ax + b ==> a is Coefficients
#  
#  B1 = sum((x(i) - mean(x)) * (y(i) - mean(y))) / sum( (x(i) - mean(x))^2 )
#  
# 
# - We have learned some things above and can simplify this arithmetic to:
# 
#  B1 = covariance(x, y) / variance(x)
#  
#  
#  - we need to estimate a value for B0, also called the intercept as it controls the starting point of the line where it intersects the y-axis.
#  
#  B0 = mean(y) - B1 * mean(x)
#  

# In[69]:


### 3.  Estimate Coefficients
#B1 = sum((x(i) - mean(x)) * (y(i) - mean(y))) / sum( (x(i) - mean(x))^2 )

# Calculate coefficients
def coefficients(x,y):	
	x_mean, y_mean = mean(x), mean(y)
	b1 = covariance(x, x_mean, y, y_mean) / variance(x, x_mean)
	b0 = y_mean - b1 * x_mean
	return [b0, b1]
    


# In[70]:


coefficients(x,y)


# ### Make Predictions
# 
#  y = b0 + b1 * x

# In[71]:


def simple_linear_regression(x_train,y_train, x_test):
	predictions = list()
	b0, b1 = coefficients(x_train,y_train)
	for row in x_test:
		yhat = b0 + b1 * row
		predictions.append(yhat)
	return predictions


# In[72]:


test = [18,10,4]
type(test)
simple_linear_regression(x,y, test)


# #### cost function

# In[73]:


def compute_cost(X, y, params):
    n_samples = len(y)
    h = X @ params
    return (1/(2*n_samples))*np.sum((h-y)**2)


# In[85]:


import numpy as np
n_features = np.size(x)
n_features

np.zeros(n_features )

len(np.zeros(n_features )), len(x)


# In[86]:


compute_cost(x,y,np.zeros(n_features))


# #### end cost function

# ### Another Way

# In[150]:


# (Input)
X = 1,2,3,4,5
# (Prediction) 
Y = 1,2,3,4,5
x1,y1 = 1,4
x2,y2 = 5,80


# - The first step is to come up with a formula in the form of y = mx + b where x is a known value and y is the predicted value.
# 
# 
# 
# - To calculate the Prediction y for any Input value x we have two unknowns, the m = slope(Gradient) and b = y-intercept(also called bias)

# #### Slope (m = Change in y/ Change in x)
# - The slope of the line is calculated as the change in y divided by change in x, so the calculation will look like
# 
# ![image.png](attachment:image.png)

# In[151]:


m = (y2-y1) / (x2-x1) 
y1,y2,x2,x1
m


# - The y-intercept / bias shall be calculated using the formula y-y1 = m(x-x1)
# 
# ![image.png](attachment:image.png)

# In[153]:


#y-y1 = m(x-x1)

# y = 4 + 19x - 19
# y = 19x - 15

b = -15


# - Once we arrived at our formula, we can verify the same by substituting x for both starting and ending points which were used to calculate the formula as it should provide the same y value.
# 
# ![image.png](attachment:image.png)

# In[156]:


### Intermetidate Prediction

# y = 19x -15

x=1 

(19*1) - 15 

x =5

(19 * 5)


# - These values are different from what was actually there in the training set (understandably as original graph was not a straight line), and if we plot this(x,y) graph against the original graph, the straight line will be way off the original points in the graph of x=2,3, and 4.

# #### Minimizing the Error
# 
# - Least Square Regression is a method which minimizes the error in such a way that the sum of all square error is minimized. Here are the steps you use to calculate the Least square regression.
# 
# - First, the formula for calculating m = slope is
# 
# ![image.png](attachment:image.png)

# So let’s calculate all the values required to come up with the slope(m), first start with calculating values with x
# 
# ![image.png](attachment:image.png)

# - Now let’s calculate the values with y
# 
# ![image.png](attachment:image.png)

# - The availability of these values allows us to calculate Sum of all
# 
# - (x — xmean)*(y — ymean)
# 
# ![image.png](attachment:image.png)

# - Now let’s calculate the denominator part of the equation which is
# 
# - Sum of (x — xmean)**2
# 
# ![image.png](attachment:image.png)

# - So the overall calculation would be
# ![image.png](attachment:image.png)

# - Calculation of y-Intercept
# 
# - The y-intercept is calculated using the formula b = ymean — m * xmean
# 
# ![image.png](attachment:image.png)

# - The overall formula can now be written in the form of y = mx + b as
# 
# ![image.png](attachment:image.png)

# - Using Least Square Regression on X,Y values
# 
# - Let’s see how the prediction y changes when we apply y = 19.2x + (-22.4) on all x values.
# 
# ![image.png](attachment:image.png)

# #### references
# - https://towardsdatascience.com/mathematics-for-machine-learning-linear-regression-least-square-regression-de09cf53757c
# 
# - https://machinelearningmastery.com/implement-logistic-regression-stochastic-gradient-descent-scratch-python/

# https://github.com/Apress/python-projects-for-beginners/blob/master/Week_07.ipynb
# 
# all the concept, sorting, index, tree
# https://github.com/OmkarPathak/Python-Programs
# 
# 
# ML - from scracth implementation 
# https://github.com/dibgerge/ml-coursera-python-assignments
# 
# 
# basic types 
# https://github.com/edyoda/python-assignments

# #### 1
# . Only Vowels: Ask for user input and write a for loop that will output all the vowels within it. For example:
# 
# "Hello" --> "eo" 

# In[35]:


ans = input("Enter a word: ")

for letter in ans:
    if letter in ['a', 'e', 'i', 'o', 'u']:
        print(letter)


# #### 2
# Remove Duplicates: Remove all duplicates from the list below. 
# 
# Hint: Use the .count() method. The output should be ["Bob", "Kenny", "Amanda"]
# 
# >>> names = ["Bob", "Kenny", "Amanda", "Bob", "Kenny"] 

# In[34]:


names = ["Bob", "Kenny", "Amanda", "Bob", "Kenny"]

for name in names:
    while names.count(name) > 1:
        names.remove(name)
        
print(names)


# #### 3 
# Pyramids: Use a for loop to build a pyramid of x's. It should be modular so that if you loop to 5 or 50, it still creates evenly spaced rows. Hint: Multiply the string 'x' by the row. For example, if you loop to the range of 4, it should produce the following result: 
# 
# 
#      x
#     x x
#    x x x
#   x x x x

# In[36]:


row = 5

for i in range(row):
    print(' ' * (row - i) + ' x' * i)


# #### 4
# 
#  Output Names: Write a loop that will iterate over a list of items and only output items which have letters inside of a string. Take the following list for example, only "John" and "Amanda" should be output:
# 
# >>> names = [ "John", " ", "Amanda", 5] 
# 

# In[37]:


names = [ "John", " ", "Amanda", 5]

for name in names:
    if type(name) == str:
        if name.strip() != '':
            print(name)


# #### 5
# 
# Checking Inclusion - Part 2: Ask the user for input and check to see if what they wrote has an 'ing' at the end. Hint: Use slicing. 
# 

# In[38]:


ans = input('Please input a word: ')

if 'ing' in ans[-3:]:
    print('Your word ends with "ing"')
elif 'ing' not in ans[-3:]:
    print('Your word does not end with "ing"')


# #### 6
# Age Group: Ask the user to input their age. Depending on their input, output one of the following groups:
# 
# a. Between 0 and 12 = "Kid"
# 
# b. Between 13 and 19 = "Teenager"
# 
# c. Between 20 and 30 = "Young Adult"
# 
# d. Between 31 and 64 = "Adult"
# 
# e. 65 or above = "Senior"

# In[39]:


age = int(input('How old are you? '))

if age >= 0 and age <= 12:
    print('Kid')
elif age >= 13 and age <= 19:
    print('Teenager')
elif age >= 20 and age <= 30:
    print('Young Adult')
elif age >= 31 and age <= 64:
    print('Adult')
elif age >= 65:
    print('Senior')


# ##### 7
# 
# Check given string is Palindrome or not

# In[42]:


def PalindromeTester(text):
    text2 = text [::-1]
    if text2 == text:
        return True
    else:
        return False
text = input ("Enter Text: ")
print("Text {} is Palindrome".format(text)) if PalindromeTester(text) else print ("Text {} is not a Palindrome".format(text))


# ##### 8
# Selection sort

# In[48]:


#Author: OMKAR PATHAK
#This program shows an example of selection sort

#Selection sort iterates all the elements and if the smallest element in the list is found then that number
#is swapped with the first

#Best O(n^2); Average O(n^2); Worst O(n^2)

def selectionSort(List):
    for i in range(len(List) - 1): #For iterating n - 1 times
        minimum = i
        for j in range( i + 1, len(List)): # Compare i and i + 1 element
            if(List[j] < List[minimum]):
                minimum = j
        if(minimum != i):
            List[i], List[minimum] = List[minimum], List[i]
    return List

if __name__ == '__main__':
    List = [3, 4, 2, 6, 5, 7, 1, 9]
    print('Sorted List:',selectionSort(List))


# ##### 9
# 
# Write a Python script to generate and print a dictionary that contains a number (between 1 and n) in the form (x, x*x)
# 
#     Sample Dictionary ( n = 5) :
#     Expected Output : {1: 1, 2: 4, 3: 9, 4: 16, 5: 25}
# 

# In[44]:


n=int(input("Input a number: "))
d = dict()

for x in range(1,n+1):
    d[x]=x*x

print(d)


# ##### 10
# Write a Python program that prints all the numbers from 0 to 6 except 3 and 6.
# 
#     Note : Use 'continue' statement.
#     Expected Output : 0 1 2 4 5
# 

# In[45]:


for x in range(6):
    if (x == 3 or x==6):
        continue
    print(x,end=' ')
print("\n")


# ##### 11
# 
# Write a Python program to get the Fibonacci series between 0 to 50.
# Note : The Fibonacci Sequence is the series of numbers :
# 
#     0, 1, 1, 2, 3, 5, 8, 13, 21, ....
#     Every next number is found by adding up the two numbers before it.
#     Expected Output : 1 1 2 3 5 8 13 21 34
# 

# In[46]:


x,y=0,1

while y<50:
    print(y)
    x,y = y,x+y


# ##### 12
#  Write a Python program to construct the following pattern, using a nested loop number.
# 
# 1 22 333 4444 55555 666666 7777777 88888888 999999999

# In[47]:


for i in range(10):
    print(str(i) * i)


# ##### 13
# Write a program to identify duplicate values from list
# 

# In[52]:


my_list = [1, 2, 3, 2, 1, 4, 5, 6, 5, 1, 2]
repeats = []
for i in my_list:
    pop = my_list.pop()
    if pop in repeats:
        continue
    elif pop in my_list:
        repeats.append(pop)
    repeats.sort()
repeats


# 

# ##### 14
# 
# - Is pangram.
# -  Checks whether a phrase is pangram, that is, if
# - it contains all the letters of the alphabet.

# In[33]:


def is_pangram( phrase ):
    abec = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' ]

    for a in phrase:
        if a in abec:
            i = abec.index( a )
            abec.pop( i )

    if len( abec ) == 0:
        return True

    return False


# In[34]:


word = input("enter any word : ")
is_pangram(word)


# ##### 15
# - Filter long words.
# - Receives a list of words and an integer `n` and returns
# - a list of the words that are longer than n.
# 

# In[37]:


def filter_long_words( words, x ):

    result = []

    for word in words:
        if len( word ) >= x:
            result.append( word )

    return result


# In[39]:


filter_long_words(['splash', 'training', 'institue', 'chennai'], 8)


# ###### 16.
# - Generate n chars.
# - Generates `n` number of characters of the given one.

# In[41]:


def generate_n_chars( times, char ):

    output = ''

    while( times > 0 ):
        output += char
        times = times - 1

    return output
generate_n_chars(5, 's')


# ##### 17. Historigram.
# - Takes a list of integers and prints a historigram of it.
# -   historigram( [ 1, 2, 3 ] ) ->
# -       *
# -      **
# -      ***

# In[49]:


def historigram( items ):

    for x in items:
        chars = generate_n_chars( x, '*' )
        print( chars )
historigram(range(1, 10) )
historigram([1,2,3])


# ###### 18. Max in list.
# - Gets the larges number in a list of numbers.

# In[50]:


def max_in_list( list ):

    max = list[ 0 ]

    for x in list:
        if x > max:
            max = x

    return max

max_in_list([10, 2, 3, 43, 1])


# ###### 19 MAP
#    - Map strings
# - Maps a list of words into a list of integers representing each
# - word length.
# - It uses the `map()` higher-order-function.

# In[59]:


def map_words_v1( words ):
    return map( lambda word : (len(word),word) , words )

op = map_words_v1(['splash', 'trainig'])
list(op)


# ###### 20
# - Does the same as #27 but uses `List comprehensions`.

# In[62]:


def map_words_v2( words ):
    return [( len( word ), word) for word in words ]

map_words_v2(['splash', 'training'])


# ###### 21 
# -  Using high-order-functions, this function receives a list of words,
# - and return the longest one.

# In[66]:


def find_longest_word_advanced( words ):

    largest = max( words, key = len )
    print(largest)
    return len( largest )


# In[67]:


find_longest_word_advanced(['splash', 'traininig'])


# ###### 22
# Filter Long Words.
# - Takes a list of words and an integer and then return
# - an array with all the words that are longer in length that the integer passed.

# In[72]:


def filter_long_words_advanced( words, x ):

    validate = lambda word: len( word ) >= x
    filtered_words = filter( validate, words )

    return list(filtered_words)


# In[74]:


filter_long_words_advanced(['splash', 'traininig'], 7)


# ##### 23
# ##### cumulative product
# - write a funtion a to compute product list of numbers
# - input [1,2,3,4]
# - output [1,2,6,24]

# In[29]:


list=[1,2,3,4]
new_list=[] 
j=1
for i in range(0,len(list)):
    j*=list[i]
    new_list.append(j) 
     
print(new_list) 


# ##### 24
# -- use RE
# 
# - find all the elements which contains both e and n 
# 
# - input - ['goal','new','user','sit','eat','dinner']
# - output - ['new','dinner']

# In[50]:


import re
items = ['goal','new','user','sit','eat','dinner']



for itm in items:
    if re.findall('e+', itm):
        itm
#     re.findall('e(.*?)n', itm, re.DOTALL)


# In[ ]:





# #### Advanced Reference
# 
# 1 . https://github.com/PabloVallejo/python-exercises/tree/master/sections
#     
