# -*- coding: utf-8 -*-
"""
Created on Thu Dec 22 09:53:14 2016

@author: vishnu.sk
"""

import pandas as pd
from pymongo import MongoClient
import datetime

date_format = "%Y-%m-%d"
now = datetime.datetime.now()

db_client = MongoClient("52.91.122.15", 27017)
Data_ = list(db_client.MLSLite.Market_Share_new.find({'ListOfficeName':'TheMLSonline.com'},{'ListPrice':1,'ClosePrice':1,'CloseDate':1,'lsratio':1,'ListAgentFullName':1,'PropertySubType':1, 'StateOrProvince': 1}))
df = pd.DataFrame(Data_)

#class AgentPerforma():

def cleanup(df):
    df = df[df.PropertySubType.notnull()]
    df = df[df.PropertySubType != ""]
    df = df[df.lsratio <= 10]
    df = df.dropna()
    return df

df = cleanup(df)
unique_agents = df['ListAgentFullName'].unique()
unique_properties = df['PropertySubType'].unique()
spans = [1, 3, 6, 12, 18]

def monthdelta(date, delta):
    m, y = (date.month + delta) % 12, date.year + ((date.month) + delta - 1) // 12
    if not m:
        m = 12
    d = min(date.day, [31, 29 if y % 4 == 0 and not y % 400 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return date.replace(day=d, month=m, year=y)

stats = {}

def get_stats(df):
    record = {}
    record["Total_ClosePrice"] = sub_df['ClosePrice'].sum()
    record['Total_ListPrice'] = sub_df['ListPrice'].sum()
    Total_lsratio = sub_df["lsratio"].sum()
    record_count = sub_df.shape[0]
    record['Avg_Sold_to_List_percent'] = 100 * ((float(record_count) / float(Total_lsratio))) if Total_lsratio != 0 else 0
    record['Number_of_Transactions'] = record_count 
    record['Span'] = '%dM' % (monthback)
    return record

for agent in unique_agents:
    stats[agent] = {}
    stats[agent]['All'] = {}
    for property_type in unique_properties:
        stats[agent][property_type] = {}
        for monthback in spans:
            record = stats[agent][property_type]['%dM' % monthback] = {}
            sub_df = df[(df['ListAgentFullName'] == agent) & (df['PropertySubType'] == property_type) & (df['CloseDate'] >= monthdelta(now, -monthback).strftime(date_format)) ]
            record.update(get_stats(sub_df))
            
    for monthback in spans:
        property_type = "All"
        record = stats[agent][property_type]['%dM' % monthback] = {}
        sub_df = df[(df['ListAgentFullName'] == agent) & (df['CloseDate'] >= monthdelta(now, -monthback).strftime(date_format)) ]
        record.update(get_stats(sub_df))
for key, value in stats.items():
    new_stats = {'_id': key, 'activity_stats':value}
    db_client.agents.performance.insert_one(new_stats)
    



'''
dicts = df.to_dict(orient="index")
list_ = []
agents = []
prop_type = []



wanted_keys = ["ClosePrice", "ListPrice", "lsratio", "CloseDate", "ListAgentFullName", "PropertySubType"]
Data = []
for key,list_value in dicts.items():
    new_dict = {k: list_value[k] for k in set(wanted_keys) & set(list_value.keys())}
    Data.append(new_dict)
    agents.append(list_value['ListAgentFullName'])
    uniq_agents = list(set(agents))
    prop_type.append(list_value['PropertySubType'])
    wanted_props = list(set(prop_type))
    
for i in uniq_agents:
    for j in wanted_props:
        counter = 0
        dict = {}
        dict['Total_ClosePrice'] = 0
        dict['Total_ListPrice'] = 0
        dict['Agent_Name'] = i
        dict['PropertySubType'] = j
        Total_lsratio = 0
        spans = [1, 3, 6, 12, 18]
        for records in Data:
            if i == records['ListAgentFullName'] and j == records['PropertySubType'] and ((datetime.datetime.strptime(str(now), date_format) - datetime.datetime.strptime(str(records['CloseDate']), date_format)).days)/30 <= monthback:
                counter += 1
                dict["Total_ClosePrice"] = dict["Total_ClosePrice"] + records["ClosePrice"]
                dict['Total_ListPrice'] = dict['Total_ListPrice'] + records["ListPrice"]
                Total_lsratio = Total_lsratio + records["lsratio"] 
                dict['Avg_Sold_to_List_percent'] = 100 * ((float(counter) / float(Total_lsratio)))
                dict['Number_of_Transactions'] = counter 
                dict['Span'] = '%dM' % (monthback)
                list_.append(dict)
            elif i == records['ListAgentFullName'] and j == records['PropertySubType'] and ((datetime.datetime.strptime(str(now), date_format) - datetime.datetime.strptime(str(records['CloseDate']), date_format)).days)/30 <= 3:
                counter += 1
                dict["Total_ClosePrice"] = dict["Total_ClosePrice"] + records["ClosePrice"]
                dict['Total_ListPrice'] = dict['Total_ListPrice'] + records["ListPrice"]
                Total_lsratio = Total_lsratio + records["lsratio"] 
                dict['Avg_Sold_to_List_percent'] = 100 * ((float(counter) / float(Total_lsratio)))
                dict['Number_of_Transactions'] = counter 
                dict['Span'] = '3M'
                list_.append(dict)
            elif i == records['ListAgentFullName'] and j == records['PropertySubType'] and ((datetime.datetime.strptime(str(now), date_format) - datetime.datetime.strptime(str(records['CloseDate']), date_format)).days)/30 <= 6:
                counter += 1
                dict["Total_ClosePrice"] = dict["Total_ClosePrice"] + records["ClosePrice"]
                dict['Total_ListPrice'] = dict['Total_ListPrice'] + records["ListPrice"]
                Total_lsratio = Total_lsratio + records["lsratio"] 
                dict['Avg_Sold_to_List_percent'] = 100 * ((float(counter) / float(Total_lsratio)))
                dict['Number_of_Transactions'] = counter 
                dict['Span'] = '6M'
                list_.append(dict)
            elif i == records['ListAgentFullName'] and j == records['PropertySubType'] and ((datetime.datetime.strptime(str(now), date_format) - datetime.datetime.strptime(str(records['CloseDate']), date_format)).days)/30 <= 12:
                counter += 1
                dict["Total_ClosePrice"] = dict["Total_ClosePrice"] + records["ClosePrice"]
                dict['Total_ListPrice'] = dict['Total_ListPrice'] + records["ListPrice"]
                Total_lsratio = Total_lsratio + records["lsratio"] 
                dict['Avg_Sold_to_List_percent'] = 100 * ((float(counter) / float(Total_lsratio)))
                dict['Number_of_Transactions'] = counter 
                dict['Span'] = '12M'
                list_.append(dict)
            elif i == records['ListAgentFullName'] and j == records['PropertySubType'] and ((datetime.datetime.strptime(str(now), date_format) - datetime.datetime.strptime(str(records['CloseDate']), date_format)).days)/30 <= 15:
                counter += 1
                dict["Total_ClosePrice"] = dict["Total_ClosePrice"] + records["ClosePrice"]
                dict['Total_ListPrice'] = dict['Total_ListPrice'] + records["ListPrice"]
                Total_lsratio = Total_lsratio + records["lsratio"] 
                dict['Avg_Sold_to_List_percent'] = 100 * ((float(counter) / float(Total_lsratio)))
                dict['Number_of_Transactions'] = counter 
                dict['Span'] = '15M'
                list_.append(dict)
            elif i == records['ListAgentFullName'] and j == records['PropertySubType'] and ((datetime.datetime.strptime(str(now), date_format) - datetime.datetime.strptime(str(records['CloseDate']), date_format)).days)/30 <= 18:
                counter += 1
                dict["Total_ClosePrice"] = dict["Total_ClosePrice"] + records["ClosePrice"]
                dict['Total_ListPrice'] = dict['Total_ListPrice'] + records["ListPrice"]
                Total_lsratio = Total_lsratio + records["lsratio"] 
                dict['Avg_Sold_to_List_percent'] = 100 * ((float(counter) / float(Total_lsratio)))
                dict['Number_of_Transactions'] = counter 
                dict['Span'] = '18M'
                dict['CloseDate'] = records['CloseDate']
                list_.append(dict) 
final_list = []
for k in range(0, len(list_)):
    if list_[k] not in list_[k+1:]:
        final_list.append(list_[k])
final_dict ={}
for prop in wanted_props:
    new_dict = {}
    for value in final_list:
        agent = value['Agent_Name']
        
        if value['PropertySubType'] == prop:
            new_dict[agent] = value
            final_dict[prop] = new_dict
            #final_dict['All'] = final_list
for key_,val in final_dict.items():
    for keys,new_val in val.items():
        new_val.pop('Agent_Name')
        new_val.pop('PropertySubType')
        
        
        
        
for records in Data:




    from datetime import datetime
    date_format = "%Y-%m-%d"
    a = datetime.strptime(str(records['CloseDate']), date_format)
    b = datetime.strptime(str(now), date_format)
    d = b-a'''
    
'''def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0'''    
    
#def date_map(str_date):
   #return unix_time_millis(datetime.datetime.strptime(str_date, date_format))

#df['CloseDateMils'] = map(lambda x: date_map(x), df['CloseDate'])

#print df['CloseDate']

#print df['CloseDateMils']

# df_from_last_year = df[df['CloseDate'] >= monthdelta(now, -12).strftime(date_format)]