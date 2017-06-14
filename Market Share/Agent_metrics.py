
import pandas as pd
from pymongo import MongoClient
import datetime
from dbauth import DATABASE_ACCESS

date_format = "%Y-%m-%d"
now = datetime.datetime.now()
ListOfficeName = 'TheMLSonline.com'
db_client = MongoClient("52.91.122.15", 27017)
db_client.MLSLite.authenticate(**DATABASE_ACCESS)
db_client.listingoffices.authenticate(**DATABASE_ACCESS)
Data_ = list(db_client.MLSLite.Market_Share_new.find({'ListOfficeName': ListOfficeName},{'ListPrice':1,'ClosePrice':1,'CloseDate':1,'lsratio':1,'ListAgentFullName':1,'PropertySubType':1, 'StateOrProvince': 1}))
df = pd.DataFrame(Data_)

def cleanup(df):
    df = df[df.PropertySubType.notnull()]
    df = df[df.PropertySubType != ""]
    df = df[df.lsratio <= 10]
    df = df.dropna()
    return df

df = cleanup(df)
unique_agents = df['ListAgentFullName'].unique()
unique_properties = df['PropertySubType'].unique()
unique_states = df['StateOrProvince'].unique()
spans = [1, 3, 6, 12, 18]

def monthdelta(date, delta):
    m, y = (date.month + delta) % 12, date.year + ((date.month) + delta - 1) // 12
    if not m:
        m = 12
    d = min(date.day, [31, 29 if y % 4 == 0 and not y % 400 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return date.replace(day=d, month=m, year=y)

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

stats = {}
for states in unique_states:
    for agent in unique_agents:
        stats[agent] = {}
        stats[agent]['All'] = {}
        for property_type in unique_properties:
            stats[agent][property_type] = {}
            for monthback in spans:
                record = stats[agent][property_type]['%dM' % monthback] = {}
                sub_df = df[(df['ListAgentFullName'] == agent) & (df['StateOrProvince'] == states) & (df['PropertySubType'] == property_type) & (df['CloseDate'] >= monthdelta(now, -monthback).strftime(date_format)) ]
                record.update(get_stats(sub_df))
                
        for monthback in spans:
            property_type = "All"
            record = stats[agent][property_type]['%dM' % monthback] = {}
            sub_df = df[(df['ListAgentFullName'] == agent) & (df['StateOrProvince'] == states) & (df['CloseDate'] >= monthdelta(now, -monthback).strftime(date_format)) ]
            record.update(get_stats(sub_df))
    for key, value in stats.items():
        new_stats = {'_id':{ListOfficeName,states,{'AgentName': key, 'performance_index':value}}}
        db_client.listingoffices.agentperformance.insert_one(new_stats)
