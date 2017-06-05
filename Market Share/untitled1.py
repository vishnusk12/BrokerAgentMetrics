# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 17:44:16 2017

@author: vishnu.sk
"""

from pymongo import MongoClient
from dbauth import DATABASE_ACCESS
import pandas as pd
import datetime
date_format = "%Y-%m-%d"
now = datetime.datetime.now()
db_client = MongoClient("mongo-master.propmix.io", 27017)
db_client.MLSLite.authenticate(**DATABASE_ACCESS)
pipeline = [{"$match": {'StateOrProvince': {"$ne": None},
                        'StateOrProvince': {"$ne": ""},
                        'City':'Miami',
                        'StandardStatus':'Sold',
                        'CloseDate': {'$gte':'2016-04-01'},
                        'ClosePrice': {"$ne": 0},
                        'ClosePrice': {"$ne": None},
                        'ClosePrice': {"$ne": ""},
                        'ListPrice': {"$ne": ""},
                        'ListPrice': {"$ne": None},
                        'ListPrice': {"$ne": 0},
                        'PostalCode':{"$ne": ""},
                        'PostalCode':{"$ne":None},
                        'PropertySubType': {"$ne": 0},
                        'PropertySubType': {"$ne": ""}}},
 { "$project" : { "ListPrice" : 1 , "CloseDate" : 1 , "StateOrProvince" : 1 , "City" : 1 , "PostalCode" : 1 , "ClosePrice" : 1 , "StandardStatus" : 1, "PropertySubType" : 1}}]
Data_ = list(db_client.MLSLite.mlslite_unique.aggregate(pipeline))
df = pd.DataFrame(Data_)

def cleanup(df):
    df = df[df.PropertySubType.notnull()]
    df = df[df.PropertySubType != ""]
    df = df[df.PostalCode != ""]
    df = df[df.PostalCode.notnull()]
    df = df.dropna()
    return df

df = cleanup(df)
unique_properties = df['PropertySubType'].unique()
unique_zip = df['PostalCode'].unique()
spans = [1, 2, 3, 6, 12]