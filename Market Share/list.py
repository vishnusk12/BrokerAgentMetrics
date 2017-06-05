# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 11:07:09 2017

@author: vishnu.sk
"""

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
pipeline = [{"$match": {'StateOrProvince': {"$ne": None},
                        'StateOrProvince': {"$ne": ""},
                        'ListOfficeName' : ListOfficeName,
                        'CloseDate': {'$gte':'2015-04-01'},
                        'StandardStatus': 'Sold',
                        'StandardStatus': 'Active',
                        'ClosePrice': {"$ne": 0},
                        'ListPrice': {"$ne": 0},
                        'ListAgentFullName': {"$ne": None},
                        'ListAgentFullName': {"$ne": ""}}},
            {"$project": {'ListPrice': 1, 'CloseDate': 1, 'StateOrProvince': 1, 'City': 1, 'lsratio': 1 , 'PostalCode': 1, 'ClosePrice': 1, 'StandardStatus': 1, 'PropertySubType': 1, 'ListOfficeName': 1, 'ListAgentFullName': 1}}]
Data_ = list(db_client.MLSLite.mlslite_unique.aggregate(pipeline, allowDiskUse=True))