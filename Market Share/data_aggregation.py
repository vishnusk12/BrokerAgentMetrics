# -*- coding: utf-8 -*-
"""
Created on Wed Jan 04 14:24:02 2017

@author: vishnu.sk
"""

from pymongo import MongoClient
db_client = MongoClient("52.91.122.15", 27017)
pipeline = [{"$match": {"StandardStatus":'Sold', 
                       'CloseDate':{'$gte':'2015-04-01'}, 
                        "ListAgentFullName":{"$ne":None},
                        "ListAgentFullName":{"$ne":""},
                        "ListOfficeName":{"$ne":None},
                        "ListOfficeName":{"$ne":""},
                        "PropertySubType":{"$ne":None},
                        "PropertySubType":{"$ne":""},
                        "ClosePrice":{"$ne":None},
                        "ClosePrice":{"$ne":""},
                        "ListPrice":{"$ne":""},
                        "ListPrice":{"$ne":None}}},
            {"$group":
            {
            "_id" : "$ListOfficeName",
            "Count":{'$sum':1} }}]
data = list(db_client.MLSLite.mlslite_unique.aggregate(pipeline))