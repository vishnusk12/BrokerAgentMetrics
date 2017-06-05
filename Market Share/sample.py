# -*- coding: utf-8 -*-
"""
Created on Thu Dec 22 09:53:14 2016

@author: vishnu.sk
"""

from pymongo import MongoClient
db_client = MongoClient("52.91.122.15", 27017)
Data_ = list(db_client.MLSLite.Market_Share_new.find({},{'ListOfficeName':'TheMLSonline.com','ListPrice':1,'ClosePrice':1,'lsratio':1,'ListAgentFullName':1}))