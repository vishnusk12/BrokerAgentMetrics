# -*- coding: utf-8 -*-
"""
Created on Wed Jan 04 12:19:07 2017

@author: vishnu.sk
"""
from pymongo import MongoClient
import csv
db_client = MongoClient("52.91.122.15", 27017)
input_file = csv.DictReader(open("data.csv"))
list = []
for row in input_file:
    dict={}
    dict["ListOfficeName"] = row["_id"]
    dict["NumberOfTransactions"] = row["Count"]
    list.append(dict)
for k in list:
    stats = {'_id':k["ListOfficeName"], 'stats': k}
    db_client.listingoffices.unique_listingoffices.insert_one(stats)
