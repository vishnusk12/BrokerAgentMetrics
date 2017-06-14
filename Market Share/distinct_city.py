

from pymongo import MongoClient
from dbauth import DATABASE_ACCESS
db_client = MongoClient("mongo-master.propmix.io", port=33017)
db_client.MLSLite.authenticate(**DATABASE_ACCESS)
data = list(db_client.MLSLite.mlslite_unique.distinct('City'))
data = [x.lower() for x in data if x is not None and x]
