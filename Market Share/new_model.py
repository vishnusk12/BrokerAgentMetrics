
from pymongo import MongoClient
db_client = MongoClient("52.91.122.15", 27017)
pipeline = [{"$match":{'StateOrProvince':{"$ne":None},
                       'StateOrProvince':{"$ne":""},
                       'ClosePrice' : {"$ne": 0},
                        'ListPrice' : {"$ne": 0},
                       'ListOfficeName':{"$ne":None},
                       'ListAgentFullName':{"$ne":None},
                        'ListAgentFullName':{"$ne":""},
                       'ListOfficeName':{"$ne":""}}},
                    {"$group":
                    {
                     "_id" : {'State':"$StateOrProvince",
                     'ListOfficeName':"$ListOfficeName"},
                     'ListingAgents':{"$addToSet":"$ListAgentFullName"},
                     "NumberOfTransactions":{"$sum":1}}},
                     {"$project":{"_id":1,"ListingAgents":1,"NumberOfTransactions":1,"NumberOfAgents":{ "$size": "$ListingAgents" }}}]

Data = list(db_client.MLSLite.Market_Share_new.aggregate(pipeline,allowDiskUse=True))
for record in Data:
    new_Data = {"_id":record["_id"], "ListingAgents":record["ListingAgents"], "NumberOfAgents":record["NumberOfAgents"], "NumberOfTransactions":record["NumberOfTransactions"] }
    db_client.listingoffices.listingofficestats.insert_one(new_Data)
