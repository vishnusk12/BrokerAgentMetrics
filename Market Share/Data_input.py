
from pymongo import MongoClient
db_client = MongoClient("52.91.122.15", 27017)
Data_Aggregation  = [{"$match":
                                                 {   "StandardStatus":'Sold'  ,
                                                      'CloseDate':{"$gte":'2015-04-01'},
                                                      "ListAgentFullName":{
                                    "$ne":""},"ListOfficeName":{"$ne":""},"ListOfficeName":{"$ne":None},"ListAgentFullName":{"$ne":None},"ClosePrice":{"$ne":None},"ListPrice":{"$ne":None},"price_sqft":{"$ne":None},"LivingArea":{"$ne":None},"location":{"$ne":None}


                                             }}]
Data_ = list(db_client.MLSLite.mlslite_unique.aggregate(Data_Aggregation))
for i in Data_:
    db_client.Market_Share.New_Data.insert_one({"ListPrice": i["ListPrice"], "CloseDate": i["CloseDate"], "StateOrProvince": i["StateOrProvince"], "City": i["City"], "price_sqft": i["price_sqft"], "PropertyType": i["PropertyType"], "Latitude": i["Latitude"], "Longitude": i["Longitude"], "property_id": i["property_id"], "lsratio": i["lsratio"], "PostalCode": i["PostalCode"], "ClosePrice": i["ClosePrice"], "StandardStatus": i["StandardStatus"], "LotSizeSquareFeet": i["LotSizeSquareFeet"], "SI_FIPS": i["SI_FIPS"], "LivingArea": i["LivingArea"], "PropertySubType": i["PropertySubType"], "ListOfficeName": i["ListOfficeName"], "ListAgentFullName": i["ListAgentFullName"], "location": i["location"] })
