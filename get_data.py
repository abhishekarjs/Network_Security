import os
import sys
import json
import numpy
from dotenv import load_dotenv
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
#print(MONGO_DB_URL)
import certifi
ca = certifi.where()
import pandas as pd
import numpy as np
import pymongo

from networksecurity.exceptions.exception import NetworkSecurityException
from networksecurity.loggers.logger import logging

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def csv_to_json_convertor(self,file_path):
        try:
            df = pd.read_csv(file_path)
            df.reset_index(drop =True,inplace= True)
            records = list(json.loads(df.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def pushing_data_to_mongo(self,records,database,collection):
        try:
            self.records = records
            self.database = database
            self.collection = collection
            
            self.mongo_client  =  pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            return len(self.records)
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
if __name__=="__main__":
    FILE_PATH = "./Network_Data/Networkdata.csv"
    DATABASE = "NetworkSec"
    COLLECTION = "NetworkData"
    netwrkobj = NetworkDataExtract()
    records = netwrkobj.csv_to_json_convertor(FILE_PATH)
    total_records = netwrkobj.pushing_data_to_mongo(records,DATABASE,COLLECTION)
    print(total_records)