import os
import json
import numpy
from dotenv import load_dotenv
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
import certifi
ca = certifi.where()
import pandas as pd
import numpy as np
import pymongo

from networksecurity.exceptions.exception import NetworkSecurityException
from networksecurity.logger.logger import logging

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def csv_to_json_convertor(self):
        try:
            pass
        except:
            raise NetworkSecurityException(e,sys)
        
    def pushing_data_to_mongo(Self):
        try:
            pass
        except:
            raise NetworkSecurityException(e,sys)
    
    if __name__=="__main__":
        pass