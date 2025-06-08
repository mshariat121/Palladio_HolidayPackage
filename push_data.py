import json
import os
import sys

from dotenv import load_dotenv

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")

print(MONGO_DB_URL)

import certifi

ca = certifi.where()

import numpy as np
import pandas as pd
import pymongo

from Palladio_HolidayPackage.exception.exception import (
    Palladio_HolidayPackage_Exception,
)
from Palladio_HolidayPackage.logging.logger import logging


class PalladioDataExtract:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise Palladio_HolidayPackage_Exception(e, sys)

    def csv_to_jason_convertor(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise Palladio_HolidayPackage_Exception(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        try:
            self.records = records
            self.database = database
            self.collection = collection

            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[self.database]

            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            return len(self.records)
        except Exception as e:
            raise Palladio_HolidayPackage_Exception(e, sys)


if __name__ == "__main__":
    FILE_PATH = "Data/Travel.csv"
    DATABASE = "MO121AI"
    COLLECTION = "Data"
    palladiObj = PalladioDataExtract()
    records = palladiObj.csv_to_jason_convertor(file_path=FILE_PATH)
    print(records)
    no_of_records = palladiObj.insert_data_mongodb(records, DATABASE, COLLECTION)
    print(no_of_records)
