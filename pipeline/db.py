#!/usr/bin/python3
"""Module with database connection classess"""

# System libraries
import os, copy

# Third-party libraries
import psycopg2
from pymongo import MongoClient


from config import Config
VARS = [ 
    Config.PSQL_DB,
    Config.DB_HOST,
    Config.PSQL_PASSWORD,
    Config.PSQL_PORT,
    Config.PSQL_USER,
    Config.MONGO_PORT,
    Config.MONGO_DB
    ]

class PSQLConnection(object):
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                dbname=VARS[0],
                user=VARS[4],
                host=VARS[1],
                password=VARS[2],
                port=VARS[3]
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        except:
            print("Cannot connect to database.")   

    
    def query_all(self, tablename):
        queries = []
        try:
            self.cursor.execute(f"SELECT * FROM {tablename}")
            items = self.cursor.fetchall()
            if items:
                query_gen = [queries.append(item) for item in items]
                return queries
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        else:
            return queries

    
    def drop_table(self, tablename):
        try:
            drop_table_command = f"DROP TABLE {tablename} CASCADE"
            self.cursor.execute(drop_table_command)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

class MONGOConnection:
    def __init__(self):
        """Initalises and returns MongoDB database based on config"""
        self.conection =  MongoClient(VARS[1], VARS[5])[VARS[6]]
    
    def db(self):
        return self.conection

    @classmethod
    def load_data(cls, mongo_collection, dataset_collection):
        """Loads the data into mongoDB and returns the results"""
        try:
            if Config.RESET_MONGO_COLLECTIONS_ON_UPDATE:
                mongo_collection.delete_many({})
            
            return mongo_collection.insert_many(dataset_collection)
        except Exception as e :
            print(f'\n\nError >> {e} \n\n')



