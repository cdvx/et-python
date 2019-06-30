#!/usr/bin/python3
"""This data pipeline uses movie lens database. Takes PostgreSQL data and puts it into mongoDB"""

# Author : cdvx

# Local libraries
from .db import MONGOConnection, PSQLConnection
from config import Config
from .etl_helpers import *

def main():
    """main method starts a pipeline, extracts data,
    transforms it and loads it into a mongo client"""
    if Config.PRINT_INFO:
        print('Starting data pipeline')
        print('Initialising  connection')
    psql_conn = PSQLConnection()
    
    if Config.PRINT_INFO:
        print('PSQL connection Completed')
        print('Starting data pipeline stage 1 : Extracting data from PSQL')
    
    PSQL_data = extract_data(psql_conn)
    
    if Config.PRINT_INFO:
        print('Stage 1 completed! Data successfully extracted from PSQL')
        print('Starting data pipeline stage 2: Transforming data from PSQL for MongoDB')
        print('Transforming genres dataset')
    genres_collection = transform_data(PSQL_data, "genres")
    
    if Config.PRINT_INFO:
        print('Successfully transformed genres dataset')
        print('Transforming users dataset')
    users_collection = transform_data(PSQL_data, "users")
    
    if Config.PRINT_INFO:
        print('Successfully transformed users dataset')
        print('Transforming movies dataset')
    movies_collection = transform_data(PSQL_data, "movies")
    
    if Config.PRINT_INFO:
        print('Successfully transformed users dataset')
        print('Stage 2 completed! Data successfully transformed')
        print('Intialising MongoDB connection')
    mongo_conn = MONGOConnection().db()
    
    if Config.PRINT_INFO:
        print('MongoDB connection Completed')
        print('Starting data pipeline stage 3: Loading data into MongoDB')
    result = MONGOConnection.load_data(mongo_conn['genres'], genres_collection)
    
    if Config.PRINT_RESULTS:
        print('Successfully loaded genres')
        print(result)
    result = MONGOConnection.load_data(mongo_conn['users'], users_collection)
    
    if Config.PRINT_RESULTS:
        print('Successfully loaded users')
        print(result)
    result = MONGOConnection.load_data(mongo_conn['movies'], movies_collection)
    
    if Config.PRINT_RESULTS:
        print('Successfully loaded movies')
        print(result)
    
    if Config.PRINT_INFO:
        print('Stage 3 completed! Data successfully loaded')
        print('Closing PSQL connection')

    psql_conn.cursor.close()
    # mongo_conn.close()
    if Config.PRINT_INFO:
        print('PSQL connection closed successfully')
        print('Ending data pipeline')
