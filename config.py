#!/usr/bin/python3
"""Config for data pipeline"""


class Config:
    PSQL_USER = "postgres"
    PSQL_PASSWORD = ""
    DB_HOST = "localhost"
    PSQL_DB = "movielens"
    PSQL_PORT = 5432
    MONGO_PORT = 27017
    MONGO_DB = "movielens"

    # Pipeline constants
    RESET_MONGO_COLLECTIONS_ON_UPDATE = True # Resets the collections if a collection already exists, if false, the data is appeneded to the collection
    PRINT_INFO = True # Print options for debugging purposes
    PRINT_RESULTS = True # Print option for debugging purposes


