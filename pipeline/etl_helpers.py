
"""Module with helper functions for ETL process"""
# System libraries
import copy, json

def extract_data(psql_conn):
    """Given a cursor, Extracts data from PostgreSQL movielens dataset
    and returns all the tables with their data"""
    genres = psql_conn.query_all("genres")
    movie_genres = psql_conn.query_all("genres_movies")
    movies = psql_conn.query_all("movies")
    ratings = psql_conn.query_all("ratings")
    users = psql_conn.query_all("users")
    tables = (genres, movie_genres, ratings, users, movies)
    return tables

def transform_genres(dataset, dataset_collection, tmp_collection):
    for item in dataset[0]:
        tmp_collection['_id'] = item[0]
        tmp_collection['genre'] = item[1]
        dataset_collection.append(copy.copy(tmp_collection))
    return dataset_collection

def transform_users(dataset, dataset_collection, tmp_collection):
    for item in dataset[3]:
        tmp_collection['_id'] = item[0]
        tmp_collection['age'] = item[1]
        tmp_collection['gender'] = item[2]
        tmp_collection['occupatin'] = item[3]
        tmp_collection['zip_code'] = item[4]
        dataset_collection.append(copy.copy(tmp_collection))
    return dataset_collection

def transform_movies(dataset, dataset_collection, tmp_collection):
    for item in dataset[4]:
        tmp_collection['_id'] = item[0]
        tmp_collection['title'] = item[1]
        tmp_collection['release_date'] = json.dumps(item[2], indent=4, sort_keys=True, default=str)
        # tmp_collection['video'] = item[3]
        # tmp_collection['IMDBURL'] = item[4]
        # embedding movie genres
        movie_genres_collection = []
        for movie_genres_item in dataset[1]:
            if movie_genres_item[0] == tmp_collection['_id']:
                movie_genres_collection.append(copy.copy(movie_genres_item[1]))
        tmp_collection['genres'] = movie_genres_collection
        # embedding ratings
        ratings_collection = []
        for ratings_item in dataset[2]:
            if ratings_item[1] == tmp_collection['_id']:
                tmp_ratings_collection = {}
                tmp_ratings_collection['user_id'] = ratings_item[0]
                tmp_ratings_collection['rating'] = ratings_item[2]
                tmp_ratings_collection['timestamp'] = ratings_item[3]
                ratings_collection.append(copy.copy(tmp_ratings_collection))
        tmp_collection['ratings'] = ratings_collection
        dataset_collection.append(copy.copy(tmp_collection))
    return dataset_collection



def transform_data(dataset, table):
    """Transforms the data to load it into mongoDB, returns a JSON object"""
    dataset_collection = []
    tmp_collection = {}
    table_transform_mapper ={
        "genres": transform_genres,
        "users": transform_genres,
        "movies": transform_movies
    }
    return table_transform_mapper[
        table](dataset,
               dataset_collection,
               tmp_collection)