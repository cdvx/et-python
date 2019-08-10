The application runs on python 3.7

## About
This is a command line application to demonstrate a sample ETL pipeline in python. 
It takes a PostgreSQL dataset that is provided by [movies.sql](https://raw.githubusercontent.com/cdvx/etl-python/movies-sql/movielens.sql) and transfers the data to MongoDB.

## Prerequisties
Create a new database and run [movies.sql](https://raw.githubusercontent.com/cdvx/etl-python/movies-sql/movielens.sql)

```
wget https://raw.githubusercontent.com/cdvx/etl-python/movies-sql/movielens.sql
createdb movielens
psql -d movielens < movielens.sql
```

**These conditions should be met too:**
- [x] A MongoDB server is running
- [x] Make sure you configure ```config.py``` with appropirate variables
- [x] Activate environment and Install the dependencies 

```
pipenv --python 3.7
pipenv shell
pipenv install
```

## How to run
In the terminal, type
```
python3 run.py
```

## Author
CedricLusiba
