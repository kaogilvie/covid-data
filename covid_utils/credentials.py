import os

def get_postgres_creds():
    return {
        'user': os.getenv('COVID_DB_USER'),
        'password': os.getenv('COVID_DB_PASSWORD'),
        'host': 'localhost',
        'port': '5432',
        'database': 'covid'
    }
