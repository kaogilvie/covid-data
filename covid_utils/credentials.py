import os

def get_postgres_creds(env='local'):
    if env == 'prod':
        creds = {
                'user': os.getenv('COVID_HEROKU_USER'),
                'password': os.getenv('COVID_HEROKU_DB_PW'),
                'host': os.getenv('COVID_HEROKU_HOST'),
                'port': os.getenv('COVID_HEROKU_PORT'),
                'database': os.getenv('COVID_HEROKU_DB')
            }
    elif env == 'local':
        creds = {
                'user': os.getenv('COVID_DB_USER'),
                'password': os.getenv('COVID_DB_PASSWORD'),
                'host': 'localhost',
                'port': '5432',
                'database': 'covid'
        }
    else:
        print(f"Connection configuration error. Using {env} instead of defined environments.")
        raise Exception

    return creds
