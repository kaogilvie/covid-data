import os

def get_postgres_creds(local=True):
    if local is False:
        creds = {
                'user': os.getenv('AWS_COVID_USER'),
                'password': os.getenv('AWS_COVID_DB_PW'),
                'host': os.getenv('AWS_COVID_HOST'),
                'port': os.getenv('AWS_COVID_PORT'),
                'database': os.getenv('AWS_COVID_DB')
            }
    else:
        creds = {
                'user': os.getenv('COVID_DB_USER'),
                'password': os.getenv('COVID_DB_PASSWORD'),
                'host': 'localhost',
                'port': '5432',
                'database': 'covid'
        }

    return creds
