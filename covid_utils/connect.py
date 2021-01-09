import sqlalchemy
import psycopg2 as pg


def dbconn(creds, env='local'):
    """
    Pass in raw SQL to this connection.
    """
    if env == 'prod':
        conn = pg.connect(creds['host'], sslmode='require')
    else:
        conn = pg.connect(**creds)
    return conn

def pandas_dbconn(creds, env='local'):
    """
    DB is the name of the database as referred to in the creds dictionary.
    Defaults to postgres protocol; can force it to use the redshift-sqlalchemy shim library.
    """
    protocol = 'postgresql://'

    if env == 'prod':
        postgres_uri = creds['host']
    else:
        postgres_uri = protocol+f"{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"

    dbconn = sqlalchemy.create_engine(postgres_uri)
    return dbconn
