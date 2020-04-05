import sqlalchemy
import psycopg2 as pg


def dbconn(creds):
    """
    Pass in raw SQL to this connection.
    """
    conn = pg.connect(**creds)
    return conn

def pandas_dbconn(creds, ssl=False, redshift=False):
    """
    DB is the name of the database as referred to in the creds dictionary.
    Defaults to postgres protocol; can force it to use the redshift-sqlalchemy shim library.
    """
    if redshift is True:
        protocol = 'redshift+psycopg2://'
    else:
        protocol = 'postgresql://'

    postgres_uri = protocol+f"{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
    if ssl is True:
        dbconn = sqlalchemy.create_engine(postgres_uri, connect_args = {'sslmode':'require'})
    else:
        dbconn = sqlalchemy.create_engine(postgres_uri)
    return dbconn
