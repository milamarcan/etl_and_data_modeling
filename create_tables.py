import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read("database.cfg.template")

db_host_def = config.get("postgres_def", "host")
db_name_def = config.get("postgres_def", "dbname")
db_user_def = config.get("postgres_def", "user")
db_password_def = config.get("postgres_def", "password")

db_host = config.get("postgres_db1", "host")
db_name = config.get("postgres_db1", "dbname")
db_user = config.get("postgres_db1", "user")
db_password = config.get("postgres_db1", "password")


def create_database():
    """
    Description: Creates database and connects to it

    Arguments: None

    Returns: Connection and cursor to database
    """

    # connect to default database
    conn = psycopg2.connect(
        "host=db_host_def dbname=db_name_def user=db_user_def password=db_password_def"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create new database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS db_name")
    cur.execute("CREATE DATABASE db_name WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to new database
    conn = psycopg2.connect(
        "host=db_host dbname=db_name user=db_user password=db_password"
    )
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: Drops each table using the queries in `drop_table_queries` list.

    Arguments:
        cur: the cursor object
        conn: connection to the database

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: Drops (if exists) and Creates the database; establishes connection
    with the database and gets cursor to it; drops all the tables;
    creates all tables needed; closes the connection.

    Arguments:
        None

    Returns:
        None
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
