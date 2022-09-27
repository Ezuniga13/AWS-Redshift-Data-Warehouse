import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Extract raw data from s3 buckets and stage in redshift.

    Arguments:
    cur -- the cursor for a psycopg2 to execute commands in database.
    conn -- connection to cluster.
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Extract data from staging tables to star schema.
        
    Arguments:
    cur -- the cursor for a psycopg2 to execute commands in database.
    conn -- connection to cluster.
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """Execute ETL process by instantiating functions."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER']
                                    .values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
