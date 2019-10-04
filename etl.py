import configparser
import psycopg2
import sys
from sql_queries import copy_dim_table_queries,  truncate_dim_table_queries, copy_fact_table_queries,  truncate_fact_table_queries


def truncate_dim_tables(cur, conn):
    """
        This function truncates the dim tables
        Parameters:
                cur: cursor or query
                conn: connection to the database
        Returns:
                None
    """
    print('start truncate dim tables')
    for query in truncate_dim_table_queries:
        print('running: ' + query)    
        cur.execute(query)
        conn.commit()


def load_dim_tables(cur, conn):
    """
        this functions copies all data from the CSV files in the dim tables
        Parameters:
                cur: cursor or query
                conn: connection to the database
        Returns:
                None
    """
    print('start load dim tables')
    for query in copy_dim_table_queries:
        print('running: ' + query) 
        cur.execute(query)
        conn.commit()


def truncate_fact_tables(cur, conn):
    """
        This function truncates the fact tables
        Parameters:
                cur: cursor or query
                conn: connection to the database
        Returns:
                None
    """
    print('start truncate fact tables')
    for query in truncate_fact_table_queries:
        print('running: ' + query)    
        cur.execute(query)
        conn.commit()


def load_fact_tables(cur, conn):
    """
        this functions copies all data from the CSV files in the dim tables
        Parameters:
                cur: cursor or query
                conn: connection to the database
        Returns:
                None
    """
    print('start load fact tables')
    for query in copy_fact_table_queries:
        print('running: ' + query) 
        cur.execute(query)
        conn.commit()

def main():
    """
        This is the main procedure that executes the truncate of tables and loading of tables
        Parameters:
                None
        Returns:
                None
    """
    RunDimOrFact = str(sys.argv[1])
    
    print('start etl process')
    config = configparser.ConfigParser()
    print(config)
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    cur = conn.cursor()
    
    if  RunDimOrFact == 'dim':
        truncate_dim_tables(cur, conn)
        load_dim_tables(cur, conn)
    elif RunDimOrFact == 'fact':
        truncate_fact_tables(cur, conn)
        load_fact_tables(cur, conn)    
    
    

    conn.close()


if __name__ == "__main__":
    print('running ETL process')
    main()
    print('finished ETL process')