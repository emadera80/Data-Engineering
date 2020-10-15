import configparser 
import psycopg2
from sql_queries import test_queries 

def start_test(cur, conn): 
    """ 
        Get the number of records for each table 

    """

    for query in test_queries: 
        cur.execute(query)
        results = cur.fetchone() 
        for row in results: 
            print('{} returns {} records'.format(query, row))

def main(): 
    """
        Testing
    """

    config=configparser.ConfigParser() 
    config.read('config.cfg')

    conn = psycopg2.connect('host={} dbname={} user={} password={} port={}'.format(*config['CLUSTER'].values()))
    cur = conn.cursor() 

    start_test(cur, conn)

    conn.close() 

if __name__=='__main__':
    main() 