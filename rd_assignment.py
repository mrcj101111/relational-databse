import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import argparse

class PostgresDb:
    def __init__(self, username, password, db_name):
        connection = psycopg2.connect(dbname='postgres', user=username, host='localhost', password=password)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute('CREATE DATABASE rd_assignment')

    def read_sql_file(self):
        sql_file = open('products_table.sql', 'r').read()
        print(sql_file)

        for command in sql_file:
            self.cursor.execute(command)


parser = argparse.ArgumentParser()
parser.add_argument('-username', action='store', dest='username', help='database username')
parser.add_argument('-password', action='store', dest='password', help='database password')
parser.add_argument('-db_name', action='store', dest='db_name', help='database name')
args = parser.parse_args()
initialize = PostgresDb(username=args.username, password=args.password, db_name=args.db_name)
