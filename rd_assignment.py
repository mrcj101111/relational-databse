import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import argparse


class PostgresDb:

    def existing_database(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-db_name', action='store', dest='db_name', help='Database name', required=True)
        parser.add_argument('-username', action='store', dest='username', help='username', required=True)
        parser.add_argument('-password', action='store', dest='password', help='password', required=True)
        args = parser.parse_args()
        connection = psycopg2.connect(dbname=args.db_name, user=args.username,
                                      host='localhost', password=args.password)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection
        # self.cursor.execute('CREATE DATABASE %s ;' % new_database)

    def read_sql_file(self, connection):
        sql_script = open('products_table.sql', 'r').read().replace('\n', '').split(';')[:-1]
        cursor = connection.cursor()
        for line in sql_script:
            cursor.execute(line)


initialize = PostgresDb().existing_database()
execute_sql_file = PostgresDb().read_sql_file(connection=initialize)
