import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import argparse
import csv


class PostgresDb:

    def connect_to_db(self):
        connection = psycopg2.connect(dbname=args.db_name, user=args.username,
                                      host='localhost', password=args.password)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

    def new_database(self):
        connection = psycopg2.connect(dbname='postgres', user=args.username, host='localhost', password=args.password)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute('CREATE DATABASE %s ;' % args.db_name)


class SqlQueries:

    def read_sql_file(self, connection):
        sql_script = open('products_table.sql', 'r').read().replace('\n', '').split(';')[:-1]
        cursor = connection.cursor()
        for line in sql_script:
            cursor.execute(line)

    def add_column(self):
        connection = psycopg2.connect(dbname=args.db_name, user=args.username,
                                      host='localhost', password=args.password)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE products ADD is_active boolean null;")
        connection.commit()

    def write_to_tabel(self):
        connection = psycopg2.connect(dbname=args.db_name, user=args.username,
                                      host='localhost', password=args.password)
        cursor = connection.cursor()
        sql_file = open('db_entries.sql', 'r').read()
        cursor.execute("INSERT INTO products (id, created, modified, description, amount, is_active) VALUES %s ;"
                       % sql_file)
        connection.commit()

    def stdout_print_out(self):
        connection = psycopg2.connect(dbname=args.db_name, user=args.username,
                                      host='localhost', password=args.password)
        cursor = connection.cursor()

        # cursor.execute("SELECT * FROM products")
        # rows = cursor.fetchall()
        # with open('output.csv', 'w') as fp:
        #     csv_writer = csv.writer(fp)
        #     csv_writer.writerows(rows)

        sql = "COPY (SELECT * FROM products) TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open('output.csv', 'w') as file:
            cursor.copy_expert(sql, file)


parser = argparse.ArgumentParser()
parser.add_argument('-username', action='store', dest='username', help='username', required=True)
parser.add_argument('-password', action='store', dest='password', help='password', required=True)
parser.add_argument('-db_name', action='store', dest='db_name', help='database name', required=True)
args = parser.parse_args()
# new_db = PostgresDb().new_database()
# initialize = PostgresDb().connect_to_db()
# execute_sql_file = PostgresDb().read_sql_file(connection=initialize)
#add_column = PostgresDb().add_column()
#add_entries = PostgresDb().write_to_tabel()
print_out = SqlQueries().stdout_print_out()
