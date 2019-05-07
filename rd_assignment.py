import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import argparse
import sys
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
        sql = "COPY (SELECT * FROM products) TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open('stdout.csv', 'w') as file:
            cursor.copy_expert(sql, file)

    def write_highest_amount(self):
        connection = psycopg2.connect(dbname=args.db_name, user=args.username,
                                      host='localhost', password=args.password)
        cursor = connection.cursor()
        sql = "COPY (SELECT * FROM products ORDER BY amount DESC) TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open('highest_amount_acs.csv', 'w') as file:
            cursor.copy_expert(sql, file)

    def active_and_above_10(self):
        connection = psycopg2.connect(dbname=args.db_name, user=args.username,
                                      host='localhost', password=args.password)
        cursor = connection.cursor()
        sql = "COPY (SELECT * FROM products WHERE amount > 10 AND is_active = TRUE)" \
              "TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open('active_and_above_10.csv', 'w') as file:
            cursor.copy_expert(sql, file)

    def inactive(self):
        connection = psycopg2.connect(dbname=args.db_name, user=args.username,
                                      host='localhost', password=args.password)
        cursor = connection.cursor()
        sql = "COPY (SELECT description, amount, is_active FROM products WHERE is_active = FALSE) TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open('inactive.csv', 'w') as file:
            cursor.copy_expert(sql, file)


print('Are you connecting to an existing db?')
input = input()
if input == 'yes':
    print('true')
else:
    print('false')
parser = argparse.ArgumentParser()
parser.add_argument('-username', action='store', dest='username', help='username', required=True)
parser.add_argument('-password', action='store', dest='password', help='password', required=True)
parser.add_argument('-db_name', action='store', dest='db_name', help='database name', required=True)
args = parser.parse_args()
#new_db = PostgresDb().new_database()
#initialize = PostgresDb().connect_to_db()
#execute_sql_file = SqlQueries().read_sql_file(connection=initialize)
#add_column = SqlQueries().add_column()
#add_entries = SqlQueries().write_to_tabel()

#print_out = SqlQueries().stdout_print_out()
#highest_amount = SqlQueries().write_highest_amount()
active_and_above_10 = SqlQueries().inactive()
