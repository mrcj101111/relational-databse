import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import argparse
import random
import time

# Class to connect to db and alter the table.
class PostgresDb:
    connection = psycopg2.connect(dbname='rd_assignment', user='', host='localhost', password='')
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    # Read sql file with query to add a table to db and and column
    def read_sql_file(self, sql_read_file_name):
        sql_script = open(sql_read_file_name, 'r').read().replace('\n', '').split(';')[:-1]
        cursor = self.cursor
        for line in sql_script:
            cursor.execute(line)

        cursor.execute("ALTER TABLE products ADD is_active boolean null;")
        self.connection.commit()

    # SQL queries
    def run_sql_queries(self):
        cursor = self.cursor

        # Set the start and end date for the random dates
        start_timestamp = time.mktime(time.strptime('Jun 1 2010  01:33:00', '%b %d %Y %I:%M:%S'))
        end_timestamp = time.mktime(time.strptime('Jun 1 2017  12:33:00', '%b %d %Y %I:%M:%S'))

        # For each line insert 20 table values
        for i in range(21):
            id = i
            random_day = time.strftime('%b %d %Y %I:%M:%S', time.localtime(random.randrange(start_timestamp,end_timestamp)))
            description = 'edit file'
            random_status = random.choice([True, False])
            random_amount = random.randint(1, 1000)

            cursor.execute("INSERT INTO products (id, created, modified, description, amount, is_active) VALUES (%s, %s, %s, %s, %s, %s)",
                            (id, random_day, random_day, description, random_amount, random_status))
            self.connection.commit()

    # Create a printout of table
        sql = "COPY (SELECT * FROM products) TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open('stdoutName.csv', 'w') as file:
            cursor.copy_expert(sql, file)

    # create a printout of table, arranged by highest number
        sql = "COPY (SELECT * FROM products ORDER BY amount DESC) TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open('highest_amount_output.csv', 'w') as file:
            cursor.copy_expert(sql, file)

    # Create a printout of all values that is active with an amount of >10
        sql = "COPY (SELECT * FROM products WHERE amount > 10 AND is_active = TRUE)" \
              "TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open('active_and_above_10_output.csv', 'w') as file:
            cursor.copy_expert(sql, file)

    # Create a printout of all inactive entries
        sql = "COPY (SELECT description, amount, is_active FROM products WHERE is_active = FALSE)" \
              "TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open('inactive_output.csv', 'w') as file:
            cursor.copy_expert(sql, file)

# Initialize query functions
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-input", action='store', dest="input", help="input filename", required=True)
    args = parser.parse_args()
    PostgresDb().read_sql_file(sql_read_file_name=args.input)
    PostgresDb().run_sql_queries()
    print('Your queries was successful!')
