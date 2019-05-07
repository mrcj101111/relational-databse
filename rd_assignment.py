import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Class is for db connection
class PostgresDb:

    # connect to an existing db
    def connect_to_db(self, username, password, db_name):
        connection = psycopg2.connect(dbname=db_name, user=username,
                                      host='localhost', password=password)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

    # connect to a default db in order to create a new db
    def create_db(self, username, password, db_name):
        connection = psycopg2.connect(dbname='postgres', user=username, host='localhost', password=password)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute('CREATE DATABASE %s ;' % db_name)

# Class for all sql queries to be performed
class SqlQueries:

    # Read sql file with query to add a table to db
    def read_sql_file(self, connection, sql_read_file_name):
        sql_script = open(sql_read_file_name, 'r').read().replace('\n', '').split(';')[:-1]
        cursor = connection.cursor()
        for line in sql_script:
            cursor.execute(line)

    # Add an additional column called "is_active" to existing table
    def add_column(self, connection):
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE products ADD is_active boolean null;")
        connection.commit()

    # Add a list of entries to the db table
    def write_to_tabel(self, connection, sql_fields_input):
        cursor = connection.cursor()
        sql_file = open(sql_fields_input, 'r').read()
        cursor.execute("INSERT INTO products (id, created, modified, description, amount, is_active) VALUES %s ;"
                       % sql_file)
        connection.commit()

    # Create a printout of table
    def stdout_print_out(self, connection, stdoutName):
        cursor = connection.cursor()
        sql = "COPY (SELECT * FROM products) TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open(stdoutName, 'w') as file:
            cursor.copy_expert(sql, file)

    # create a printout of table, arranged by highest number
    def write_highest_amount(self, connection, highest_amount_output):
        cursor = connection.cursor()
        sql = "COPY (SELECT * FROM products ORDER BY amount DESC) TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open(highest_amount_output, 'w') as file:
            cursor.copy_expert(sql, file)

    # Create a printout of all values that is active with an amount of >10
    def active_and_above_10(self, connection, active_and_above_10_output):
        cursor = connection.cursor()
        sql = "COPY (SELECT * FROM products WHERE amount > 10 AND is_active = TRUE)" \
              "TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open(active_and_above_10_output, 'w') as file:
            cursor.copy_expert(sql, file)

    # Create a printout of all inactive entries
    def inactive(self, connection, inactive_output):
        cursor = connection.cursor()
        sql = "COPY (SELECT description, amount, is_active FROM products WHERE is_active = FALSE)" \
              "TO STDOUT WITH CSV HEADER DELIMITER ','"
        with open(inactive_output, 'w') as file:
            cursor.copy_expert(sql, file)


# Queries to execute all the sql functions
class AddDatabase:

    def add_new_db(self):
        db_query = input('Do you want to create a new database? (yes/no): ')
        if db_query == 'yes':
            username = input('Please provide a username: ')
            password = input('Please provide a password: ')
            db_name = input('Please provide a db_name: ')
            PostgresDb().create_db(username, password, db_name)
            self.connect_to_db_and_amend()
        else:
            self.connect_to_db_and_amend()

    def connect_to_db_and_amend(self):
        input('Please press ENTER in order to input database details to connect to database')
        existing_username = input('Please provide a username: ')
        existing_password = input('Please provide a password: ')
        existing_db_name = input('Please provide a db_name: ')
        initialize = PostgresDb().connect_to_db(existing_username, existing_password, existing_db_name)
        sql_read_file_name = input('Please enter file name to import table from: ')
        SqlQueries().read_sql_file(connection=initialize, sql_read_file_name=sql_read_file_name)
        SqlQueries().add_column(initialize)
        sql_add_entries = input('Please enter file name of table entries to add: ')
        SqlQueries().write_to_tabel(initialize, sql_add_entries)
        std_out_name = input('Please enter file name to which you want to print your stdout: ')
        SqlQueries().stdout_print_out(initialize, std_out_name)
        highest_amount_output = input('Please enter file name to which you want to '
                                      'print your table, arranged by highest amount: ')
        SqlQueries().write_highest_amount(initialize, highest_amount_output)
        active_and_above_10_output = input('Please enter file name to which you want to'
                                           ' print your table of active fields with an amount higher than 10: ')
        SqlQueries().active_and_above_10(initialize, active_and_above_10_output)
        inactive_output = input('Please enter file name to which you want to print your table of inactive fields: ')
        SqlQueries().inactive(initialize, inactive_output)
        print('Your database is all set up and your necessary files are printed!')

# Initialize query functions
if __name__ == '__main__':
    AddDatabase().add_new_db()
