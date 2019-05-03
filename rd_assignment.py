import psycopg2


class PostgresDb:
    def __init__(self):
        self.cursor = None
        self.connection = psycopg2.connect(dbname='rd_assignment', user='corne', host='localhost', password='test')


