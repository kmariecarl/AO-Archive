# This script processes a csv file of output from Analyst-0.2.3-smx using the 'matrix' operation.
# The output is a table within a PostgreSQl database that contains the raw and averaged TT between OD pairs for the
# selected time period.

#################################
#           IMPORTS             #
#################################

import argparse
import psycopg2

#################################
#           CLASSES           #
#################################
class DBObject:
    def __init__(self, db, schema, table):
        self.db = db
        self.schema = schema
        self.table = table
        try:
            con = psycopg2.connect(f"dbname = '{self.db}' user='kristincarlson' host='localhost' password=''")
            con.set_session(autocommit=True)
            # Initiate cursor object on db
            cursor = con.cursor()
            self.cursor = cursor
        except:
            print('I am unable to connect to the database')

    def create_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (origin BIGINT, destination BIGINT, deptime CHAR(4), traveltime INTEGER)"
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))

    def copy_from(self, file_path):

        query = f"COPY {self.schema}.{self.table} FROM '{file_path}' DELIMITER ',' CSV HEADER"
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        #mod.elapsedTime(start_time)

    def create_index(self):
        print('Creating index on origin')
        query1 = f"CREATE INDEX origin_{self.table} ON {self.schema}.{self.table} (origin);"
        self.cursor.execute(query1)

        query2 = f"CREATE INDEX deptime_{self.table} ON {self.schema}.{self.table} (deptime);"
        self.cursor.execute(query2)
        print(self.cursor.mogrify(query2))
        print(f'Index added to table {self.table}')

        query3 = f"CREATE INDEX destination_{self.table} ON {self.schema}.{self.table} (destination);"
        self.cursor.execute(query3)
        print(self.cursor.mogrify(query3))
        print(f'Index added to table {self.table}')


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  # ENTER AS kristincarlson
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS public
    parser.add_argument('-table', '--TABLE_NAME', required=True,
                        default=None)  # Table in schema i.e. tt_aws_station
    parser.add_argument('-path', '--FILE_PATH', required=True,
                        default=None)  # The entire file path where the unzipped results are stored.
    args = parser.parse_args()

    # instantiate a db object
    my_db_obj = DBObject(args.DB_NAME, args.SCHEMA_NAME, args.TABLE_NAME)

    my_db_obj.create_table()
    my_db_obj.copy_from(args.FILE_PATH)
    my_db_obj.create_index()
