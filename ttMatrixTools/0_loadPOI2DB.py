# This is a supporting script of the dual access workflow
# This is a stand alone program that takes a csv of IDs and counts and puts them into the db for
# use with the 'assembleResults2.py' program.


#################################
#           IMPORTS             #
#################################

import argparse
import psycopg2
from datetime import datetime
from progress import bar
import csv

#################################
#           CLASSES           #
#################################

class ProgressBar:
    def __init__(self, lines):
        self.bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=lines)
    def add_progress(self):
        self.bar.next()
    def end_progress(self):
        self.bar.finish()

class DBObject:
    def __init__(self, db, schema, table):
        self.db = db
        self.schema = schema
        self.table = table  # this can also be the name of a view


#################################
#           FUNCTIONS           #
#################################

def create_poi_table(poi_tab, column_str, cursor):
     print("\n Creating POI Table")
     query = f"CREATE TABLE IF NOT EXISTS {poi_tab.schema}.{poi_tab.table} {column_str}"
     cursor.execute(query)
     print(cursor.mogrify(query))
     print(datetime.now())

def load_poi_table(poi_tab, file_path, cursor):

     print("\n Copying POI data into DB")
     query = f"COPY {poi_tab.schema}.{poi_tab.table} FROM '{file_path}' DELIMITER ',' CSV HEADER"
     cursor.execute(query)
     print(cursor.mogrify(query))
     print(datetime.now())

     #################################
     #           OPERATIONS          #
     #################################


if __name__ == '__main__':
    print(datetime.now())

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  # ENTER AS kristincarlson
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS public
    parser.add_argument('-poi_file', '--POI_FILE_PATH', required=True,
                        default=None)  # The entire file path where the POI data is stored
    parser.add_argument('-id', '--BLOCK_ID', required=True,
                        default=None)  # name of block ID column for joining with TT matrix data i.e. GEOID10, NCESID
    parser.add_argument('-poi_table', '--POI_TABLE_NAME', required=True,
                        default=None)  # i.e. assign new table name here

    args = parser.parse_args()
    block_id = args.BLOCK_ID

    poi_tab = DBObject(args.DB_NAME, args.SCHEMA_NAME, args.POI_TABLE_NAME)
    # Connect to db
    try:
        con = psycopg2.connect(f"dbname = '{poi_tab.db}' user='kristincarlson' host='localhost' password=''")
        con.set_session(autocommit=True)
        # Initiate cursor object on db
        cursor = con.cursor()
    except:
        print('I am unable to connect to the database')
    else:
        with open(args.POI_FILE_PATH, 'r') as f:
            poi_dict = csv.DictReader(f)
            print("POI file fieldnames \n", poi_dict.fieldnames)
            column_str = f"({block_id} BIGINT,"
            for i in poi_dict.fieldnames:
                if i != f"{block_id}":  # This column is dealt with earlier
                    column_str = column_str + "_" + str(i) + " INTEGER, "
            column_str = column_str[:-2]
            column_str = column_str + ")"
        # Creates the poi table in the database for the first time, then loads the data from file
        create_poi_table(poi_tab, column_str, cursor)
        load_poi_table(poi_tab, args.POI_FILE_PATH, cursor)
