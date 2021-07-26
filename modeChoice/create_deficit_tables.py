# This script simply creates multiple tables, 1 per state for a user specified travel time threshold of access deficits
# in the kristincarlson database. Code used for evaluating access deficits across the U.S.


#################################
#           IMPORTS             #
#################################

import argparse
import psycopg2
from datetime import datetime


#################################
#           CLASSES           #
#################################

class DBObject:
    def __init__(self, db, schema, table):
        self.db = db
        self.schema = schema
        self.table = table  # this can also be the name of a view

#################################
#           FUNCTIONS           #
#################################

def create_state_views(ad_tab, id, thresh, cursor):
    print(f"\n Creating view {id}")
    query = f"CREATE OR REPLACE VIEW {ad_tab.schema}.state{id}_{thresh} AS " \
            f"SELECT * FROM {ad_tab.schema}.{ad_tab.table} " \
            f"WHERE cast(left(blockid, 2) as int) = {id} AND threshold = {thresh};"
    cursor.execute(query)
    # print(cursor.mogrify(query))
    print(datetime.now())

# These functions differ slightly by not requiring a 'threshold' value because it's assumed the input table is built off
# a match of 1 specific transit threshold with 1 specific auto threshold i.e. 45 min vs 30 min.
def create_state_views_alt(ad_tab, id, thresh, cursor):
    print(f"\n Creating view {id}")
    query = f"CREATE OR REPLACE VIEW {ad_tab.schema}.state{id}_{thresh} AS " \
            f"SELECT * FROM {ad_tab.schema}.{ad_tab.table} " \
            f"WHERE cast(left(blockid, 2) as int) = {id};"
    cursor.execute(query)
    # print(cursor.mogrify(query))
    print(datetime.now())

def export_to_csv(ad_tab, id, thresh, p, cursor):
    print(f"\n Exporting state {id}")
    query = f"COPY (select * from {ad_tab.schema}.state{id}_{thresh}) TO '{p}{id}_{thresh}.csv' DELIMITER ',' CSV HEADER; "
    cursor.execute(query)
    # print(cursor.mogrify(query))
    print(datetime.now())



 #################################
 #           OPERATIONS          #
 #################################

if __name__ == '__main__':
    print(datetime.now())

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-db', '--DB_NAME', required=True, default='kristincarlson')  # ENTER AS kristincarlson
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default='mode_choice')  # ENTER AS mode_choice
    parser.add_argument('-table', '--TABLE_NAME', required=True, default='access_deficit')  # ENTER AS access_deficit
    parser.add_argument('-p', '--FILE_PATH', required=True,  # enter as /Users/kristincarlson/Dropbox/AO_Projects/TRB2021/kristin/data/state_deficit
                        default='/Volumes/GoogleDrive/Shared drives/Accessibility Observatory/AO project archive/AO projects/Mode_Choice/state_deficits/')  # The entire file path where the POI data is stored
    parser.add_argument('-thresh', '--THRESHOLD', required=True,
                        default=None)  # type singular tt threshold to export tables for, i.e. 300, 1800, 3600, etc.
                                        # where transit and auto use the SAME threshold

    args = parser.parse_args()
    path = args.FILE_PATH
    thresh = args.THRESHOLD
    p = args.FILE_PATH

    flag = input("If the input table lists all thresholds type 'Y' else type 'N':  ")

    ad_tab = DBObject(args.DB_NAME, args.SCHEMA_NAME, args.TABLE_NAME)  # ad stands for access deficit
    # Connect to db
    try:
        con = psycopg2.connect(f"dbname = '{ad_tab.db}' user='kristincarlson' host='localhost' password=''")
        con.set_session(autocommit=True)
        # Initiate cursor object on db
        cursor = con.cursor()
    except:
        print('I am unable to connect to the database')
    else:
        # stateids = ['27','24','05','06','12','37','19','53','47','25','51','17'] #11 #+ ['13','26','36','39','42','48'] + ['54','42','22','36','49','26','01','20']
        stateids = ['01', '02', '04', '05', '06', '08', '09', '10',
                    '11', '12', '13', '15', '16', '17', '18', '19', '20',
                    '21', '22', '23', '24', '25', '26', '27', '28', '29', '30',
                    '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
                    '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56']  # some may need to be removed
        # If yes, then use process assuming transit and auto thresholds are the same i.e. compare 30 min with 30 min
        # if no, then use slightly different sql queries for the input table to export the views needed
        if flag == 'Y':
            for id in stateids:
                create_state_views(ad_tab, id, thresh, cursor)
                export_to_csv(ad_tab, id, thresh, p, cursor)
        else:
            for id in stateids:
                create_state_views_alt(ad_tab, id, thresh, cursor)
                export_to_csv(ad_tab, id, thresh, p, cursor)
    print("Done")
