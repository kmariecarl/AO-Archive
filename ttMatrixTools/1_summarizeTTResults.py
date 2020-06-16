# This script processes a csv file of output from Analyst-0.2.3-smx using the 'matrix' operation.
# The output is a table within a PostgreSQl database that contains the min, max and deciles of traveltime
# between OD pairs for the selected time period.

# NOTE: Requires import of the aggs_for_arrays extension in the Postgresql database you are working in
# https://pgxn.org/dist/aggs_for_arrays/

#################################
#           IMPORTS             #
#################################

import argparse
import psycopg2
from psycopg2 import extras
from datetime import datetime
from progress import bar
import bz2
import os
import sys
import subprocess
import csv
import atexit

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
        self.table = table
        try:
            con = psycopg2.connect(f"dbname = '{self.db}' user='kristincarlson' host='localhost' password=''")
            con.set_session(autocommit=True)
            # Initiate cursor object on db
            cursor = con.cursor()
            self.cursor = cursor
        except:
            print('I am unable to connect to the database')

    def drop_table(self, table_out):
        print("Dropping tables")
        query = f"DROP TABLE IF EXISTS {self.schema}.{self.table}, {self.schema}.{table_out} CASCADE "
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())

    def create_table(self):
        print("Creating input table")
        query = f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (origin BIGINT, destination BIGINT, deptime CHAR(4), traveltime INTEGER)"
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())

    # Create inidvidual indicies
    def create_index(self, field):
        print(f'Creating index on {field}')
        query1 = f"CREATE INDEX {self.table}_{field} ON {self.schema}.{self.table}({field});"
        self.cursor.execute(query1)
        print(f'{field} index added to table {self.table}')
        print(datetime.now())

    def create_output_table(self, table_out):
        print("Creating output table")
        query = f"CREATE TABLE IF NOT EXISTS {self.schema}.{table_out} (origin bigint, destination bigint, " \
                f"_min integer,_10th int, _20th int, _30th int, _40th int, _50th int," \
                f"_60th int, _70th int, _80th int, _90th int, _max int)"
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())

    # Three different code sections to attempt to load data without unzipping results file
    def copy_from(self, file_path):
        # bash_command = f"bzcat TT_AWS_Station-results.csv.bz2 | wc"
        # # out = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE).communicate()[0]
        # out = subprocess.Popen(bash_command.split(), stdout=sys.stdout, stderr=sys.stderr).communicate()[0]
        # linecount = out.split()
        # print(linecount)
        # load_bar = ProgressBar(211736889)

        # print("Opening zipped file")
        # with bz2.open(f"{file_path}", "rt") as f:
        #     next(f)  # Skip header row
        #     print("Reading zipped file into table")
        #     for row in f:
        #         i = row.split(',')
        #         query = f"INSERT INTO {self.schema}.{self.table} (origin, destination, deptime, traveltime)" \
        #                 f"VALUES ({i[0]}, {i[1]}, {i[2]}, {i[3]})"
        #
        #         self.cursor.execute(query)
        #         load_bar.add_progress()
        #
        #     load_bar.end_progress()
        #     print(datetime.now())

# # Credit to http://yogeshsakhreliya.blogspot.com/2019/10/insert-multiple-records-in-postgresql.html
#         with bz2.open(f"{file_path}", "rt") as f:
#             next(f)  # Skip header row
#             print("Reading zipped file into table")
#
#             final_data = []  # create chunks of 4 million rows
#             count = 0
#             for row in f:
#                 if row and count < 4000000:
#                     i = row.split(',')
#                     final_data.append((i[0], i[1], i[2], i[3]))
#                     count += 1
#                 else:
#                     extras.execute_values(self.cursor, f"INSERT INTO {self.schema}.{self.table} "
#                                                        f"(origin, destination, deptime, traveltime)"
#                                                        f" VALUES %s", final_data)  # %s stands for a wildcard row of data
#                     print("chunk of 4,000,000 rows added")
#                     count = 0
#                     final_data = []
#
#         print(datetime.now())
#         with bz2.open(f"{file_path}", 'rb') as f:
#             # next(f)  # Skip header row
#             print("Copying data into DB")
#             print("NOTE: ~0.7257 GB of data is loaded every minute")
#             # this_copy = f'''COPY {self.schema}.{self.table} FROM '{f}' WITH CSV HEADER  '''
#             # self.cursor.copy_expert(f'''COPY {self.schema}.{self.table} FROM stdin WITH CSV HEADER  ''', f)
#             # self.cursor.copy_expert(f, f"{self.table}")
#             self.cursor.copy_from(f, f"{self.schema}.{self.table}", sep=",")

            print("Copying data into DB")
            print("NOTE: ~0.7257 GB of data is loaded every minute")
            query = f"COPY {self.schema}.{self.table} FROM '{file_path}' DELIMITER ',' CSV HEADER"
            self.cursor.execute(query)
            print(self.cursor.mogrify(query))
            print(datetime.now())


    def write_sql_function(self):
        print("Writing calc_deciles function")
        postgresql_function = """create or replace function calc_deciles(_tt integer[])
                                returns integer[] as $$
                                declare 
                                    _missing integer := 0;
                                    _maxint integer := 2147483647;
                                begin
                                    _missing := 120 - array_length(_tt, 1);
                                
                                    while _missing > 0 loop
                                        _tt := array_append(_tt, _maxint);
                                        _missing := _missing - 1;
                                
                                    end loop;
                                
                                    return array_to_percentiles(_tt, array[0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]);
                                end; $$
                                language plpgsql;"""
        self.cursor.execute(postgresql_function)
        print(self.cursor.mogrify(postgresql_function))
        print(datetime.now())

    def calc_deciles(self, table_out):
        print("Performing decile calculation and inserting data to new table")
        print("NOTE: ~0.342 GB of data is processed per minute")
        print("NOTE: Current program version does not apply indicies to the raw TT table")
        query = f"""insert into {self.db}.{self.schema}.{table_out}
                    select subq.origin, subq.destination, 
                        tile[1] as _min, tile[2] as _10th, tile[3] as _20th, tile[4] as _30th, tile[5]as _40th, tile[6] as _50th, 
                        tile[7] as _60th, tile[8] as _70th, tile[9] as _80th, tile[10] as _90th, tile[11] as _max
                    from (
                        select origin, destination, calc_deciles(array_agg(traveltime)) as tile
                        from {self.db}.{self.schema}.{self.table}
                        group by origin, destination) as subq;"""
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())


    # def median_tt(self, pairs, table_out):
    #     mybar = ProgressBar(2854475) #296 x 120 x 49127 = 15858196 x 0.18 = 2854475
    #     for tup in pairs:
    #         o = tup[0]
    #         d = tup[1]
    #         query = f"SELECT ROUND(PERCENTILE_CONT(0.50) " \
    #                     f"WITHIN GROUP (ORDER BY nested.traveltime)::NUMERIC, 0) AS median_tt " \
    #                     f"FROM (" \
    #                             f"SELECT origin, destination, traveltime " \
    #                             f"FROM {self.schema}.{self.table} " \
    #                             f"WHERE destination = {d} AND origin = {o}" \
    #                     f") AS nested;" \
    #
    #         self.cursor.execute(query)
    #         c = self.cursor.fetchall()
    #         query2 = f"INSERT INTO {self.schema}.{table_out} (origin, destination, traveltime)" \
    #                  f"VALUES ({o}, {d}, {c[0][0]});"
    #         self.cursor.execute(query2)
    #         mybar.add_progress()
    #     mybar.end_progress()
    #     print(datetime.now())
    def clean_close(self):
        self.cursor.close()
        print("\n Connection closed")

def run_command(bash_command):
    # process = subprocess.Popen(bash_command.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # output, error = process.communicate()
    # print(error)
    # print(output)

    subprocess.Popen(bash_command.split(), stdout=sys.stdout, stderr=sys.stderr).communicate() # stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)





#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    print(datetime.now())

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  # ENTER AS kristincarlson
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS public
    parser.add_argument('-table_in', '--TABLE_INPUT_NAME', required=True,
                        default=None)  # Table in schema i.e. tt_aws_station
    parser.add_argument('-table_out', '--TABLE_OUTPUT_NAME', required=True,
                        default=None)  # i.e. tt_median
    parser.add_argument('-path', '--FILE_PATH', required=True,
                        default=None)  # The entire file path where the unzipped, raw results are stored.
    args = parser.parse_args()

    # instantiate a db object
    my_db_obj = DBObject(args.DB_NAME, args.SCHEMA_NAME, args.TABLE_INPUT_NAME)
    my_db_obj.drop_table(args.TABLE_OUTPUT_NAME)
    my_db_obj.create_table()
        # my_db_obj.create_index("origin")
        # my_db_obj.create_index("destination")
    my_db_obj.create_output_table(args.TABLE_OUTPUT_NAME)
    my_db_obj.write_sql_function()
    try:
        my_db_obj.copy_from(args.FILE_PATH)
        my_db_obj.calc_deciles(args.TABLE_OUTPUT_NAME)
    finally:
        my_db_obj.clean_close()
