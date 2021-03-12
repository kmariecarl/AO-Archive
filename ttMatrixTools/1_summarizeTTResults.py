# TIRP non-work destination workflow
# This is a supporting script of the dual access workflow

# This script processes a csv file of output from Analyst-0.2.3-smx using the 'matrix' operation.
# The output is a table within a PostgreSQl database that contains the min, max and deciles of traveltime
# between OD pairs for the selected time period. This code is flexible for any matrix input as long as there are
# columns for origin, destination, deptime, traveltime

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

    def create_table_tt(self):
        print("Creating input table")
        query = f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (origin BIGINT, destination BIGINT, deptime CHAR(4), traveltime INTEGER)"
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())

    def create_table_cmltv(self, destination_types):
        print("Creating input table")
        column_str = "(label BIGINT, deptime CHAR(4), threshold INTEGER,"
        for i in destination_types:
            column_str = column_str + str(i) + " INTEGER, "
        column_str = column_str[:-2]
        column_str = column_str + ")"
        query = f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} {column_str}"
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())

    # Three different code sections to attempt to load data without unzipping results file
    def copy_from(self, file_path):
            print("Copying data into DB")
            print("NOTE: ~0.7257 GB of data is loaded every minute")
            query = f"COPY {self.schema}.{self.table} FROM '{file_path}' DELIMITER ',' CSV HEADER"
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

    def create_output_table_tt(self, table_out):
        print("Creating output table")
        query = f"CREATE TABLE IF NOT EXISTS {self.schema}.{table_out} (origin bigint, destination bigint, " \
                f"_min integer,_10th int, _20th int, _30th int, _40th int, _50th int," \
                f"_60th int, _70th int, _80th int, _90th int, _max int)"
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())

    def create_output_table_cmltv(self, table_out):
        print("Creating output table")
        query = f"CREATE TABLE IF NOT EXISTS {self.schema}.{table_out} (origin bigint, destination_count int, " \
                f"_min integer,_10th int, _20th int, _30th int, _40th int, _50th int," \
                f"_60th int, _70th int, _80th int, _90th int, _max int)"
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())

    # uses linear interpolation to determine percentile when landing between two values
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

    def calc_deciles_tt(self, table_out):
        print("Performing decile calculation and inserting data to new table")
        print("NOTE: ~0.342 GB of data is processed per minute")
        print("NOTE: Current program version does not apply indicies to the raw TT table")
        query = f"""insert into {self.db}.{self.schema}.{table_out}
                    select subq.origin, subq.destination, 
                        tile[1] as _min, tile[2] as _10th, tile[3] as _20th, tile[4] as _30th, tile[5] as _40th, 
                        tile[6] as _50th, tile[7] as _60th, tile[8] as _70th, tile[9] as _80th, tile[10] as _90th,
                        tile[11] as _max
                    from (
                        select origin, destination, calc_deciles(array_agg(traveltime)) as tile
                        from {self.db}.{self.schema}.{self.table}
                        group by origin, destination) as subq;"""
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())

    def calc_deciles_cmltv(self, table_out, dest):
        print("Performing decile calculation and inserting data to new table")
        print("NOTE: ~0.xxx GB of data is processed per minute")
        print("NOTE: Current program version does not apply indicies to the raw TT table")
        query = f"""insert into {self.db}.{self.schema}.{table_out}
                    select subq2.label, subq2.{dest}, 
                        tile[1] as _min, tile[2] as _10th, tile[3] as _20th, tile[4] as _30th, tile[5] as _40th, 
                        tile[6] as _50th, tile[7] as _60th, tile[8] as _70th, tile[9] as _80th, tile[10] as _90th,
                        tile[11] as _max
                    from (
                        select subq.label, subq.{dest}, calc_deciles(array_agg(traveltime)) as tile
                        from (
                            select "label", deptime, {dest}, (array_agg(threshold order by threshold ))[1] as traveltime
                            from {self.db}.{self.schema}.{self.table} group by "label", deptime, {dest} order by "label") 
                            as subq group by "label", {dest} ) 
                         as subq2;"""
        self.cursor.execute(query)
        print(self.cursor.mogrify(query))
        print(datetime.now())


    def clean_close(self):
        self.cursor.close()
        print("\n Connection closed")

def run_command(bash_command):
    subprocess.Popen(bash_command.split(), stdout=sys.stdout, stderr=sys.stderr).communicate() # stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    print(datetime.now())

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-run_type', '--TT_OR_CMLTV', required=True,
                        default=None)  # Is the data from the TT matrix or a cumulative access analysis? TT or CMLTV
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
    my_db_obj.write_sql_function()

    my_db_obj.drop_table(args.TABLE_OUTPUT_NAME)  # If table was made in a previous attempt


    # Two courses of action, TT or CMLTV
    # Course 1: TT
    if args.TT_OR_CMLTV == 'TT':

        my_db_obj.create_table_tt()
        my_db_obj.create_output_table_tt(args.TABLE_OUTPUT_NAME)
        try:
            my_db_obj.copy_from(args.FILE_PATH)
            my_db_obj.calc_deciles_tt(args.TABLE_OUTPUT_NAME)
        finally:
            my_db_obj.clean_close()
    # Course 2: CMLTV
    # This is an experimental path to circumvent TT matrix generation and instead calculate approximate TTs based on
    # higher resolution (min-by-min) accessibilities. Not used in final TIRP results. 03/11/2021
    elif args.TT_OR_CMLTV == 'CMLTV':
        try:
            destination_types = []
            with open(args.FILE_PATH, 'r') as f:
                csvfile = csv.DictReader(f)
                headers = csvfile.fieldnames
            destination_types.extend(headers[3:])

            my_db_obj.create_table_cmltv(destination_types)
            my_db_obj.copy_from(args.FILE_PATH)  # First load in the data

            for dest in destination_types:
                print(f"Processing deciles for destination: {dest}")
                # my_db_obj.drop_table(f"{args.TABLE_OUTPUT_NAME}_{dest}")  # This is not suited for iterative table gen
                my_db_obj.create_output_table_cmltv(f"{args.TABLE_OUTPUT_NAME}_{dest}")
                my_db_obj.calc_deciles_cmltv(f"{args.TABLE_OUTPUT_NAME}_{dest}", dest)
        finally:
            my_db_obj.clean_close()
    else:
        print("Invalid input. Must type either 'TT' or 'CMLTV'")
