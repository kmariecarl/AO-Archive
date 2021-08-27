# This program reads in csv files from dual2_assembleResults.py and puts them into a sqlite db file along with the
# LEHD RAC values to begin aggregating the dual accessibility results over the metro worker population and potentially
# other groupings such as per city, per county groupings.

#################################
#           IMPORTS             #
#################################

import argparse
from datetime import datetime
from progress import bar
import os
import json
import subprocess
import sys
import csv
import fnmatch
import sqlite3
from sqlite3 import Error
import csv_to_sqlite



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

class FolderObject:
    def __init__(self, dir, name):
        self.dir = dir
        self.scenario_name = name
        self.db = None
        self.tables = []
        self.rac = None

    def create_db(self, poi_list, rac):
        db = find_file(self.scenario_name, 'agg.sqlite')
        if db:
            self.db = f"{self.dir}{self.scenario_name}/agg.sqlite"
            print(f'-----DB already created in folder {self.scenario_name}, Passing-----')
        else:
            file_list = []
            file_list.append(rac)
            for p in poi_list:
                i = find_file(self.scenario_name, f"change_*_{p}.csv")
                input_file = f"{self.dir}{self.scenario_name}/{i}"
                file_list.append(input_file)
            options = csv_to_sqlite.CsvOptions()
            csv_to_sqlite.write_csv(file_list, f"{self.dir}{self.scenario_name}/agg.sqlite", options)
            self.db = f"{self.dir}{self.scenario_name}/agg.sqlite"
        conn = create_connection(self.db)
        cur = conn.cursor()
        query = f"""SELECT sum(rC000) FROM change_base_bde_grocery A left join Joined_RAC_FILE_2017 B on A.origin = B.GEOID10 where bs_1 > 0 and bs_1 < 300;"""
        cur.execute(query)
        print(cur.fetchone())
        return conn

    def view_tables(self, conn):
        cur = conn.cursor()
        query = f"""SELECT name FROM sqlite_master"""
        cur.execute(query)
        for row in cur.fetchall():
            if 'RAC' in row[0]:
                self.rac = row[0]
            else:
                self.tables.append(row[0])

    def aggregate(self, conn, columns, tt_buckets):
        for table in self.tables:
            with open(f'{self.dir}{self.scenario_name}/output.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=',')
                self.left_join(conn, table, self.rac)
                total_rac = self.get_total_workers(conn, table)
                # Fills in one row at a time AKA 0–5 min for all columns, then 5–10 for all columns, etc.
                output = self.agg_sub(table, conn, columns, tt_buckets)
                writer.writerows(output)






    def left_join(self, conn, table, rac):
        cur = conn.cursor()
        query = f"""ALTER TABLE {table} ADD COLUMN rac integer;"""
        cur.execute(query)

        names = [description[0] for description in cur.description]
        print(names)

        query = f"""UPDATE {table} 
                    SET rac = (SELECT rC000
                                FROM {rac}
                                WHERE origin = GEOID10);"""
        cur.execute(query)

    def get_total_workers(self, conn, table):
        cur = conn.cursor()
        query = f"""SELECT SUM(rac) FROM {table};"""
        cur.execute(query)
        print(f"Total workers in analysis region: {cur.fetchone()};")
        total_rac = cur.fetchone()
        return total_rac

    def agg_sub(self, table, conn, columns, tt_buckets):
        cur = conn.cursor()
        output = []
        tt_min = 0
        for t in tt_buckets:
            row = []
            tt_max = t
            for c in columns:
                query = f"""SELECT sum(rac) FROM {table} WHERE {c} > {tt_min} AND {c} < {tt_max};"""
                cur.execute(query)
                val = cur.fetchone()
                row.append(val)
            output.append(row)
        return output

#################################
#           FUNCTIONS           #
#################################
def make_lists(input):
    out = []
    for i in input:
        out.append(str(i))
    return out

# Solution from https://kite.com/python/examples/4293/os-get-the-relative-paths-of-all-files-and-subdirectories-in-a-directory
def find_file(chgdir, string):
    my_file = None
    for file in os.listdir(f'{chgdir}/.'):
        if fnmatch.fnmatch(file, f"{string}"):
            my_file = file
    return my_file




# def create_table_string():
#     tab_str = ""
#     count = 0
#     while count < 10:
#         tab_str = tab_str + f"bs_{count + 1} integer,"\
#                   + f"updt_{count + 1} integer,"\
#                   + f"abschg{count + 1} integer,"\
#                   + f"pctchg{count + 1} real,"
#         count += 1
#     tab_str = tab_str[:-1]
#     print(tab_str)
#     return tab_str

# Create a connection to a sqlite db file, solution found at https://www.sqlitetutorial.net/sqlite-python/creating-database/
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except Error as e:
        print(e)

    return conn


# Create table function solution found at https://www.sqlitetutorial.net/sqlite-python/create-tables/
# def create_table(conn, create_table_sql):
#     """ create a table from the create_table_sql statement
#     :param conn: Connection object
#     :param create_table_sql: a CREATE TABLE statement
#     :return:
#     """
#     try:
#         c = conn.cursor()
#         c.execute(create_table_sql)
#     except Error as e:
#         print(e)


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    print(datetime.now())

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-dir', '--DIRECTORY_PATH', required=True,
                        default=None)  # Path from root to where the folders are
    args = parser.parse_args()

    rac = '/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2017/Joined_RAC_FILE_2017.csv'

    print("---Notes about this program---")
    print("-0-Default RAC file is from 2017")
    print("-0-Assumes dual access is calculated for 10 destination")
    print("-0-Output is hard coded at 3 destinations max, beyond that we are not calculating summary statistics")
    print("-0-TT buckets summarized for 0–60 minutes in 5 min increments")
    print("\n")

    p = input('List of pois to calculate dual accessibility for, i.e. grocery, health, edu, etc. : ').split()
    print("\n")
    pois = make_lists(p)

    input_file_columns = ['bs_1', 'updt_1', 'abschg1', 'pctchg1', 'bs_2', 'updt_2', 'abschg2', 'pctchg2',
                          'bs_3', 'updt_3', 'abschg3', 'pctchg3']
    tt_buckets = list(range(5, 65, 5))  # [5, 10, 15, ..., 60]

    dir = args.DIRECTORY_PATH

    print("-----Making list of folder objects----- \n")
    with open(f'{dir}/folder_list.json') as json_file:
        folder_names = json.load(json_file)
        folder_list = []
        for key, name in folder_names.items():
            folder_list.append(FolderObject(dir, name))


    # Provide file input to tell the program which folders should be accessed for the change results
    print('-----Beginning aggregation process----- \n')
    with open(f'{dir}/comparisons.csv') as csv_file:
        comparisons = csv.reader(csv_file)
        next(comparisons)
        for row in comparisons:
            f = return_folder(folder_list, row)
            conn = f.create_db(pois, rac)
            f.view_tables(conn)
            f.aggregate(conn, input_file_columns, tt_buckets)
            conn.close()

