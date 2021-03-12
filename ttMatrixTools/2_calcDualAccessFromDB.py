# TIRP non-work destination workflow
# This is a supporting script of the dual access workflow

# This program optionally loads POI data to Postgres database then forces computation of the dual
# accessibility (how long to reach 1, 2, 3, 10 destinations of type X). Output format should be a .csv

# This program relies on the sorted order of the incoming data. The SQL query orders the data on origin and traveltime

# Notes, data will be joined on block ids (GEOID10, NCESID, etc.) so tt_median table and poi table must be
# associated with a unique block id
# Blocks that cannot reach another block that contains one of the desired destinations (grocery, healthcare, etc)
# within the maxtime limit (i.e. 60 minutes) will be assigned maxtime integers instead of NULL.

#################################
#           IMPORTS             #
#################################

import argparse
import psycopg2
from datetime import datetime
from myToolsPackage import matrixLinkModule as mod
from progress import bar
import csv
import matplotlib.pylab as plt
import numpy as np



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

# def user_input_poi(poi_num):
#     if int(poi_num) > 1:
#         poi_in = input("\n Space separated list of POIs to create sum for single category: ")
#         poi_list = poi_in.split()
#         print(poi_list)
#         poi_column = input("\n Type name of new summation column: ")
#     else:
#         poi_column = input("\n Type name of single poi field (preceed name with underscore): ")
#         poi_list = []
#         poi_list.append(poi_column)
#         print(poi_list)
#
#     return poi_list, poi_column

def make_view(tt_tab, poi_tab, percentile, block_id):
    query0 = F"DROP VIEW IF EXISTS v_{tt_tab.table}_{poi_tab.table}"
    cursor.execute(query0)
    print(f"\n Creating view for {poi_tab.table}")
    # summation_str = ""
    # for poi in poi_list:
    #     if poi_list.index(poi) == 0:
    #         summation_str = summation_str + f"B.{poi}"  # First element in list
    #     summation_str = summation_str + f" + B.{poi}"  # All other elements in list
    # print(summation_str)

    view_name = f"v_{tt_tab.table}_{poi_tab.table}"
    query = f"""CREATE VIEW {view_name} AS
                SELECT A.origin, A.destination, A.{percentile}, B._count AS _count
                FROM {tt_tab.db}.{tt_tab.schema}.{tt_tab.table} A
                LEFT JOIN {poi_tab.db}.{poi_tab.schema}.{poi_tab.table} B
                ON A.destination = B.{block_id};"""
    cursor.execute(query)
    print(cursor.mogrify(query))
    print(datetime.now())
    return view_name

def select_distinct(poi_view, cursor):
    print("\n Make a list of all origins")
    query = f"""select distinct origin 
                from {poi_view.db}.{poi_view.schema}.{poi_view.table} 
                order by origin;"""
    cursor.execute(query)
    print(cursor.mogrify(query))
    print(datetime.now())
    out = cursor.fetchall()
    o_list = []
    for o in out:
        o_list.append(o[0])
    return o_list

def conciseNames():
    # Create concise names for file output
    name_split = tt_tab.table.split('_')[:-1]
    table = '_'.join(name_split)
    poi = poi_tab.table.split('_')[-1]
    return table, poi

def calc_dual(poi_view, percentile, o_list, limit, cursor):
    print(f"\n Calculating dual accessibility for percentile {percentile} and poi {poi_tab.table}")

    # Returns list of tuples [(...), (...),...] spot 0 is block id, spot 1 is tt, spot 2 is num of pois in block
    # Set is returned ordered by origin and by TT, thus lowest TT w/1+ destinations comes in first
    set = query_tt_poi(poi_view, percentile, cursor)
    set_dict = {}  # Dictionary with origins as keys and list of tuples as values
    prev_o = None
    for s in set:   # Grab the first row for each origin that is returned in the set (the set has repeating origins)
        curr_o = s[0]
        if prev_o != curr_o:
            set_dict[s[0]] = []
            set_dict[s[0]].append((s[1], s[2]))  # Append tuple of tt and num of dest
            prev_o = curr_o
        else:
            set_dict[s[0]].append((s[1], s[2]))
            prev_o = curr_o

    fieldnames = [x for x in range(1, limit + 1, 1)]
    fieldnames.insert(0, 'origin')
    table, poi = conciseNames()
    writer = mod.mkDictOutput(f"1_{table}{percentile}_{poi}", fieldnames)

    calc_bar = ProgressBar(len(o_list))

    for origin in o_list:
        tt_list = []
        entry = {'origin': origin}
        if origin in set_dict:
            for tup in set_dict[origin]:
                count = 0
                number_of_pois_in_block = tup[1]
                while count < number_of_pois_in_block:
                    if len(tt_list) == limit:  # Use this control seq to cap the dual access measure for X destinations
                        break
                    else:
                        tt_list.append(tup[0])  # append the travel time until the while loop completes
                        entry[len(tt_list)] = tup[0]  # 2147483647
                        count += 1
        calc_bar.add_progress()
        # Assign maxtime integer to output where an origin cannot reach X destinations in 60 min, or whatever the
        # original threshold was.
        for f in fieldnames:
            if f not in entry:
                dict_update = {f: 2147483647}
                entry.update(dict_update)
        writer.writerow(entry)
    calc_bar.end_progress()

def query_tt_poi(poi_view, percentile, cursor):
    query = f"""select origin, {percentile}, _count
                from {poi_view.db}.{poi_view.schema}.{poi_view.table}
                where _count > 0 and {percentile} <= 3600
                order by origin, {percentile};"""
    cursor.execute(query)
    out = cursor.fetchall()
    return out

def transpose_data(tt_tab, limit):
    query = f"""select subq.origin, 
                coalesce(c[1], 2147483647)  as _1, coalesce(c[2], 2147483647) as _2, coalesce(c[3], 2147483647) as _3,
                coalesce(c[4], 2147483647) as _4, coalesce(c[5], 2147483647) as _5, coalesce(c[6], 2147483647) as _6,
                coalesce(c[7], 2147483647) as _7, coalesce(c[8], 2147483647) as _8, coalesce(c[9], 2147483647) as _9,
                coalesce(c[10], 2147483647) as _10 from (
                    select origin, array_agg(_50th) as c 
                    from {tt_tab.db}.{tt_tab.schema}.{tt_tab.table} where destination_count between 1 and {limit}
                    group by origin) as subq;"""
    cursor.execute(query)
    out = cursor.fetchall()
    return out

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
    parser.add_argument('-deciles_table', '--DECILES_TABLE_NAME', required=True,
                        default=None)  # Table in schema i.e. tt_79_base_deciles
    parser.add_argument('-percentile', '--PERCENTILE', required=True,
                        default=None)  # Travel time percentile to carry forward in dual access calcs i.e. 10th, 50th, etc
    parser.add_argument('-limit', '--DEST_LIMIT', required=True,
                        default=None)  # Calc dual access for 1–limit destinations i.e. 10, 25, 100, etc
    parser.add_argument('-poi_table', '--POI_TABLE_NAME', required=True,
                        default=None)  # poi_es, poi_grocery, poi_health, etc
    parser.add_argument('-load_poi_YN', '--LOAD_POI_Y_N', required=True,
                        default=None)  # option for triggering the loading of POI data to a db table
    parser.add_argument('-id', '--BLOCK_ID', required=True,
                        default=None)  # name of block ID column for joining with TT matrix data i.e. GEOID10, NCESID
    # parser.add_argument('-poi_table', '--POI_TABLE_NAME', required=False,
    #                     default=None)  # i.e. assign new table name here
    parser.add_argument('-poi_file', '--POI_FILE_PATH', required=False,
                        default=None)  # The entire file path where the POI data is stored
    # parser.add_argument('-poi_num', '--SINGLE_OR_MULTIPLE', required=False,
    #                     default=None)  # 1 indicates a single poi column, 2 or more indicates a new summation column will be made
    args = parser.parse_args()

    percentile = args.PERCENTILE
    limit = int(args.DEST_LIMIT)
    block_id = args.BLOCK_ID

    # Two courses of action, TT or CMLTV
    # Course 1: TT
    if args.TT_OR_CMLTV == 'TT':
        # Create db objects for the deciles table (exists already) and the poi table (might need to be created below)
        tt_tab = DBObject(args.DB_NAME, args.SCHEMA_NAME, args.DECILES_TABLE_NAME)
        poi_tab = DBObject(args.DB_NAME, args.SCHEMA_NAME, args.POI_TABLE_NAME)

        # Connect to db
        try:
            con = psycopg2.connect(f"dbname = '{tt_tab.db}' user='kristincarlson' host='localhost' password=''")
            con.set_session(autocommit=True)
            # Initiate cursor object on db
            cursor = con.cursor()
        except:
            print('I am unable to connect to the database')
        else:

            # If the POI table has not loaded to db yet, do so now
            if args.LOAD_POI_Y_N == "Y":
                with open(args.POI_FILE_PATH, 'r') as f:
                    poi_dict = csv.DictReader(f)
                    print("POI file fieldnames \n", poi_dict.fieldnames)
                    column_str = f"({block_id} BIGINT,"
                    for i in poi_dict.fieldnames:
                        if i != f"{block_id}":  #This column is dealt with earlier
                            column_str = column_str + "_" + str(i) + " INTEGER, "
                    column_str = column_str[:-2]
                    column_str = column_str + ")"
                # Creates the poi table in the database for the first time, then loads the data from file
                create_poi_table(poi_tab, column_str, cursor)
                load_poi_table(poi_tab, args.POI_FILE_PATH, cursor)

            view_name = make_view(tt_tab, poi_tab, percentile, block_id)
            # Create db object for newly created view
            poi_view = DBObject(args.DB_NAME, args.SCHEMA_NAME, view_name)

            o_list = select_distinct(poi_view, cursor)
            results = {}
            for o in o_list:
                results[o] = {}

            calc_dual(poi_view, percentile, o_list, limit, cursor)
            cursor.close()

    # Course 2: CMLTV
    # This is an experimental course that was ultimately not used in the final TIRP resutls
    elif args.TT_OR_CMLTV == 'CMLTV':
        tt_tab = DBObject(args.DB_NAME, args.SCHEMA_NAME, args.DECILES_TABLE_NAME)
        # Connect to db
        try:
            con = psycopg2.connect(f"dbname = '{tt_tab.db}' user='kristincarlson' host='localhost' password=''")
            con.set_session(autocommit=True)
            # Initiate cursor object on db
            cursor = con.cursor()
        except:
            print('I am unable to connect to the database')
        else:
            result_set = transpose_data(tt_tab, limit)  # Returns a list of tuples [(,,,,,)]
            fieldnames = [x for x in range(1, limit + 1, 1)]
            fieldnames.insert(0, 'origin')
            file = open(f"dual_access{percentile}_{tt_tab.table}.csv", 'w')
            writer = csv.writer(file)
            writer.writerow(fieldnames)
            for row in result_set:
                writer.writerow(row)
    else:
        print("Invalid input. Must type either 'TT' or 'CMLTV'")




