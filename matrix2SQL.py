#Use this script for creating the travel time matrix db.
#The db path and name provided by the user initiates the db.
#Two tables are created, the o2pnr and pnr2d tables.
#A fifth column is added which converts the deptime to seconds for use in the TTMatrixLinkSQL.py script

#Example Usage: kristincarlson~ python matrix2SQL.py -path /Users/kristincarlson/Dropbox/Bus-Highway/Task3/
# Restart2/IntermodalAccess/1_TTMatrixLink/ -name newdbname.sqlite -table1 o2pnr -data1 <path/file.txt>
#-table2 pnr2d -data2 <path/file.txt>

import sqlite3
import csv
import argparse

#################################
#           FUNCTIONS           #
#################################


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-path', '--FOLDER_PATH', required=True, default=None)  #ENTER AS /Users/kristincarlson/Dropbox/Bus-Highway/Task3/Restart2/IntermodalAccess/1_TTMatrixLink/
    parser.add_argument('-name', '--DB_NAME', required=True, default=None)  #ENTER AS xxxx.sqlite
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #Assign Table 1 name in schema, i.e. pnr2d15
    parser.add_argument('-data1', '--TABLE1_PATH_DATA', required=True, default=None)  #List path to .csv file
    parser.add_argument('-table2', '--TABLE2_NAME', required=True, default=None)  #Assign Table 2 name in schema, i.e. pnr2d15
    parser.add_argument('-data2', '--TABLE2_PATH_DATA', required=True, default=None)  #List path to .csv file
    parser.add_argument('-jobstab', '--JOBS_TABLE_NAME', required=False, default=None)  #Assign jobs table name in schema, i.e. jobs
    parser.add_argument('-jobsdata', '--JOBS_PATH_DATA', required=False, default=None)  #List path to .csv file
    args = parser.parse_args()

    #Make db
    sqlite_file = '{}{}'.format(args.FOLDER_PATH, args.DB_NAME)

    con = sqlite3.connect(sqlite_file)
    cur = con.cursor()

    # Create a new table in the SQLite database where the o2pnr data will go.
    try:
        cur.execute('CREATE TABLE IF NOT EXISTS {} (origin VARCHAR, destination VARCHAR, deptime VARCHAR, traveltime BIGINT);'.format(args.TABLE1_NAME))

    except sqlite3.OperationalError:
        print('Error: Please debug')

    #Open the data for table 1 into a DictReader Object
    with open('{}'.format(args.TABLE1_PATH_DATA)) as mydata:
        ttmatrix1 = csv.DictReader(mydata)
        #Produce list for each row of input data
        to_db_tbl1 = [(row['origin'], row['destination'], row['deptime'], row['traveltime']) for row in ttmatrix1]

    #Batch process the data into table 1
    try:
        cur.executemany(
            "INSERT INTO {} (origin, destination, deptime, traveltime) VALUES (?, ?, ?, ?);".format(args.TABLE1_NAME),
            to_db_tbl1)
    except sqlite3.OperationalError:
        print('Error: Values already inserted to {}.'.format(args.TABLE1_NAME))

    # Create a new table in the SQLite database where the pnr2d data will go.
    try:
        cur.execute('CREATE TABLE IF NOT EXISTS {} (origin VARCHAR, destination VARCHAR, deptime VARCHAR, traveltime BIGINT);'
                    .format(args.TABLE2_NAME))

    except sqlite3.OperationalError:
        print('Error: Please debug.')

    #Open the data for table 2 into a DictReader Object
    with open('{}'.format(args.TABLE2_PATH_DATA)) as mydata:
        ttmatrix2 = csv.DictReader(mydata)
        to_db_tbl2 = [(row['origin'], row['destination'], row['deptime'], row['traveltime']) for row in ttmatrix2]

    #Batch process the data into table 1
    try:
        cur.executemany(
            "INSERT INTO {} (origin, destination, deptime, traveltime) VALUES (?, ?, ?, ?);".format(args.TABLE2_NAME),
            to_db_tbl2)
    except sqlite3.OperationalError:
        print('Error: Values already inserted to {}.'.format(args.TABLE2_NAME))

    # Add a column to table 1 & 2 to translate deptimes into an integer in seconds.
    try:
        cur.execute('ALTER TABLE {} ADD deptime_sec BIGINT;'.format(args.TABLE1_NAME))
        cur.execute('UPDATE {} SET deptime_sec = SUBSTR(deptime, 1, 2) * 3600 + SUBSTR(deptime, 3, 4) * 60;'.format(args.TABLE1_NAME))

        cur.execute('ALTER TABLE {} ADD deptime_sec BIGINT;'.format(args.TABLE2_NAME))
        cur.execute('UPDATE {} SET deptime_sec = SUBSTR(deptime, 1, 2) * 3600 + SUBSTR(deptime, 3, 4) * 60;'.format(args.TABLE2_NAME))

    except sqlite3.OperationalError:
        print('Error: Column data already exists in tables.')

    #Create Indices on the new deptime_sec column of the tables.
    try:
        cur.execute('CREATE INDEX o2pnr_deptime_origin ON {} (deptime_sec ASC, origin ASC);'.format(args.TABLE1_NAME))
        cur.execute('CREATE INDEX pnr2d_deptime_origin ON {} (deptime_sec ASC, origin ASC);'.format(args.TABLE2_NAME))
    except sqlite3.OperationalError:
        print('Error: Indices already created')

    # Create a new table in the SQLite database where the jobs data will go.
    try:
        cur.execute(
            'CREATE TABLE IF NOT EXISTS {} (geoid10 VARCHAR, C000 BIGINT);'.format(
                args.JOBS_TABLE_NAME))

    except sqlite3.OperationalError:
        print('Error: Jobs Table: Please debug')

    # Open the data for table into a DictReader Object
    with open('{}'.format(args.JOBS_PATH_DATA)) as mydata:
        jobs = csv.DictReader(mydata)
        # Produce list for each row of input data
        to_db_jobs = [(row['GEOID10'], row['C000']) for row in jobs]

    # Batch process the data into table 1
    try:
        cur.executemany(
            "INSERT INTO {} (geoid10, C000) VALUES (?, ?);".format(
                args.JOBS_TABLE_NAME),
            to_db_jobs)
    except sqlite3.OperationalError:
        print('Error: Values already inserted to {}.'.format(args.JOBS_TABLE_NAME))


    #Committing change and closing the connection to the database file.
    con.commit()
    con.close()