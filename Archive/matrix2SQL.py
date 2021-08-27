#Use this script for creating the travel time matrix db.
#The db path and name provided by the user initiates the db.
#Three tables are created, the o2pnr, pnr2d, and jobs tables.
#A fifth column is added which converts the deptime to seconds for use in the TTMatrixLinkSQL.py script
#Table 2 data is expected to be in a .bz2 zipped file.

#Example Usage: kristincarlson~ python matrix2SQL.py -path /Users/kristincarlson/Dropbox/Bus-Highway/Task3/
# Restart2/IntermodalAccess/1_TTMatrixLink/ -name newdbname.sqlite -table1 o2pnr -data1 <path/file.txt>
#-table2 pnr2d -data2 <path/file.txt> -jobstab jobs -jobsdata <path/geoid2jobs.csv>

import sqlite3
import csv
import argparse
import bz2
import progressbar
from myToolsPackage import matrixLinkModule as mod

#################################
#           FUNCTIONS           #
#################################
def createTables():
    # Create a new table in the SQLite database where the o2pnr data will go.
    try:
        cur.execute(
            'CREATE TABLE IF NOT EXISTS {} (origin VARCHAR, destination VARCHAR, deptime VARCHAR, traveltime BIGINT);'
                .format(TABLE1))

    except sqlite3.OperationalError:
        print('Error: Please debug')

    # Create a new table in the SQLite database where the pnr2d data will go.
    try:
        cur.execute(
            'CREATE TABLE IF NOT EXISTS {} (origin VARCHAR, destination VARCHAR, deptime VARCHAR, traveltime BIGINT, deptime_sec BIGINT);'
            .format(TABLE2))
    except sqlite3.OperationalError:
        print('Error: Please debug.')

    # Create a new table in the SQLite database where the jobs data will go.
    try:
        cur.execute(
            'CREATE TABLE IF NOT EXISTS {} (geoid10 VARCHAR, C000 BIGINT);'.format(JOBS))

    except sqlite3.OperationalError:
        print('Error: Jobs Table: Please debug')

    print('Tables {}, {}, and {} created'.format(TABLE1, TABLE2, JOBS))
    #bar.update()

def insertO2P():
    # Open the data for table 1 into a DictReader Object
    #fieldnames = ('origin', 'destination', 'deptime', 'traveltime')
    with open('{}'.format(args.TABLE1_PATH_DATA)) as f:
        ttmatrix1 = csv.DictReader(f) #, fieldnames=fieldnames, delimiter=',')
        # Produce list for each row of input data stored
        to_db_tbl1 = [(row['origin'], row['destination'], row['deptime'], row['traveltime']) for row in ttmatrix1]
        print('Origin info added to list {}'.format(DB_NAME))
        #bar.update()
    # Batch process the data into table 1
    try:
        cur.executemany(
            "INSERT INTO {} (origin, destination, deptime, traveltime) VALUES (?, ?, ?, ?);"
                .format(TABLE1), to_db_tbl1)
        #bar.update()
        print('Origin table added to db {}'.format(DB_NAME))

    except sqlite3.OperationalError:
        print('Error: Values already inserted to {}.'.format(args.TABLE1_NAME))

def insertP2D():
    # Open the data for table 2 into a DictReader Object
    fieldnames = ('origin', 'destination', 'deptime', 'traveltime')
    with bz2.open(args.TABLE2_PATH_DATA, "rt") as f:  # open the bz2 file in rt = text mode
        reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',')  # Hand f obj. right to DictReader
        count = 0
        for row in reader:
            if count != 0:
                #bar.update()
                # Iteratively create the deptime_sec rows.
                deptime_sec = mod.convert2Sec(row['deptime'])
                cur.execute("INSERT INTO {} (origin, destination, deptime, traveltime, deptime_sec) VALUES (?, ?, ?, ?, ?);"
                            .format(TABLE2),
                            (row['origin'], row['destination'], row['deptime'], row['traveltime'], deptime_sec))
            count += 1
        print('Destination table added to db {}'.format(DB_NAME))

def insertJobs():
    # Open the data for table into a DictReader Object
    with open('{}'.format(args.JOBS_PATH_DATA)) as jobsfile:
        jobs = csv.DictReader(jobsfile)
        # Produce list for each row of input data
        to_db_jobs = [(row['GEOID10'], row['C000']) for row in jobs]
        #bar.update()
    # Batch process the data into table 1
    try:
        cur.executemany(
            "INSERT INTO {} (geoid10, C000) VALUES (?, ?);".format(JOBS),to_db_jobs)
        #bar.update()
        print('Jobs table finished')
    except sqlite3.OperationalError:
        print('Error: Values already inserted to {}.'.format(JOBS))

def updateO2PField():
    # Add a column to table 1 to translate deptimes into an integer in seconds. No need to do this for table 2 because
    #I did it in the original loop.
    try:
        cur.execute('ALTER TABLE {} ADD deptime_sec BIGINT;'.format(TABLE1))
        cur.execute('UPDATE {} SET deptime_sec = SUBSTR(deptime, 1, 2) * 3600 + SUBSTR(deptime, 3, 4) * 60;'.format(TABLE1))
        #bar.update()
        print('Deptimes converted to Seconds for {} table'.format(TABLE1))

    except sqlite3.OperationalError:
        print('Error: Column data already exists in table.')

def createIndices():
    #Create Indices on the new deptime_sec column of the tables.
    try:
        cur.execute('CREATE INDEX o2pnr_deptime_origin ON {} (deptime_sec ASC, origin ASC);'.format(TABLE1))
        cur.execute('CREATE INDEX pnr2d_deptime_origin ON {} (deptime_sec ASC, origin ASC);'.format(TABLE2))
        #bar.update()
        print("Indices Added")
    except sqlite3.OperationalError:
        print('Error: Indices already created')


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    #Introduce progressbar
    #widgets = [progressbar.Percentage(), progressbar.Bar()]
    #bar = progressbar.ProgressBar(widgets=widgets, max_value=progressbar.UnknownLength).start()

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

    DB_NAME = args.DB_NAME
    TABLE1 = args.TABLE1_NAME
    TABLE2 = args.TABLE2_NAME
    JOBS = args.JOBS_TABLE_NAME

    #Make db
    sqlite_file = '{}{}'.format(args.FOLDER_PATH, DB_NAME)

    #Initiate cursor object on db
    con = sqlite3.connect(sqlite_file)
    cur = con.cursor()

    #Execute functions:
    createTables()
    insertO2P()
    insertP2D()
    insertJobs()
    updateO2PField()
    createIndices()


    print('DB {} has been created and filled'.format(DB_NAME))
    #Committing change and closing the connection to the database file.
    #bar.finish()
    con.commit()
    con.close()