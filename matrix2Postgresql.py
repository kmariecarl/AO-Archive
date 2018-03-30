#Use this script for creating the travel time matrix db.
#The connection parameters may need to be manipulated in the con statement when connecting to a new DB server.
#Three tables are created, the o2pnr, pnr2d, and jobs tables.
#A fifth column is added which converts the deptime to seconds for use in the TTMatrixLinkSQL.py script
#Table 2 data is expected to be in a .bz2 zipped file.

#Example Usage: kristincarlson~ python matrix2SQL.py -path /Users/kristincarlson/Dropbox/Bus-Highway/Task3/
# Restart2/IntermodalAccess/1_TTMatrixLink/ -name newdbname.sqlite -table1 o2pnr -data1 <path/file.txt>
#-table2 pnr2d -data2 <path/file.txt> -jobstab jobs -jobsdata <path/geoid2jobs.csv>

import psycopg2
import csv
import argparse
import bz2
import progressbar
from myToolsPackage import matrixLinkModule as mod

#################################
#           FUNCTIONS           #
#################################
def createSchema():
    try:
        query = "CREATE SCHEMA IF NOT EXISTS {};".format(SCHEMA_NAME)
        cur.execute(query,)
    except:
        print('Error: Schema already exists')

def createTables():
    # Create a new table in the postgresql database where the o2pnr data will go.
    try:
        query = "CREATE TABLE IF NOT EXISTS {}.{} (origin VARCHAR, destination VARCHAR, deptime VARCHAR, traveltime BIGINT);"\
            .format(SCHEMA_NAME,TABLE1)
        cur.execute(query,)
    except:
        print('Error: Please debug')

    # Create a new table in the postgresql database where the pnr2d data will go.
    try:
        query = "CREATE TABLE IF NOT EXISTS {}.{} (origin VARCHAR, destination VARCHAR, deptime VARCHAR, traveltime BIGINT, deptime_sec BIGINT bin INTEGER);"\
            .format(SCHEMA_NAME,TABLE2)
        cur.execute(query,)

    except:
        print('Error: Please debug.')

    # Create a new table in the postgresql database where the jobs data will go.
    try:
        query = 'CREATE TABLE IF NOT EXISTS {}.{} (geoid10 VARCHAR, C000 BIGINT);'.format(SCHEMA_NAME, JOBS)
        cur.execute(query,)

    except:
        print('Error: Jobs Table: Please debug')


    print('Tables {}, {}, and {} created'.format(TABLE1, TABLE2, JOBS))

#Use this function before inserting more data, clears existing data in table
def truncateTables(db, schema, table):
    try:
        query = "TRUNCATE {}.{}.{};".format(db, schema, table)
        cur.execute(query,)
    except:
        print('Table {} could not be truncated'.format(table))


def insertO2P():
    #Clear data that may already be in table from previous run
    truncateTables(DB_NAME, SCHEMA_NAME, TABLE1)
    # Open the data for table 1 into a DictReader Object
    with open('{}'.format(args.TABLE1_PATH_DATA)) as f:
        ttmatrix1 = csv.DictReader(f)
        # Produce list for each row of input data stored
        to_db_tbl1 = [(row['origin'], row['destination'], row['deptime'], row['traveltime']) for row in ttmatrix1]

    # Batch process the data into table 1
    try:
        cur.executemany(
            "INSERT INTO {}.{} (origin, destination, deptime, traveltime) VALUES (%s, %s, %s, %s);"
                .format(SCHEMA_NAME,TABLE1), to_db_tbl1)

        print('Origin table added to schema {}'.format(SCHEMA_NAME))

    except:
        print('Error: Values already inserted to {}.'.format(args.TABLE1_NAME))

def insertP2D():
    truncateTables(DB_NAME, SCHEMA_NAME, TABLE2)
    # Open the data for table 2 into a DictReader Object
    fieldnames = ('origin', 'destination', 'deptime', 'traveltime')
    with bz2.open(args.TABLE2_PATH_DATA, "rt") as f:  # open the bz2 file in rt = text mode
        reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',')  # Hand f obj. right to DictReader
        count = 0
        for row in reader:
            if count != 0:
                # Iteratively create the deptime_sec rows.
                deptime_sec = mod.convert2Sec(row['deptime'])
                bin = assignBin(deptime_sec)

                cur.execute("INSERT INTO {}.{} VALUES (%s, %s, %s, %s, %s, %s);".format(SCHEMA_NAME,TABLE2),
                            (row['origin'], row['destination'], row['deptime'], row['traveltime'], deptime_sec, bin))
            count += 1
        print('Destination table added to schema {}'.format(SCHEMA_NAME))

def assignBin(deptime_sec):
    if deptime_sec >= 21600 and deptime_sec < 22500:
        bin = 1
    elif deptime_sec >= 22500 and deptime_sec < 23400:
        bin = 2
    elif deptime_sec >= 23400 and deptime_sec < 24300:
        bin = 3
    elif deptime_sec >= 24300 and deptime_sec < 25200:
        bin = 4
    elif deptime_sec >= 25200 and deptime_sec < 26100:
        bin = 5
    elif deptime_sec >= 26100 and deptime_sec < 27000:
        bin = 6
    elif deptime_sec >= 27000 and deptime_sec < 27900:
        bin = 7
    elif deptime_sec >= 27900 and deptime_sec < 28800:
        bin = 8
    elif deptime_sec >= 28800 and deptime_sec < 29700:
        bin = 9
    elif deptime_sec >= 29700 and deptime_sec < 30600:
        bin = 10
    elif deptime_sec >= 30600 and deptime_sec < 31500:
        bin = 11
    elif deptime_sec >= 31500 and deptime_sec <= 32400:
        bin = 12
    return bin


def insertJobs():
    truncateTables(DB_NAME, SCHEMA_NAME, JOBS)
    # Open the data for table into a DictReader Object
    with open('{}'.format(args.JOBS_PATH_DATA)) as jobsfile:
        jobs = csv.DictReader(jobsfile)
        # Produce list for each row of input data
        to_db_jobs = [(row['GEOID10'], row['C000']) for row in jobs]
    # Batch process the data into table 1
    try:
        cur.executemany(
            "INSERT INTO {}.{} (geoid10, C000) VALUES (%s, %s);".format(SCHEMA_NAME,JOBS),to_db_jobs)

        print('Jobs table finished')
    except:
        print('Error: Values already inserted to {}.'.format(JOBS))

def updateO2PField():
    # Add a column to table 1 to translate deptimes into an integer in seconds. No need to do this for table 2 because
    #I did it in the original loop.
    try:
        cur.execute('ALTER TABLE {}.{} ADD deptime_sec BIGINT;'.format(SCHEMA_NAME,TABLE1))
        cur.execute('UPDATE {}.{} SET deptime_sec = SUBSTR(deptime, 1, 2) * 3600 + SUBSTR(deptime, 3, 4) * 60;'.format(SCHEMA_NAME,TABLE1))

        print('Deptimes converted to Seconds for {} table'.format(TABLE1))

    except:
        print('Error: Column data already exists in table.')

def createIndices():
    #Create Indices on the new deptime_sec column of the tables.
    try:
        cur.execute('CREATE INDEX o2pnr_deptime_origin ON {}.{} (deptime_sec ASC, origin ASC);'.format(SCHEMA_NAME,TABLE1))
        cur.execute('CREATE INDEX pnr2d_deptime_origin ON {}.{} (deptime_sec ASC, origin ASC);'.format(SCHEMA_NAME, TABLE2))

        print("Indices Added")
    except:
        print('Error: Indices already created')


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    #parser.add_argument('-path', '--FOLDER_PATH', required=True, default=None)  #ENTER AS /Users/kristincarlson/Dropbox/Bus-Highway/Task3/Restart2/IntermodalAccess/1_TTMatrixLink/
    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  # ENTER AS Testing
    parser.add_argument('-name', '--SCHEMA_NAME', required=True, default=None)  #ENTER AS Testing
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #Assign Table 1 name in schema, i.e. pnr2d15
    parser.add_argument('-data1', '--TABLE1_PATH_DATA', required=True, default=None)  #List path to .csv file
    parser.add_argument('-table2', '--TABLE2_NAME', required=True, default=None)  #Assign Table 2 name in schema, i.e. pnr2d15
    parser.add_argument('-data2', '--TABLE2_PATH_DATA', required=True, default=None)  #List path to .csv file
    parser.add_argument('-jobstab', '--JOBS_TABLE_NAME', required=False, default=None)  #Assign jobs table name in schema, i.e. jobs
    parser.add_argument('-jobsdata', '--JOBS_PATH_DATA', required=False, default=None)  #List path to .csv file
    args = parser.parse_args()

    DB_NAME = args.DB_NAME
    SCHEMA_NAME = args.SCHEMA_NAME
    TABLE1 = args.TABLE1_NAME
    TABLE2 = args.TABLE2_NAME
    JOBS = args.JOBS_TABLE_NAME

    try:
        con = psycopg2.connect("dbname = '{}' user='aodbadmin' host='localhost' password=''".format(DB_NAME))
        con.set_session(autocommit=True)
    except:
        print('I am unable to connect to the database')

    #Initiate cursor object on db
    cur = con.cursor()

    #Execute functions:
    createSchema()
    createTables()
    insertO2P()
    insertP2D()
    insertJobs()
    updateO2PField()
    createIndices()


    print('Schema {} has been created and filled'.format(SCHEMA_NAME))
    #All changes are autocommited, closing the connection to the database.
    con.close()