#Use this script to load the 15th percentile files or min files by PNR into a single Pnr2d15 or pnr2d15min table within a PostgreSQL db.

#Example Usage: kristincarlson~ python ~/..TTMatrixLink_Testing -db TTMatrixLink -schema test -table3 pnr2d15 -path /Volumes/Thang_2/Task3_Extension/Transit_PNR_D_TT_0.2.2-all/Transit_PNR_D_TT_78_Calc/PNR_MinTT

#################################
#           IMPORTS             #
#################################
from myToolsPackage import matrixLinkModule as mod
import argparse
import psycopg2

#################################
#           FUNCTIONS           #
#################################

def createExtensionList(pnr_list_file):
    pnr_list = mod.readList(pnr_list_file, 'integer')
    extension_list = []
    for pnr in pnr_list:
        extension_name = 'PNR_{}_MinTT.csv'.format(pnr)
        extension_list.append(extension_name)
    return extension_list



def createTable():
    query = "CREATE TABLE IF NOT EXISTS {}.{} (origin INTEGER, destination BIGINT, deptime CHAR(4), traveltime INTEGER)"
    cur.execute(query.format(SCHEMA, TABLE3))
    print(cur.mogrify(query.format(SCHEMA, TABLE3)))

def insertFileInfo(extension_list):
    for extension in extension_list:
        query = "COPY {}.{} FROM '{}{}' DELIMITER ',' CSV HEADER"
        cur.execute(query.format(SCHEMA, TABLE3, PATH, extension))
        print(cur.mogrify(query.format(SCHEMA, TABLE3, PATH, extension)))
        mod.elapsedTime(start_time)
def addDeptimeSec():
    query = "ALTER TABLE {}.{} ADD COLUMN deptime_sec integer; UPDATE {}.{} SET deptime_sec = cast(substring(deptime, 1, 2) as integer) *3600 + cast(substring(deptime, 3, 4) as integer) * 60 "
    cur.execute(query.format(SCHEMA, TABLE3, SCHEMA, TABLE3))
    print(cur.mogrify(query.format(SCHEMA, TABLE3, SCHEMA, TABLE3)))

def createIndex():
    print('Creating index on origin')
    query1 = "CREATE INDEX origin_pnr2dmin ON {}.{} (origin);"
    cur.execute(query1.format(SCHEMA, TABLE3))

    query2 =  "CREATE INDEX deptime_sec_pnr2dmin ON {}.{} (deptime_sec);"
    cur.execute(query2.format(SCHEMA, TABLE3))
    print(cur.mogrify(query2.format(SCHEMA, TABLE3)))
    print('Index deptime_sec_pnr2dmin added to table {}'.format(TABLE3))

    query3 =  "CREATE INDEX destination_sec_pnr2dmin ON {}.{} (destination);"
    cur.execute(query3.format(SCHEMA, TABLE3))
    print(cur.mogrify(query3.format(SCHEMA, TABLE3)))
    print('Index deptime_sec_pnr2dmin added to table {}'.format(TABLE3))


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  #ENTER AS TTMatrixLink
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS ttmatrices
    parser.add_argument('-table3', '--TABLE3_NAME', required=True, default=None)  #Table 3 in schema, i.e. pnr2d15 or pnr2dmin
    parser.add_argument('-path', '--FILE_PATH', required=True, default=None)  #The entire file path where all the percentile files are stored.
    parser.add_argument('-pnr', '--PNR_LIST', required=True, default=None)  #The file that stores a list of PNRs for use in creating extensions
    args = parser.parse_args()

    DB_NAME = args.DB_NAME
    SCHEMA = args.SCHEMA_NAME
    TABLE3 = args.TABLE3_NAME
    PATH = args.FILE_PATH
    PNRList = args.PNR_LIST

    try:
        con = psycopg2.connect("dbname = '{}' user='aodbadmin' host='localhost' password=''".format(DB_NAME))
        con.set_session(autocommit=True)
    except:
        print('I am unable to connect to the database')

    #Initiate cursor object on db
    cur = con.cursor()

    extensionList = createExtensionList(PNRList)
    createTable()
    insertFileInfo(extensionList)
    addDeptimeSec()
    createIndex()