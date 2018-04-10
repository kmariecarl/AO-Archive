#Use this script to load the 15th percentile files by PNR into a single Pnr2d15 table within a PostgreSQL db.

#Example Usage: kristincarlson~ python ~/..TTMatrixLink_Testing -db TTMatrixLink -schema test -table3 pnr2d15 -path ./UnitTest_Percentile/

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
    pnr_list = mod.readList(pnr_list_file)
    extension_list = []
    for pnr in pnr_list:
        extension_name = 'PNR15_{}_destination.txt'.format(pnr)
        extension_list.append(extension_name)
    return extension_list



def createTable():
    query = "CREATE TABLE IF NOT EXISTS {}.{} (origin VARCHAR, destination VARCHAR, deptime SMALLINT, traveltime INTEGER)"
    cur.execute(query.format(SCHEMA, TABLE3))
    print(cur.mogrify(query.format(SCHEMA, TABLE3)))

def insertFileInfo(extension_list):
    for extension in extension_list:
        query = "COPY {}.{} FROM '{}{}' DELIMITER ',' CSV HEADER"
        cur.execute(query.format(SCHEMA, TABLE3, PATH, extension))
        print(cur.mogrify(query.format(SCHEMA, TABLE3, PATH, extension)))
        mod.elapsedTime(start_time)

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  #ENTER AS TTMatrixLink
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS ttmatrices
    parser.add_argument('-table3', '--TABLE3_NAME', required=True, default=None)  #Table 3 in schema, i.e. pnr2d15
    parser.add_argument('-path', '--FILE_PATH', required=True, default=None)  #The file path where all the percentile files are stored.
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