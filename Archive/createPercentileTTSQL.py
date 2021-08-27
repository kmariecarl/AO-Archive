#This script reads in data from a sqlite db where the o2pnr and pnr2d matrices are stored. The script adds a third table
#that mainly recreates the pnr2d table but instead stores the 15th percentile travel time for 15 minute bins.

#Assumption:
#The fifteenth percentile of travel times is taken for destinations that do not equal the max tt. Ex. a destination that
#cannot be reached for 12/15 of the minutes between 6:00-6:14 but for 3 of those minutes can be, just those 3 TT will
#be used to calculate the 15th percentile TT.Then if the percentile calculated is between two real TT, the nearest TT
#is chosen instead of interpolating between them.

#The binning process rounds down so that a percentile is calculated for 6:00-8:45 but no percentile is calculated for 9:00.

#Currently if this script is run more than once on the same DB, duplicate rows are inserted.
#EXAMPLE USAGE: kristincarlson$ python createPercentileTTSQL.py -path <file path> -name <db.sqlite> -table1 <o2pnr>
# -table2 <pnr2d> -table3 <pnr2d15>


#################################
#           IMPORTS             #
#################################
import argparse
import numpy
from myToolsPackage import matrixLinkModule as mod
import sqlite3


#################################
#           FUNCTIONS           #
#################################

# Query the o2pnr and pnr2d matrices to make lists of unique origins and deptimes.
def makeList():

    # cur.execute("SELECT DISTINCT origin FROM {};".format( TABLE2)) #This grabs PNRS!
    # origins = cur.fetchall()
    # #Turns the fetchall object to a normal list
    # origin_list = list(sum(origins, ()))
    # print('ORIGIN LIST SELECTED:')
    # print(origin_list)
    #
    # cur.execute("SELECT DISTINCT deptime_sec FROM {};".format(TABLE1)) #This grabs the deptime series of the origin set
    # or_depsec = cur.fetchall()
    # or_depsec_list = list(sum(or_depsec, ()))
    # print('ORIGIN DEPARTURE LIST SELECTED:')
    # print(or_depsec_list)
    #
    # cur.execute("SELECT DISTINCT destination FROM {};".format(TABLE2)) #This grabs the final destinations
    # dest = cur.fetchall()
    # dest_list = list(sum(dest, ()))
    # print('DESTINATION LIST SELECTED:')
    # print(dest_list)

    #Faster way to select unique list of PNRs, deptimes, Destinations
    cur.execute("SELECT {}.origin FROM {} GROUP BY origin;".format(TABLE2, TABLE2))
    origins = cur.fetchall()
    origin_list = list(sum(origins, ()))

    cur.execute("SELECT {}.deptime_sec FROM {} GROUP BY deptime_sec;".format(TABLE1, TABLE1))
    or_depsec = cur.fetchall()
    or_depsec_list = list(sum(or_depsec, ()))

    cur.execute("SELECT {}.destination FROM {} GROUP BY destination;".format(TABLE2, TABLE2))
    dest = cur.fetchall()
    dest_list = list(sum(dest, ()))

    return origin_list, or_depsec_list, dest_list

#This function finds the 15 TT values for each destination, then sends to the 'calcPercentile' function before returning
#and getting written to file and to the db.
def triageTT(pnr, dest): #, or_depsec_list_sort):
    #Assuming that the pnr2d matrix has a bin column added...
    for bin in bin_list:
        or_dep = bin_dict[bin]
        or_dep_sec = str(mod.back2Time(or_dep))
        cur.execute('SELECT traveltime FROM {} WHERE origin = ? AND bin = ? AND destination = ? ORDER BY traveltime ASC LIMIT 1 OFFSET 1'.format(TABLE2), (pnr, bin, dest))
        bin15 = cur.fetchone()
        new_TT = bin15[0] #Always returns the 2nd item which will also be the lowest
        cur.execute('INSERT INTO {} (origin, destination, deptime, traveltime, deptime_sec) VALUES (?, ?, ?, ?, ?);'.format(TABLE3), (pnr, dest, or_dep, or_dep_sec, new_TT ))
        con.commit()

    # # Cycle through 15 minute departure time list from origin and apply to destinations
    # #Or_depsec_list_sort contains the 12 bins (6:00, 6:15, 6:30...8:45) in seconds and sorted
    # for index, or_dep_sec in enumerate(or_depsec_list_sort):
    #     next = index + 1
    #     # Make sure not to exceed 12th item in list
    #     if next < len(or_depsec_list_sort):
    #         cur.execute('SELECT traveltime '
    #                     'FROM {} '
    #                     'WHERE deptime_sec >= ? AND deptime_sec < ? AND origin = ? AND destination = ?'
    #                     'ORDER BY deptime_sec;'.format(TABLE2), (or_dep_sec, or_depsec_list_sort[next], pnr, dest))
    #
    #         # Create list to store TT to calc 15th percentile
    #         bin15 = cur.fetchall()
    #         con.commit()
    #         #Resolve to python list
    #         new_bin15 = list(sum(bin15, ()))
    #         # Calc 15th percentile TT
    #         new_tt = calcPercentile(new_bin15)
    #         #Return origin departure time bin back to timestamp
    #         or_dep = str(mod.back2Time(or_dep_sec))
    #
    #         WRITER.writerow([pnr, dest, or_dep, or_dep_sec, new_tt])
    #         #Add new percentile TT to table 3.
    #         cur.execute('INSERT INTO {} (origin, destination, deptime, deptime_sec, traveltime) VALUES (?, ?, ?, ?, ?);'.format(TABLE3), (pnr, dest, or_dep, or_dep_sec, new_tt))
    #         con.commit()


# #This function calculates the bottom 15th percentile travel time ~= 85th percentile for a particular OD Pair
# def calcPercentile(new_bin15):
#     if allSame(new_bin15) is True:
#         tile = 2147483647
#     #This list comprehension makes a new list that does not include max values.
#     else:
#         #This function will choose the lower (and more realistic) value when the percentile value falls between a sub 90 min
#         #time and a maxtime. Otherwise if the percentile falls between two maxtimes, then the Maxtime is chosen.
#         tile = numpy.percentile(new_bin15, 15, interpolation='lower')
#     return int(tile)
#
# #Check if list contains all Maxtime values (all the same values)
# def allSame(items):
#     return all(x == items[0] for x in items)

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()

    #Read in pnr2d from sqlite file then in python find the TT and assign to bin, then export each line to the sqlite db
    parser = argparse.ArgumentParser()
    parser.add_argument('-path', '--FOLDER_PATH', required=True, default=None)  #ENTER AS /Users/kristincarlson/Dropbox/Bus-Highway/Task3/Restart2/IntermodalAccess/1_TTMatrixLink/
    parser.add_argument('-name', '--DB_NAME', required=True, default=None)  #ENTER AS O2PNR.sqlite
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #ENTER the name of the o2pnr table
    parser.add_argument('-table2', '--TABLE2_NAME', required=True, default=None)  #ENTER the name of the pnr2d table
    parser.add_argument('-table3', '--TABLE3_NAME', required=True, default=None)  #ENTER the name of the new table file i.e. pnr2d15
    parser.add_argument('-jobstab', '--JOBS_TABLE_NAME', required=False, default=None) #ENTER the jobs table name
    args = parser.parse_args()

    DB_NAME = args.DB_NAME
    TABLE1 = args.TABLE1_NAME
    TABLE2 = args.TABLE2_NAME
    TABLE3 = args.TABLE3_NAME
    JOBS = args.JOBS_TABLE_NAME

    sqlite_file = '{}{}'.format(args.FOLDER_PATH, DB_NAME)

    con = sqlite3.connect(sqlite_file)
    cur = con.cursor()

    #Now load in data from the sqlite db.
    pnrList, orDepSecList, destList = makeList()
    #orDepSecListSort = sorted(orDepSecList)
    print('INPUT LISTS CREATED')
    bin_list = list(range(1, 12, 1))
    #Match binum to departure time in seconds
    bin_dict = {}
    bin_dict[1] = 21600
    bin_dict[2] = 22500
    bin_dict[3] = 23400
    bin_dict[4] = 24300
    bin_dict[5] = 25200
    bin_dict[6] = 26100
    bin_dict[7] = 27000
    bin_dict[8] = 27900
    bin_dict[9] = 28800
    bin_dict[10] = 29700
    bin_dict[11] = 30600
    bin_dict[12] = 31500

    print("Bin Dictionary in seconds:")
    print(bin_dict)

    # Create a new table in the SQLite database where the percentile TT data will go.
    try:
        cur.execute(
        'CREATE TABLE IF NOT EXISTS {} (origin VARCHAR, destination VARCHAR, deptime VARCHAR, traveltime BIGINT, deptime_sec BIGINT);'.format(TABLE3))
        con.commit()
    except sqlite3.OperationalError:
        print('Error: Table already exists in sqlite db.')
    print('TABLE3 {} ADDED TO SQLITE DB'.format(TABLE3))

    # #Output file fieldnames
    # fieldnames = ['origin', 'destination', 'deptime', 'deptime_sec', 'traveltime']
    # file_name = '{}'.format(args.TABLE3_NAME)
    # WRITER = mod.mkOutput(file_name, fieldnames)


    #Calculate the 15th percentile tt for each PNR to destination pair in each 15 min bin.
    for pnr in pnrList:
        for dest in destList:
            triageTT(pnr, dest) #, orDepSecListSort)
        print('The 15th percentile TT has been calculated and recorded for PNR {}'.format(pnr))

    # try:
    #     query = """SELECT jobs FROM {}"""
    #     cur.execute(query.format(args.TABLE3_NAME))
    #
    # except sqlite3.OperationalError:
    #     # Add jobs data to pnr2d15
    #     cur.execute('ALTER TABLE {} ADD myjobs BIGINT;'.format(args.TABLE3_NAME))
    #     con.commit()
    #     query = """UPDATE pnr2d15
    #                 SET myjobs = (SELECT C000
    #                 FROM jobs
    #                 WHERE jobs.geoid10 = pnr2d15.destination);"""
    #     cur.execute(query)
    #     con.commit()


    print('Jobs data matched to {}'.format(args.TABLE3_NAME))
    #Once the new table has been created, add indexing
    cur.execute('CREATE INDEX IF NOT EXISTS {}_deptime_origin ON {} (deptime_sec ASC, origin ASC);'.format(args.TABLE3_NAME, args.TABLE3_NAME))
    print('Index created on pnr2d15 table')


    #Write index to table
    con.commit()
    con.close()
    mod.elapsedTime(start_time)
