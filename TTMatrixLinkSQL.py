#Assuming that TT matrices are loaded into sqlite db.

#Assumption: If an origin reaches a given PNR after the time window (i.e. 9:00) then origin is assigned
#maxtime for that particular origin, deptime, PNR combo.

#Assumption: Depsums are placed in bins according to their rounded up time. I.e. 6:21 depsum goes to the 6:30
#bin. This ensures transit rides do not depart before origin trip arrives.

#Notes: Later I will add in the ability to maintain the PNR that was used for each minimum TT.
#SQLite db is indexed on deptime_sec.origin for both the o2pnr and pnr2d15 tables

import sqlite3
import csv
from myToolsPackage import matrixLinkModule as mod
import numpy as np
import argparse

#################################
#           FUNCTIONS           #
#################################

# Query the o2pnr matrix to make lists of unique origins, deptimes, and PNRs
def makeList():
    #with con.cursor() as cur:
    cur.execute("SELECT DISTINCT origin FROM {};".format(TABLE1))
    origins = cur.fetchall()
    origin_list = list(sum(origins, ()))
    print(origin_list)

    cur.execute("SELECT DISTINCT destination FROM {};".format(TABLE1)) #This grabs PNRS!
    pnrs = cur.fetchall()
    pnr_list = list(sum(pnrs, ()))
    print(pnr_list)

    cur.execute("SELECT DISTINCT deptime_sec FROM {};".format(TABLE1))
    deptimes = cur.fetchall()
    deptime_list = list(sum(deptimes, ()))
    print(deptime_list)

    cur.execute("SELECT DISTINCT destination FROM {};".format(TABLE2))
    destinations = cur.fetchall()
    destination_list = list(sum(destinations, ()))
    print(destination_list)

    return origin_list, pnr_list, deptime_list, destination_list

#Query the o2pnr matrix for matching origin, deptime, pnr combo
def matchOrigin(origin, deptime_sec, pnr):

    cur.execute('SELECT traveltime FROM {} WHERE deptime_sec = ? AND origin = ? AND destination = ? ORDER BY deptime_sec, origin;'
                .format(TABLE1), (deptime_sec, origin, pnr))
    tt = cur.fetchall()
    return int(tt[0][0])
#Place depsum values into a 15 minute bin. Ex. 6:05 will get placed in the 6:15 bin.
#Bins are rounded up to allow for transfer time between origin egress to destination ingress
def linkBins(deptime_list_sort, depsum):
    # Assign depsum to the 15 minute bin
    for index, dptm in enumerate(deptime_list_sort):
        next = index + 1
        if next < len(deptime_list_sort):
            if depsum > dptm and depsum <= deptime_list_sort[next]:
                # Depsum_bin is in the form of seconds!
                depsum_bin = deptime_list_sort[next]
                return depsum_bin
#Each use of this function returns an array of TT that are in order of their destination. MaxTT are included
#so that the array size always equals 'dest_num'.
def matchDestination(depsum_bin, pnr):
    #Sort traveltimes into list where they are at the same index everytime. The maxTT value is listed for any dest
    #that cannot be reached.

    next_bin = depsum_bin + 900

    #Because the pnr2d15 matrix is only indexed on deptime and origin and not destination, the resulting array of tt will
    #be stored in memory while the ordering happens. If indexed on the destination, this memory process would not happen.
    cur.execute('SELECT traveltime FROM {} WHERE deptime_sec >= ? AND deptime_sec < ? AND origin = ? ORDER BY DESTINATION'.format(TABLE2), (depsum_bin, next_bin, pnr))
    tt = cur.fetchall()  #Listed by ordered destination

    tt_list = np.array(list(sum(tt, ())))

    if len(tt_list) == 0:
        tt_list = np.array([2147483647] * dest_num)

    if len(tt_list) != dest_num:
        print('ERROR!!')
    print(tt_list)
    return tt_list

def writeEntry(origin, destination_list, deptime, dest_TT_prev):
    for dest, tt in zip(destination_list, dest_TT_prev):
        entry = [origin, dest, deptime, tt]
        writer.writerow(entry)

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-path', '--DB_PATH', required=True, default=None)  #ENTER AS /Users/kristincarlson/Dropbox/Bus-Highway/Task3/TTMatrixLink_Testing/
    parser.add_argument('-name', '--DB_NAME', required=True, default=None)  #ENTER AS TTMatrix_TestingDB.sqlite
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #Table 1 in schema, i.e. o2pnr
    parser.add_argument('-table2', '--TABLE2_NAME', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-lim', '--CALC_LIMIT', required=True, default=32400)  #Calculation cutoff, i.e. 32400 = 9:00 AM
    args = parser.parse_args()

    DB_NAME = args.DB_NAME
    TABLE1 = args.TABLE1_NAME
    TABLE2 = args.TABLE2_NAME
    LIMIT = int(args.CALC_LIMIT)

    # Concatenate the sqlite file name to load existing db
    sqlite_file = '{}{}'.format(args.DB_PATH, args.DB_NAME)
    con = sqlite3.connect(sqlite_file)
    cur = con.cursor()

    #Initiate output writer file
    fieldnames = ['origin', 'destination', 'deptime', 'traveltime']
    writer = mod.mkOutput('paths_linked', fieldnames)

    originList, pnrList, deptimeList, destination_list = makeList()
    dest_num = len(destination_list)
    print('Origin List:',originList)
    print('PNR List', pnrList)
    print('Deptime List:', deptimeList)
    print('Destination List:', destination_list)


    for origin in originList:
        for deptime_sec in deptimeList:
            # Initiate prevailing minimum travel time list and matching pnr list
            destTTPrev = np.array([])

            for pnr in pnrList:
                orTT = matchOrigin(origin, deptime_sec, pnr)

                depsum = deptime_sec + orTT

                #If depsum is past the calculation window (9:00 AM), do not link paths. Instead assign maxtime.
                if depsum < LIMIT:
                    #Choose dest_deptime based on closest 15 min bin.
                    depsumBin = linkBins(deptimeList, depsum)
                    transfer = depsumBin - depsum # Int - int = int
                    #Presumably this function returns all destinations with their tt in the set, in the same order.
                    destTTList = matchDestination(depsumBin, pnr)

                    destTTtransfer = destTTList + transfer #Add the transfer time to all destination tt in the array.

                    arraysize = len(destTTPrev)
                    if arraysize > 0: #Check if dest_TT_prev is filled
                        bestTT = [min(pair) for pair in zip(destTTPrev, destTTtransfer)]
                        destTTPrev = bestTT
                    else:
                        destTTPrev = np.array(destTTtransfer)

                #Assign destTTprev to maxtime because this origin cannot reach the given PNR before 9:00.
                else:
                    destTTPrev = np.array([2147483647] * dest_num)


            writeEntry(origin, destination_list, mod.back2Time(deptime_sec), destTTPrev)
