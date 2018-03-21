#Assuming that TT matrices are loaded into sqlite db.

#Assumption: If an origin reaches a given PNR after the time window (i.e. 9:00) then origin is assigned
#maxtime for that particular origin, deptime, PNR combo.

#Assumption: Depsums are placed in bins according to their rounded up time. I.e. 6:21 depsum goes to the 6:30
#bin. This ensures transit rides do not depart before origin trip arrives.

#Assumes that the origin departure times can be matched with departure times from PNR stations up to 1.5 hours after the
#last origin departure time. Thus, the destination TT Matrix must be calculated from 6:00-10:30.

#Notes:
#SQLite db is indexed on deptime_sec.origin for both the o2pnr and pnr2d15 tables

import sqlite3
from myToolsPackage import matrixLinkModule as mod
import numpy as np
import argparse

#################################
#           FUNCTIONS           #
#################################

# Query the o2pnr matrix to make lists of unique origins, deptimes, and PNRs
def makeList():

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
    query = """SELECT traveltime
               FROM {}
               WHERE deptime_sec = ? AND origin = ? AND destination = ?
               ORDER BY deptime_sec, origin;"""
    cur.execute(query.format(TABLE1), (deptime_sec, origin, pnr))
    tt = cur.fetchall()
    return int(tt[0][0])

#Place depsum values into a 15 minute bin. Ex. 6:05 will get placed in the 6:15 bin.
#Bins are rounded up to allow for transfer time between origin egress to destination ingress
def linkBins(deptime_list_sort, depsum):
    more_time = [900*i for i in range(1, 7)]
    last_time = deptime_list_sort[-1] #Grab last time in list : 9:00 = 32400
    extend_deptime_list_sort = list(deptime_list_sort)
    for k in more_time:
        extend_deptime_list_sort.append((last_time+k))
    print('Extended deptime list:', extend_deptime_list_sort)
    # Assign depsum to the 15 minute bin
    for index, dptm in enumerate(extend_deptime_list_sort):
        next = index + 1
        if next < len(extend_deptime_list_sort):
            if depsum > dptm and depsum <= extend_deptime_list_sort[next]:
                # Depsum_bin is in the form of seconds!
                depsum_bin = extend_deptime_list_sort[next]
                return depsum_bin

#Each use of this function returns an array of TT that are in order of their destination. MaxTT are included
#so that the array size always equals 'dest_num'.
def matchDestination(depsum_bin, pnr):
    #Sort traveltimes into list where they are at the same index everytime. The maxTT value is listed for any dest
    #that cannot be reached.
    next_bin = depsum_bin + 900

    #Because the pnr2d15 matrix is only indexed on deptime and origin and not destination, the resulting array of tt will
    #be stored in memory while the ordering happens. If indexed on the destination, this memory process would not happen.
    query = """SELECT traveltime
               FROM {}
               WHERE deptime_sec >= ? AND deptime_sec < ? AND origin = ?
               ORDER BY DESTINATION"""
    cur.execute(query.format(TABLE2), (depsum_bin, next_bin, pnr))
    tt = cur.fetchall()  #Listed by ordered destination
    tt_list = np.array(list(sum(tt, ())))

    #Select the list of jobs that lines up with the travel time and destination information.
    query = """SELECT myjobs
               FROM {}
               WHERE deptime_sec >= ? AND deptime_sec < ? AND origin = ?
               ORDER BY DESTINATION"""
    cur.execute(query.format(TABLE2), (depsum_bin, next_bin, pnr))
    jobs = cur.fetchall()  # Listed by ordered destination
    jobs_list = np.array(list(sum(jobs, ())))

    #Make sure the origin + deptime combo can be matched with destinations
    if len(tt_list) == 0:
        print('No destinations found for this Origin + Deptime combo')
        tt_list = np.array([2147483647] * dest_num)
        jobs_list = np.array([0] * dest_num)

    if len(tt_list) != dest_num:
        print('ERROR!!')
    #Delete once the output has been verified.
    for quick_tt, quick_jobs in zip(tt_list, jobs_list):
        print('TT: {} matched to Jobs: {}'.format(quick_tt, quick_jobs))

    return tt_list, jobs_list

#This function writes out multiple destination rows for the fixed origin, deptime combo that was the best match for the
#selected pnr.
def writeEntry(origin, destination_list, pnr_best, deptime_sec, orTT_best, dest_TT_prev):
    for dest, tt in zip(sorted(destination_list), dest_TT_prev):
        print('For origin {} at deptime {} to pnr {}, the tt + transfer time is:'.format(origin,deptime_sec, pnr_best), orTT_best)
        print('To Destination:', dest)
        print('TT:', tt)
        sumTT = orTT_best + tt #tt includes transfer time
        print('SumTT:', sumTT)
        entry = [origin, dest, pnr_best, mod.back2Time(deptime_sec), sumTT]
        writer.writerow(entry)

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()

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
    fieldnames = ['origin', 'destination', 'pnr', 'deptime', 'traveltime']
    writer = mod.mkOutput('paths_linked_{}'.format(curtime), fieldnames)

    originList, pnrList, deptimeList, destination_list = makeList()
    dest_num = len(destination_list)

    for origin in originList:
        for deptime_sec in deptimeList:
            # Initiate prevailing minimum travel time list to destination set and weighted average.
            destTTPrev = np.array([])
            weightedAvgPrev = 2147483647

            for pnr in pnrList:
                orTT = matchOrigin(origin, deptime_sec, pnr)
                depsum = deptime_sec + orTT

                #If depsum is past the calculation window (9:00 AM), do not link paths. Instead assign maxtime.
                if depsum < LIMIT:
                    #Choose dest_deptime based on closest 15 min bin.
                    depsumBin = linkBins(deptimeList, depsum)
                    transfer = depsumBin - depsum # Int - int = int
                    #Presumably this function returns all destinations with their tt in the set, in the same order.
                    destTTList, jobsList = matchDestination(depsumBin, pnr)

                    destTTtransfer = destTTList + transfer #Add the transfer time to all destination tt in the array.

                    # Check if dest_TT_prev is filled
                    # Begin weighting process
                    if len(destTTPrev) > 0:
                        #Take reciprocal of TT from table 3
                        destTTtransfer_recip = 1./destTTtransfer
                        #Multiply reciprocal TT array by jobs to get weighted values of TT
                        weightedTT = destTTtransfer_recip*jobsList
                        #Find weighted average TT.
                        weightedAvg = np.average(weightedTT)
                        #The PNR that passes this inequality is the best PNR for the given Origin + Deptime combo
                        if weightedAvg < weightedAvgPrev:
                            weightedAvgPrev = weightedAvg
                            #Previous way which causes a hodge-podge of PNRs to be selected.
                            #bestTT = [min(pair) for pair in zip(destTTPrev, destTTtransfer)]
                            destTTPrev = destTTtransfer
                            #Set the final selected PNR and tt from origin to that PNR
                            orTT_best = orTT
                            pnr_best = pnr
                    #One first pass, initialize the destination-TT array and other values.
                    else:
                        destTTPrev = np.array(destTTtransfer)
                        # Take reciprocal of TT from table 3
                        destTTtransfer_recip = 1/destTTtransfer
                        # Multiply reciprocal TT array by jobs to get weighted values of TT
                        weightedTT = destTTtransfer_recip * jobsList
                        weightedAvg = np.average(weightedTT)
                        weightedAvgPrev = weightedAvg
                        #If the first PNR array is the best, then these values will be sent to write to file.
                        orTT_best = orTT
                        pnr_best = pnr

                #Assign destTTprev to maxtime because this origin cannot reach the given PNR before 9:00.
                else:
                    print('Origin {} reaches PNR {} after time limit {}'.format(origin, pnr, LIMIT))
                    destTTPrev = np.array([2147483647] * dest_num)

            #Once the inner PNR loop finishes, the destTTPrev list contains the lowest weighted travel times
            #Verify that destination order matches TT array order
            writeEntry(origin, destination_list, pnr_best, deptime_sec, orTT_best, destTTPrev)
