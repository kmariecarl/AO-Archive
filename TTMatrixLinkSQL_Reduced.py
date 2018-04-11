
#################################
#           ATTENTION             #
#################################

#THE PURPOSE FOR THIS SCRIPT IS SOLEY TO REDUCE THE OUTPUT WRITTEN TO DISK TO SEE HOW REMOVING 4 FIELDS AFFECTS FILE SIZE.

# Assuming that TT matrices are loaded into sqlite db.

#Assumption: If an origin reaches a given PNR after the time window (i.e. 9:00) then origin is assigned
#maxtime for that particular origin, deptime, PNR combo.

#Assumption: Depsums are placed in bins according to their rounded up time. I.e. 6:21 depsum goes to the 6:30
#bin. This ensures transit rides do not depart before origin trip arrives.

#Assumption: If a destination for PNR1 and PNR2 are compared and their travel times are equal, the PNR with the lower
#  value will be chosen from destTTtransfer list of tuples-- because the second value in the tuple is next for comparison.

#Assumes that the origin departure times can be matched with departure times from PNR stations up to 1.5 hours after the
#last origin departure time. Thus, the destination TT Matrix must be calculated from 6:00-10:30.

#Notes:
#SQLite db is indexed on deptime_sec.origin for both the o2pnr and pnr2d15 tables

#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import psycopg2

#################################
#           FUNCTIONS           #
#################################

# Query the o2pnr matrix to make lists of unique origins, deptimes, and PNRs
def makeLists():
    #Faster way to select unique list of PNRs, deptimes, Destinations
    cur.execute("SELECT {}.{}.origin FROM {}.{} GROUP BY origin;".format(SCHEMA,TABLE1, SCHEMA, TABLE1))
    origins = cur.fetchall()
    origin_list = list(sum(origins, ()))

    cur.execute("SELECT {}.{}.destination FROM {}.{} GROUP BY destination;".format(SCHEMA, TABLE1, SCHEMA, TABLE1))
    pnrs = cur.fetchall()
    pnr_list = list(sum(pnrs, ()))

    cur.execute("SELECT {}.{}.deptime FROM {}.{} GROUP BY deptime ORDER BY deptime ASC;".format(SCHEMA,TABLE1, SCHEMA,TABLE1))
    or_dep = cur.fetchall()
    or_dep_list = list(sum(or_dep, ()))

    cur.execute("SELECT {}.{}.destination FROM {}.{} GROUP BY destination;".format(SCHEMA,TABLE2, SCHEMA,TABLE2))
    dest = cur.fetchall()
    dest_list = list(sum(dest, ()))

    return origin_list, pnr_list, or_dep_list, dest_list

# def makeBinDict():
#     bindict = {}
#     bindict[1] = 21600 #6:00
#     bindict[2] = 22500 #6:15
#     bindict[3] = 23400 #6:30
#     bindict[4] = 24300
#     bindict[5] = 25200
#     bindict[6] = 26100
#     bindict[7] = 27000
#     bindict[8] = 27900
#     bindict[9] = 28800
#     bindict[10] = 29700
#     bindict[11] = 30600
#     bindict[12] = 31500 #8:45
#     #bindict[13] = 32400
#     return bindict
#     #The bins round down so there is no 9:00 AM bin.
#
# def makeReverseBinDict():
#     revbindict = {}
#     revbindict[21600] = 1 #6:00
#     revbindict[22500] = 2 #6:15
#     revbindict[23400] = 3 #6:30
#     revbindict[24300] = 4
#     revbindict[25200] = 5
#     revbindict[26100] = 6
#     revbindict[27000] = 7
#     revbindict[27900] = 8
#     revbindict[28800] = 9
#     revbindict[29700] = 10
#     revbindict[30600] = 11
#     revbindict[31500] = 12 #8:45
#     #revbindict[32400] = 13
#     return revbindict

def deptime2SecDict():
    dep2sec = {}
    dep2sec['0600'] = 21600
    dep2sec['0615'] = 22500
    dep2sec['0630'] = 23400
    dep2sec['0645'] = 24300
    dep2sec['0700'] = 25200
    dep2sec['0715'] = 26100
    dep2sec['0730'] = 27000
    dep2sec['0745'] = 27900
    dep2sec['0800'] = 28800
    dep2sec['0815'] = 29700
    dep2sec['0830'] = 30600
    dep2sec['0845'] = 31500
    dep2sec['0900'] = 32400

    return dep2sec

def makeDeptimeSecList(deptime_list):
    #The list coming in should already be sorted in ascending order
    deptime_sec_list = [mod.convert2Sec(i) for i in deptime_list]
    return deptime_sec_list


#Query the o2pnr matrix for matching origin, deptime, pnr combo
def matchOrigin(origin, deptime, pnr):
    query = """SELECT traveltime
               FROM {}.{}
               WHERE deptime = %s AND origin = %s AND destination = %s
               ORDER BY deptime, origin;"""
    cur.execute(query.format(SCHEMA,TABLE1), (deptime, origin, pnr))
    tt = cur.fetchall()
    return int(tt[0][0])

#Place depsum values into a 15 minute bin. Ex. 6:05 will get placed in the 6:15 bin.
#Bins are rounded up to allow for transfer time between origin egress to destination ingress
def linkBins(deptime_sec_list_sort, depsum):
    ##Top portion remains commented out until larger matrices are calculated
    # more_time = [900*i for i in range(1, 7)]
    # last_time = deptime_list_sort[-1] #Grab last time in list : 9:00 = 32400
    # extend_deptime_list_sort = list(deptime_list_sort)
    # for k in more_time:
    #     extend_deptime_list_sort.append((last_time+k))
    # print('Extended deptime list:', extend_deptime_list_sort)
    # Assign depsum to the 15 minute bin
    for index, dptm in enumerate(deptime_sec_list_sort): #enumerate(extend_deptime_list_sort):
        next = index + 1
        if next < len(deptime_sec_list_sort): #len(extend_deptime_list_sort):
            if depsum > dptm and depsum <=  deptime_sec_list_sort[next]: #extend_deptime_list_sort[next]:
                # Depsum_bin is in the form of seconds!
                depsum_bin = deptime_sec_list_sort[next] #extend_deptime_list_sort[next]
                return depsum_bin

#Each use of this function returns an array of TT that are in order of their destination. MaxTT are included
#so that the array size always equals 'dest_num'.
def matchDestination(or_tt, transfer, depsum_bin_sec, pnr):
    #Sort traveltimes into list where they are at the same index everytime. The maxTT value is listed for any dest
    #that cannot be reached.

    #Convert seconds back to time which is
    depsum_bin = mod.back2Time(depsum_bin_sec)
    next_bin_sec = depsum_bin_sec + 900
    next_bin = mod.back2Time(next_bin_sec)

    #Because the pnr2d15 matrix is only indexed on deptime and origin and not destination, the resulting array of tt will
    #be stored in memory while the ordering happens. If indexed on the destination, this memory process would not happen.
    query = """SELECT traveltime
               FROM {}.{}
               WHERE deptime >= %s AND deptime < %s AND origin = %s
               ORDER BY DESTINATION"""
    cur.execute(query.format(SCHEMA,TABLE2), (depsum_bin, next_bin, pnr))
    tt = cur.fetchall()  #Listed by ordered destination
    tt_list = list(sum(tt, ()))
    #Produces [(dest_tt, or_tt, transfer, pnr), (dest_tt, or_tt, transfer, pnr)]
    tt_tup_list = [(dest_tt, or_tt, transfer, pnr) for dest_tt in tt_list]

    #Make sure the origin + deptime combo can be matched with destinations
    if len(tt_tup_list) == 0:
        print('No destinations found for this Origin + Deptime combo')
        tt_tup_list = [(2147483647, or_tt, transfer, pnr)] * dest_num

    if len(tt_tup_list) != dest_num:
        print('ERROR!!')

    return tt_tup_list


#This function writes out multiple destination rows for the fixed origin, deptime combo that was the best match for the
#selected pnr.
def writeEntry(origin, deptime, destination_list, dest_TT_prev):
    for dest, tupl in zip(sorted(destination_list), dest_TT_prev):
        # print('For origin {} at deptime {} to pnr {}, the or_tt + transfer time is:'.format(origin, deptime, tt_pnr_tup[1]), )
        # print('To Destination:', dest)
        # print('TT:', tt)
        sumTT = tupl[1] + tupl[0] #Add or_TT to des_tt, dest_TT includes transfer time
        #entry = [origin, deptime (07:15), pnr, destination]
        entry = [origin, deptime, tupl[3], dest]
        writer.writerow(entry)

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  #ENTER AS TTMatrixLink
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS ttmatrices
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #Table 1 in schema, i.e. o2pnr
    parser.add_argument('-table2', '--TABLE2_NAME', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-lim', '--CALC_LIMIT', required=True, default=32400)  #Calculation cutoff, i.e. 32400 = 9:00 AM
    args = parser.parse_args()

    DB_NAME = args.DB_NAME
    SCHEMA = args.SCHEMA_NAME
    TABLE1 = args.TABLE1_NAME
    TABLE2 = args.TABLE2_NAME
    LIMIT = int(args.CALC_LIMIT)

    try:
        con = psycopg2.connect("dbname = '{}' user='aodbadmin' host='localhost' password=''".format(DB_NAME))
        con.set_session(autocommit=True)
    except:
        print('I am unable to connect to the database')

    #Initiate cursor object on db
    cur = con.cursor()

    #Initiate output writer file
    fieldnames = ['origin', 'deptime', 'pnr', 'destination']
    writer = mod.mkOutput('matrices_linked_{}'.format(curtime), fieldnames)

    #Calculate constants
    originList, pnrList, deptimeList, destination_list = makeLists()
    deptimeSecList = makeDeptimeSecList(deptimeList)
    # binDict = makeBinDict()
    # revbinDict = makeReverseBinDict()
    dep2SecDict = deptime2SecDict()
    dest_num = len(destination_list)

    for origin in originList:
        for deptime in deptimeList:
            # Initiate prevailing minimum travel time tuple list to destination set.
            destTTPrev = []

            for pnr in pnrList:
                orTT = matchOrigin(origin, deptime, pnr)
                depsum = dep2SecDict[deptime] + orTT

                #If depsum is past the calculation window (9:00 AM), do not link paths. Instead assign maxtime.
                if depsum < LIMIT:
                    #Choose dest_deptime based on closest 15 min bin.
                    depsumBin = linkBins(deptimeSecList, depsum)
                    transfer = depsumBin - depsum # Int - int = int
                    #Presumably this function returns all destinations with their tt in the set, in the same order.
                    #Values are tuples (TT, pnr).
                    destTTList = matchDestination(orTT, transfer, depsumBin, pnr)
                    #A list comprehension to recreate the list of tuples with [(orTT, transfer, tt_+_transfer, pnr),..(...)]
                    destTTtransfer = [(x[0] + transfer, x[1], x[2], x[3]) for x in destTTList] # + transfer #Add the transfer time to all destination tt in the array.

                    # Check if dest_TT_prev is filled
                    if len(destTTPrev) > 0:
                        #Previous way which causes a hodge-podge of PNRs to be selected--which is what we want.
                        bestTT = [min(pair) for pair in zip(destTTPrev, destTTtransfer)]
                        #bestTT contains a tuple with the minimum dest_TT + transfer and the associated or_TT, transfer (alone), PNR time.
                        destTTPrev = bestTT
                    #On first pass, initialize the destination-TT array and other values.
                    else:
                        destTTPrev = destTTtransfer

                #Assign destTTprev to maxtime because this origin cannot reach the given PNR before 9:00.
                else:
                    print('Origin {} reaches PNR {} after time limit {}'.format(origin, pnr, LIMIT))
                    #Assign arbitrary transfer value because the destination cannot be reached anyway.
                    transfer = 0
                    destTTPrev_max = [(2147483647, orTT, transfer, pnr)] * dest_num

            #Once the inner PNR loop finishes, the destTTPrev list contains the lowest travel times and corresponding PNRs
            #Verify that destination order matches TT array order
            writeEntry(origin, deptime, destination_list, destTTPrev)
            print('Minimum paths for origin: {} at deptime {} have been found.'.format(origin, deptime))
