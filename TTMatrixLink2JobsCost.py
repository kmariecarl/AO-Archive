#This script is an extension of TTMatrixLinkSQL.py by connecting origins to a job array and calculating accessibility.
# Assuming that TT matrices are loaded into PostgreSQL db.

#Assumption: If an origin reaches a given PNR after the time window (i.e. 9:00) then origin is assigned
#maxtime for that particular origin, deptime, PNR combo.

#Assumption: Depsums are placed in bins according to their rounded up time. I.e. 6:21 depsum goes to the 6:30
#bin. This ensures transit rides do not depart before origin trip arrives.

#Assumption: If a destination for PNR1 and PNR2 are compared and their travel times are equal, the PNR with the lower
#  value will be chosen from destTTtransfer list of tuples-- because the second value in the tuple is next for comparison.

#Assumes that the origin departure times can be matched with departure times from PNR stations up to 1.5 hours after the
#last origin departure time. Thus, the destination TT Matrix must be calculated from 6:00-10:30.

#Notes:
#The o2pnr and pnr2d15 matrices should be indexed by origin_deptime

#All costs listed in cents. ie. $4.00 listed as 400


#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import psycopg2
from collections import defaultdict
import time

#################################
#           FUNCTIONS           #
#################################

# Query the o2pnr matrix to make lists of unique origins, deptimes, and PNRs
def makeLists():
    print('Making input lists')
    #Faster way to select unique list of PNRs, deptimes, Destinations
    cur.execute("SELECT {}.{}.origin FROM {}.{} GROUP BY origin;".format(SCHEMA,TABLE1, SCHEMA, TABLE1))
    origins = cur.fetchall()
    origin_list = [x[0] for x in origins]
    print('Origin list created', mod.elapsedTime(start_time))

    cur.execute("SELECT {}.{}.destination FROM {}.{} GROUP BY destination;".format(SCHEMA, TABLE1, SCHEMA, TABLE1))
    pnrs = cur.fetchall()
    pnr_list = [x[0] for x in pnrs]
    print('PNR list created', mod.elapsedTime(start_time))

    cur.execute("SELECT {}.{}.deptime FROM {}.{} GROUP BY deptime ORDER BY deptime ASC;".format(SCHEMA,TABLE1, SCHEMA,TABLE1))
    or_dep = cur.fetchall()
    or_dep_list = [x[0] for x in or_dep]
    print('Deptime list created', mod.elapsedTime(start_time))

    cur.execute("SELECT {}.{}.destination FROM {}.{} GROUP BY destination;".format(SCHEMA,TABLE2, SCHEMA,TABLE2))
    dest = cur.fetchall()
    dest_list = [x[0] for x in dest]
    print('Destination list created', mod.elapsedTime(start_time))

    return origin_list, pnr_list, or_dep_list, dest_list


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
               WHERE origin = %s AND deptime = %s AND destination = %s;"""
    cur.execute(query.format(SCHEMA,TABLE1), (origin, deptime, pnr))
    tt = cur.fetchall()

    return tt[0][0]

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
                return depsum_bin #Seconds, contains times in the 15 minutes less than this depsum_bin

#Each use of this function returns an array of TT that are in order of their destination. MaxTT are included
#so that the array size always equals 'dest_num'.
def matchDestination(or_tt, transfer, depsum_bin_sec, pnr):
    #Sort traveltimes into list where they are at the same index everytime. The maxTT value is listed for any dest
    #that cannot be reached.

    #Convert seconds back to time which is
    print('Before converting seconds and finding bins')
    mod.elapsedTime(start_time)
    depsum_bin = mod.back2Time(depsum_bin_sec)
    #next_bin_sec = depsum_bin_sec + 900
    #next_bin = mod.back2Time(next_bin_sec)
    print('After converting seconds and finding bins')
    mod.elapsedTime(start_time)
    #Because the pnr2d15 matrix is only indexed on deptime and origin and not destination, the resulting array of tt will
    #be stored in memory while the ordering happens. If indexed on the destination, this memory process would not happen.
    query = """SELECT traveltime
               FROM {}.{}
               WHERE deptime = %s AND origin = %s
               ORDER BY DESTINATION"""
    print('Execute query to gather destinations')
    mod.elapsedTime(start_time)
    cur.execute(query.format(SCHEMA,TABLE2), (depsum_bin, pnr))  #next_bin,
    print('Destinations recieved')
    mod.elapsedTime(start_time)
    tt = cur.fetchall()  #Listed by ordered destination
    print('Fetch destinations')
    mod.elapsedTime(start_time)
    # print('This is tt_fetchall', tt)

    #Produces [(dest_tt, or_tt, transfer, pnr), (dest_tt, or_tt, transfer, pnr)]
    tt_tup_list = [(dest_tt[0], or_tt, transfer, pnr) for dest_tt in tt]
    #print('This is tt_tup_list', tt_tup_list)

    #Make sure the origin + deptime combo can be matched with destinations
    if len(tt_tup_list) == 0:
        print('No destinations found for this Origin + Deptime combo')
        tt_tup_list = [(2147483647, or_tt, transfer, pnr)] * dest_num

    if len(tt_tup_list) != dest_num:
        print('Tup list:', len(tt_tup_list), 'Dest num:', print(dest_num))
        print('ERROR!!')

    return tt_tup_list

# def returnPrevailingMinTT(origin, deptime, pnr, destTTPrev):
#     orTT = matchOrigin(origin, deptime, pnr)
#     depsum = dep2SecDict[deptime] + orTT
#
#     # If depsum is past the calculation window (9:00 AM), do not link paths. Instead assign maxtime.
#     if depsum < LIMIT:
#         # Choose dest_deptime based on closest 15 min bin.
#         depsumBin = linkBins(deptimeSecList, depsum)
#         transfer = depsumBin - depsum  # Int - int = int
#         # Presumably this function returns all destinations with their tt in the set, in the same order.
#         # Values are tuples (TT, pnr).
#         destTTList = matchDestination(orTT, transfer, depsumBin, pnr)
#         # A list comprehension to recreate the list of tuples with [(dest_tt_+_transfer, orTT, transfer, pnr),..(...)]
#         destTTtransfer = [(x[0] + transfer, x[1], x[2], x[3]) for x in
#                           destTTList]  # + transfer #Add the transfer time to all destination tt in the array.
#
#         # Check if dest_TT_prev is filled
#         if len(destTTPrev) > 0:
#             # Previous way which causes a hodge-podge of PNRs to be selected--which is what we want.
#             bestTT = [min(pair) for pair in zip(destTTPrev, destTTtransfer)]
#             # bestTT contains a tuple with the minimum dest_TT + transfer and the associated or_TT, transfer (alone), PNR time.
#             destTTPrev = bestTT
#         # On first pass, initialize the destination-TT array and other values.
#         else:
#             destTTPrev = destTTtransfer
#
#     # Assign destTTprev to maxtime because this origin cannot reach the given PNR before 9:00.
#     else:
#         print('Origin {} reaches PNR {} after time limit {}'.format(origin, pnr, LIMIT))
#         # Assign arbitrary transfer value because the destination cannot be reached anyway.
#         transfer = 0
#         destTTPrev = [(2147483647, orTT, transfer, pnr)] * dest_num
#
#     return destTTPrev


#Place destination into cost thresholds if both the origin can reach a PNR and from the PNR to a destination can be reached.
def moneyBuckets(origin, deptime, dest, tup):
    # Process for monetary thresholds
    #Assuming that the or_deptime combo can reach at least one PNR within 30 minutes
    a_cost = returnAutoCost(origin, deptime, tup[3])
    print('Origin {} at deptime {} to PNR {} costs {}'.format(origin, deptime, tup[3], a_cost))
    if a_cost is not None:
        t_cost = returnTransitCost(tup[0])
        print('PNR {} to destination {} using transit costs {}'.format(tup[3], dest, t_cost))
        if t_cost is not None:
            filterCost(dest, a_cost, t_cost)# Put destination into each of the monetary bins $2, $4, etc.
        else:
            pass
    else:
        pass

#This function matches auto path cost info from the DB to the selected or_dep_pnr combo. The cost already has value of
#time factored in, along with fuel price, driving environment, etc.
def returnAutoCost(origin, deptime, pnr):
    query = """SELECT {} 
               FROM {}.{} 
               WHERE CAST(origin as BIGINT) = %s 
               AND CAST(deptime AS INT) = %s 
               AND CAST(destination AS INT) = %s;"""
    cur.execute(query.format(SCENARIO, SCHEMA, COSTS), (str(origin), deptime, pnr))
    a = cur.fetchone()
    #This is should only occur in test situation where the links I have path_costs for only start at 7:00 am.
    if a is None:
        pass
        # a_cost = 200000 #No information at the deptime

    else:
        a_cost = a[0] #This should return a single value

        return a_cost

#This function returns the travel cost based on the constant fare and a VOT calculation done internally.
#The time includes the transfer time.
def returnTransitCost(destTT_transfer):
    if destTT_transfer != 2147483647:
        #If the optional VOT value is provided, the user would like to calculate transit costs including VOT.
        if VOT:
            # VOT for business travel is considered 100% of wage and for the US on average that value is $27.07 /person-hour, for MN: $18.03
            person_hours = round((destTT_transfer / 3600), 4)
            fare = 325  # $3.25 rush hour
            t_cost = (person_hours * VOT) + fare  # Returns a value in cents
            return t_cost
        else:
            fare = 325
            t_cost = fare
            return t_cost

    else:
        pass

#This function places the PNR scenario path cost into appropriate monetary cost bins.
def filterCost(dest, a_cost, t_cost):
    total = a_cost + t_cost

    applicable_cost_list = []
    non_applicable_cost_list = []
    for item in THRESHOLD_COST_LIST:
        if total < item:
            applicable_cost_list.append(item)
        else:
            non_applicable_cost_list.append(item)
    for cost in sorted(applicable_cost_list):
        COST_DICT[cost].append(dest)
    for empty_cost in sorted(non_applicable_cost_list):
        COST_DICT[empty_cost].append(None)

def timeBuckets(dest, tup):
    # Process for travel time thresholds
    sumTT = tup[0]  # Add or_TT to des_tt, dest_TT includes transfer time
    # Initiate dictionary specific to this origin and deptime combo
    filterTT(dest, sumTT)

#Place the current row's destination into threshold bins based on its travel time
#If destination cannot be reached, then do not include in output.
def filterTT(destination, tt):
    #Create a list of thresholds that this destination can be reached in <= time
    applicable_thresh_list = []
    for item in THRESHOLD_LIST:
        #OTP puts TT in minutes then rounds down, the int() func. always rounds down.
        #Think of TT in terms of 1 min. bins. Ex. A dest with TT=30.9 minutes should not be placed in the 30 min TT list.
        minbin = int(tt)/60
        minthresh = item/60
        if minbin < minthresh:
            #print('minbin:', minbin, 'minthresh', minthresh)
            applicable_thresh_list.append(item)

    #Once the applicable thresholds have been found, place destination into lists
    for thresh in sorted(applicable_thresh_list):
        THRESH_DICT[thresh].append(destination)


#This function evaluates the threshold dictionary provided and returns the jobs that match with the given destination list
#then write to the provided writer file.
def writeAccessFile(origin, deptime, threshold_dict, threshold_type, writer):
    for thresh, dest_list in sorted(threshold_dict.items()):

        # Destination , list actually needs to be a tuple to work with the WHERE IN query below
        dest_tup = tuple(dest_list)

        # Query DB for jobs that match destinations in the dest_list:
        query = """SELECT c000 
                   FROM {}.{} 
                   WHERE GEOID10 IN %s;"""
        cur.execute(query.format(SCHEMA, JOBS), (dest_tup,))
        jobs = cur.fetchall()

        jobs_list = [x[0] for x in jobs]

        access = sum(jobs_list)

        entry = {'label': origin, 'deptime': deptime, '{}'.format(threshold_type): thresh, 'jobs': access}
        writer.writerow(entry)


# #This function writes out multiple destination rows for the fixed origin, deptime combo that was the best match for the
# #selected pnr.
# def writeEntry(origin, deptime, destination_list, dest_TT_prev):
#     for dest, tupl in zip(sorted(destination_list), dest_TT_prev):
#         sumTT = tupl[1] + tupl[0] #Add or_TT to des_tt, dest_TT includes transfer time
#         #entry = [origin, deptime (07:15), or_TT, transfer, pnr, destination, dest_TT, totalTT]
#         entry = [origin, deptime, tupl[1], tupl[2], tupl[3], dest, tupl[0] - tupl[2], sumTT]
#         writer.writerow(entry)

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  #ENTER AS TTMatrixLink
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS ttmatrices
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #Table 1 in schema, i.e. o2pnr
    parser.add_argument('-table2', '--TABLE2_NAME', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-jobstab', '--JOBS_TABLE_NAME', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-or', '--ORIGIN_LIST', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-pnr', '--PNR_LIST', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-dep', '--DEPTIME_LIST', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-dest', '--DESTINATION_LIST', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-lim', '--CALC_LIMIT', required=True, default=32400)  #Calculation cutoff, i.e. 32400 = 9:00 AM
    parser.add_argument('-cost', '--PATH_COST_TABLE_NAME', required=False, default=None)  #Table auto_path_cost
    parser.add_argument('-scen', '--SCENARIO', required=False, default=None)  #Cost scenario to calc access from, i.e. fuel_cost, A, irs_cost
    #Optionally include the value of time on transit portion of the trip
    parser.add_argument('-vot', '--VALUE_OF_TIME', required=False, default=None)  #Value of time in cents, i.e. 2707 based on USDOT

    args = parser.parse_args()

    DB_NAME = args.DB_NAME
    SCHEMA = args.SCHEMA_NAME
    TABLE1 = args.TABLE1_NAME
    TABLE2 = args.TABLE2_NAME
    JOBS = args.JOBS_TABLE_NAME
    # COSTS = args.PATH_COST_TABLE_NAME
    # SCENARIO = args.SCENARIO
    # VOT = int(args.VALUE_OF_TIME)
    LIMIT = int(args.CALC_LIMIT)

    try:
        con = psycopg2.connect("dbname = '{}' user='aodbadmin' host='localhost' password=''".format(DB_NAME))
        con.set_session(autocommit=True)
    except:
        print('I am unable to connect to the database')

    #Initiate cursor object on db
    cur = con.cursor()


    #Initiate a second writer for outputting the final paths at each deptime to be used with the linkCostCalc and Agg procedure.
    fieldnames2 = ['label', 'deptime', 'cost', 'jobs']
    writer_cost = mod.mkDictOutput('Linked_wCostAccess_{}'.format(curtime), fieldname_list=fieldnames2)

    #Initiate output writer file
    fieldnames = ['label', 'deptime', 'threshold', 'jobs']
    writer_time = mod.mkDictOutput('Linked_wTTAccess_{}'.format(curtime), fieldname_list=fieldnames)

    #Calculate constants
    #originList, pnrList, deptimeList, destination_list = makeLists()
    originList = mod.readList(args.ORIGIN_LIST)
    pnrList = mod.readList(args.PNR_LIST)
    deptimeList = mod.readList(args.DEPTIME_LIST)
    destination_list = mod.readList(args.DESTINATION_LIST)

    deptimeSecList = makeDeptimeSecList(deptimeList)
    dep2SecDict = deptime2SecDict()
    dest_num = len(destination_list)
    #Make threshold list to check travel times against.
    THRESHOLD_LIST = [300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000, 3300, 3600, 3900, 4200,
                      4500, 4800, 5100, 5400]
    THRESHOLD_COST_LIST = [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400]

    for origin in originList:

        for deptime in deptimeList:
            # Initiate prevailing minimum travel time tuple list to destination set.
            destTTPrev = []
            #Iteratively update destTTPrev with minimum travel times.
            for pnr in pnrList:

                orTT = matchOrigin(origin, deptime, pnr)

                #Handle origins that cannot reach the provided PNR
                if orTT is not None:
                    depsum = dep2SecDict[deptime] + int(orTT)

                    # If depsum is past the calculation window (9:00 AM), do not link paths. Instead assign maxtime.
                    if depsum < LIMIT:
                        #print('depsum less than limit')
                        # Choose dest_deptime based on closest 15 min bin.
                        depsumBin = linkBins(deptimeSecList, depsum)

                        transfer = depsumBin - depsum  # Int - int = int

                        # Presumably this function returns all destinations with their tt in the set, in the same order.
                        # Values are tuples (TT, pnr).
                        print('Before destinations are matched')
                        mod.elapsedTime(start_time)
                        destTTList = matchDestination(int(orTT), transfer, depsumBin, pnr)
                        print('After destinations are matched')
                        mod.elapsedTime(start_time)
                        #destTTList = [destTT, orTT, Transfer, PNR]
                        # A list comprehension to recreate the list of tuples with [(dest_tt_+_transfer_+_orTT, orTT, transfer, pnr),..(...)]
                        totalTT = [(x[0] + x[2] + x[1], x[1], x[2], x[3]) for x in
                                          destTTList ]  #Add the transfer time and origin tt to all destination tt in the array.
                        print('List comprehension to calc totalTT')
                        mod.elapsedTime(start_time)
                        # Check if dest_TT_prev is filled
                        if len(destTTPrev) > 0:
                            #print('Destination list greater than 0')
                            # Previous way which causes a hodge-podge of PNRs to be selected--which is what we want.
                            #Here a pair is actually the entire tuple so that the orTT, pnr, transfer associated with the minimum
                            #sumTT are maintained, only the first value in the tuple (destTT + transfer + orTT) are actually compared for their
                            #min value.

                            bestTT = [min(pair) for pair in zip(destTTPrev, totalTT)]
                            print('Found best TT array')
                            mod.elapsedTime(start_time)

                            # bestTT contains a tuple with the minimum dest_TT + transfer + orTT and the associated or_TT, transfer (alone), PNR time.
                            destTTPrev = bestTT
                        # On first pass, initialize the destination-TT array and other values.
                        else:
                            #print('Initialize destination tt list')
                            destTTPrev = totalTT

                    # Assign destTTprev to maxtime because this origin cannot reach the given PNR before 9:00.
                    else:
                        #print('Origin {} reaches PNR {} after time limit {}'.format(origin, pnr, LIMIT))
                        # Assign arbitrary transfer value because the destination cannot be reached anyway.
                        transfer = 0
                        destTTPrev = [(2147483647, int(orTT), transfer, pnr)] * dest_num
                else:
                    #print('Origin {} cannot reach PNR {} within 30 minutes'.format(origin, pnr))
                    # Assign arbitrary transfer value because the destination cannot be reached anyway.
                    transfer = 0
                    orTT = 2147483647
                    destTTPrev = [(2147483647, orTT, transfer, pnr)] * dest_num

                print('PNR {} Finished'.format(pnr))
                mod.elapsedTime(start_time)


            #Before you write an entry, calculate the total TT, then access by connecting jobs to destinations
            #Make one query per threshold:
            # Create a new dict structure for the origin_deptime combo
            THRESH_DICT = defaultdict(list)
            COST_DICT = defaultdict(list)
            #Iterate through the destinations and their respective TTs.

            for dest, tup in zip(sorted(destination_list), destTTPrev):
                #moneyBuckets(origin, deptime, dest, tup)
                #timeBuckets(dest, tup)
                #print('destination and tt tuple:', dest, tup[0], tup[1], tup[2], tup[3])
                if tup[0] < 2147483647:
                    filterTT(dest, tup[0])

            #This ensures that each origin has all thresholds listed even if no jobs can be reached in the given time frame
            for item_again in THRESHOLD_LIST:
                if item_again not in THRESH_DICT:
                    THRESH_DICT[item_again].append(0)

            print(THRESH_DICT)

            #writeAccessFile(origin, deptime, COST_DICT, 'cost', writer_cost)
            writeAccessFile(origin, deptime, THRESH_DICT, 'threshold', writer_time)



            print("Origin {} matched to jobs at deptime {}".format(origin, deptime))
            mod.elapsedTime(start_time)
        print('Origin {} finished'.format(origin))
        mod.elapsedTime(start_time)
    mod.elapsedTime(start_time)
