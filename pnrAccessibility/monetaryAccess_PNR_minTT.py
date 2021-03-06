# This script is a new version of monetaryAccess_PNR.py that uses the pnr2dmin TT matrix as opposed to the old pnr2d15 matrix.

# This script reads in TAZ2PNR and PNR2DMin tables from PostGres DB and connects all viable paths then calculates the
# time-based and time+cost based accessibility

# example usage: kristincarlson~ python ~/Dropbox/Bus-Highway/Programs/gitPrograms/monetaryAccess_PNR_minTT.py -db TTMatrixLink
# -schema ttmatrices -table1 o2pnr -table2 pnr2dmin -jobstab jobs -costtab t2pnr_auto_cost -lim 31500 -scen d -fare 325 -vot 0

# Cost scenarios to choose from:
# suma = sumfuel + sumrep + sumdep + FIXED
# sumb = sumfuel + sumrep + sumdep + FIXED + sumvot
# sumc = sumirs
# sumd = sumfuel + sumrep + sumdep
# sume = sumfuel + sumrep + sumdep + sumvot

# Notes:
# All costs listed in cents. ie. $4.00 listed as 400
# I have tried many times to incorporate a PNR counter but numpy arrays do not support the operations I have tried
# The transfer penalty is a min of 0 minutes and max of 15 minutes. Results from the depsumBin operation and aggregating
# transit tt's to lower bin, max spread is 15 units



#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import psycopg2
from collections import defaultdict, OrderedDict
import time
import numpy as np
from myToolsPackage.progress import bar

#################################
#           FUNCTIONS           #
#################################

# Query the o2pnr matrix to make lists of unique origins, deptimes, and PNRs
# Assumes that the deptimes in origin set are the same as destination set when that
# may not always be the case
def makeLists():
    print('Making input lists')
    #Faster way to select unique list of PNRs, deptimes, Destinations
    cur.execute("SELECT {}.{}.origin FROM {}.{} GROUP BY origin ORDER BY origin ASC;".format(SCHEMA,TABLE1, SCHEMA, TABLE1))
    origins = cur.fetchall()
    origin_list = [x[0] for x in origins]
    print('Number of origins:', len(origin_list))
    print('Origin list created', time.time() - t0)

    cur.execute("SELECT {}.{}.destination FROM {}.{} GROUP BY destination ORDER BY destination ASC;".format(SCHEMA, TABLE1, SCHEMA, TABLE1))
    pnrs = cur.fetchall()
    pnr_list = [x[0] for x in pnrs]
    print('Number of PNRs:', len(pnr_list))
    print('PNR list created', time.time() - t0)

    cur.execute("SELECT {}.{}.deptime_sec FROM {}.{} GROUP BY deptime_sec ORDER BY deptime_sec;".format(SCHEMA,TABLE2, SCHEMA,TABLE2))
    or_dep = cur.fetchall()
    or_dep_list = [x[0] for x in or_dep]
    print('Deptime list created', time.time() - t0)
    print('Deptime List:', or_dep_list)

    cur.execute("SELECT {}.{}.destination FROM {}.{} GROUP BY destination ORDER BY destination ASC;".format(SCHEMA,TABLE2, SCHEMA,TABLE2))
    dest = cur.fetchall()
    dest_list = [x[0] for x in dest]
    print('Number of destionations:', len(dest_list))
    print('Destination list created', time.time() - t0)

    return origin_list, pnr_list, or_dep_list, dest_list

# This function relies on the db structure of the tt matrices already imported into postgresql.
# Once the tt array has been extracted, convert to numpy array.
def createPNR2D15(pnr_list, deptime_list):
    print('Building pnr2d15 dictionary in memory...')
    pnr2d15 = defaultdict(lambda: defaultdict(list))
    for pnr in pnr_list:
        for deptime in deptime_list:
            query = """SELECT traveltime
                           FROM {}.{}
                           WHERE origin = %s AND deptime_sec = %s
                           ORDER BY destination ASC"""

            cur.execute(query.format(SCHEMA, TABLE2), (pnr, deptime))  # next_bin,

            tt = cur.fetchall()  # Listed by ordered destination

            # Create a list of lists [tt, cost] then transform to numpy array
            # Initialize cost as an empty value
            # Now that Maxtimes are not included, these array's will be of variable length, need to add filler for
            # destinations without a time value.
            tt_list = [[dest_tt[0], ] for dest_tt in tt]
            if len(tt_list) < DEST_NUM:
                remainder = DEST_NUM - len(tt_list)
                for i in range(remainder):
                    tt_list.append([2147483647, ])

            tt_array = np.array(tt_list)

            pnr2d15[pnr][deptime] = tt_array

        print('PNR {} has been added to PNR2d15 dictionary'.format(pnr))
        mod.elapsedTime(start_time)
    return pnr2d15


def createJobsDict():
    print('Building Jobs Dict', time.time() - t0)
    jobs_dict = {}
    query = """SELECT geoid10, c000 
               FROM {}.{};"""
    cur.execute(query.format(SCHEMA, JOBS))
    jobs = cur.fetchall()
    for tup in jobs:
        jobs_dict[tup[0]] = tup[1]

    print('Jobs Dict Now in Memory', time.time() - t0)
    return jobs_dict

#Query the t2pnr matrix for matching origin, deptime_sec, pnr combo.
#Use when multiple departure times have been calculated for o2pnr matrix
def matchOriginAndTime(origin, deptime, pnr, scenario, null_counter):

    query = """SELECT traveltime
               FROM {}.{}
               WHERE origin = %s AND deptime_sec = %s AND destination = %s;"""
    try:
        cur.execute(query.format(SCHEMA,TABLE1), (origin, deptime, pnr))
        tt = cur.fetchall()
        or_tt = int(tt[0][0])

    except:
        print('Origin {} cannot reach PNR {} within 3.5 hours'.format(origin, pnr))
        or_tt = None

    query2 = """SELECT {}
               FROM {}.{}
               WHERE origin = %s AND deptime_sec = %s AND destination = %s;"""

    #In cases where the origin to PNR path exceeded 3.5 hours, a cost may not have been calculated for the OD pair.
    try:
        cur.execute(query2.format(scenario, SCHEMA, COSTS), (origin, deptime, pnr))
        c = cur.fetchall()
        cost = round(float(c[0][0]), 2)

    except:
        null_counter += 1
        cost = 10000  #=$100.00

    return or_tt, cost, null_counter

#Query the o2pnr matrix for matching origin and pnr combo.
#Version removes reliance on o2pnr deptime due to single departure time (7:00 AM) calculated for auto trip.
def matchOrigin(origin, deptime, pnr, scenario, null_counter):

    query = """SELECT traveltime
               FROM {}.{}
               WHERE origin = %s AND destination = %s;"""
    try:
        cur.execute(query.format(SCHEMA,TABLE1), (origin, pnr))
        tt = cur.fetchall()
        or_tt = int(tt[0][0])

    except:
        print('Origin {} cannot reach PNR {} within 3.5 hours'.format(origin, pnr))
        or_tt = None

    query2 = """SELECT {}
               FROM {}.{}
               WHERE origin = %s AND destination = %s;"""

    #In cases where the origin to PNR path exceeded 3.5 hours, a cost may not have been calculated for the OD pair.
    try:
        cur.execute(query2.format(scenario, SCHEMA, COSTS), (origin, pnr))
        c = cur.fetchall()
        cost = round(float(c[0][0]), 2)

    except:
        null_counter += 1
        cost = 10000000  #=$10,000.00

    #or_tt_cost_tup = np.array([or_tt, cost])
    return or_tt, cost, null_counter

#Place depsum values into a 15 minute bin. Ex. 6:05 will get placed in the 6:15 bin.
#Bins are rounded up (revised to round down 9/7/18) to allow for transfer time between origin egress to destination ingress
def linkBins(depsum, deptime_list):
    # Assign depsum to the 15 minute bin
    for index, dptm in enumerate(deptime_list):
        next = index + 1
        if next < len(deptime_list):

            if depsum >= dptm and depsum <  deptime_list[next]:
                # Depsum_bin is in the form of seconds!
                depsum_bin = deptime_list[next]
                return depsum_bin #Seconds, contains times in the 15 minutes less than this depsum_bin


#Place the current row's destination into threshold bins based on its travel time
#If destination cannot be reached, then do not include in output.
#Output is an ordered dictionary mapping threshold to list of GEOIDs that can be reached
def calcTimeAccess(origin, deptime, destination_list, destTTPrev, writer_time):
    thresh_dict = OrderedDict()

    for x in THRESHOLD_LIST_MINUTE:
        thresh_dict[x] = []

    # Iterate through the destinations and their respective TTs.
    for dest, totalTT_tup in zip(destination_list, destTTPrev):
        # Only assign jobs where destination can be reached
        #totalTT_tup[0] is travel time, while tup[1] is auto cost, tup[2] is PNR
        if totalTT_tup[0] < 2147483647:
            # OTP puts TT in minutes then rounds down, the int() func. always rounds down.
            # Think of TT in terms of 1 min. bins. Ex. A dest with TT=30.9 minutes should not be placed in the 30 min TT list.
            minbin = totalTT_tup[0] / 60

            for thresh in THRESHOLD_LIST_MINUTE:
                # Place destination into only the first threshold that allows that destination to be reached
                if minbin <= thresh:
                    thresh_dict[thresh].append(dest)
                    break

    writeAccessFile(origin, deptime, thresh_dict, 'threshold', writer_time)

def calcMonetaryAccess(origin, deptime, destination_list, destTTPrev, writer_cost):
    #print('time8', time.time() - t0)
    cost_dict = OrderedDict()

    for y in THRESHOLD_COST_LIST:
        cost_dict[y] = []

    # Iterate through the destinations and their respective TTs.
    for dest, totalTT_tup in zip(destination_list, destTTPrev):
        # Only assign jobs where destination can be reached
        #totalTT_tup[0] is travel time, while tup[1] is auto cost, tup[2] is PNR

        if totalTT_tup[0] < 2147483647:

            time_cost = totalTT_tup[0] * VOT/3600 #If VOT = 0, then VOT is not added to total cost
            a_t_cost = totalTT_tup[1] + int(FARE) + time_cost

            for cost in THRESHOLD_COST_LIST:
                # Place destination into only the first cost threshold that allows that destination to be reached
                if a_t_cost <= cost:

                    cost_dict[cost].append(dest)
                    break

    writeAccessFile(origin, deptime, cost_dict, 'cost', writer_cost)

#This function evaluates the threshold dictionary provided and returns the jobs that match with the given destination list
#then write to the provided writer file.
def writeAccessFile(origin, deptime, threshold_dict, threshold_type, writer):
    #For each threshold, calculate accessibility
    access_prev = 0
    #Each destination list only contains the new destinations that can be reached at each successive threshold.
    for thresh, dest_list in threshold_dict.items():

        thresh_level_access = 0

        #Write the threshold out in different ways depending on time or cost
        if threshold_type == 'threshold':
            value = thresh * 60
        else:
            value = thresh

        if len(dest_list) > 0:

            for geoid in dest_list:
                thresh_level_access += JOBS[geoid]


            access = thresh_level_access + access_prev
            entry = {'label': origin, 'deptime': mod.back2Time(deptime), '{}'.format(threshold_type): value, 'jobs': access}
            writer.writerow(entry)
            access_prev = access
        else:
            access = access_prev
            entry = {'label': origin, 'deptime': mod.back2Time(deptime), '{}'.format(threshold_type): value , 'jobs': access }
            writer.writerow(entry)


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    t0 = time.time()
    print(readable)
    bar = bar.Bar(message='Processing', fill='@', suffix='%(percent)d%%', max=3030)

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  #ENTER AS TTMatrixLink
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS ttmatrices
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #Table 1 in schema, i.e. o2pnr
    parser.add_argument('-table2', '--TABLE2_NAME', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-jobstab', '--JOBS_TABLE_NAME', required=True, default=None)  #Table 2 in schema, i.e. jobs
    parser.add_argument('-costtab', '--PATH_COST_TABLE_NAME', required=True, default=None)  #Path cost table in schema, i.e. t2pnr_auto_cost
    parser.add_argument('-lim', '--CALC_LIMIT', required=True, default=32400)  #Calculation cutoff, i.e. 8:45 = 31500, 32400 = 9:00 AM
    parser.add_argument('-scen', '--SCENARIO', required=True, default=None)  #Cost scenario to calc access from, i.e. A, B, C
    parser.add_argument('-fare', '--FARE', required=True, default=325)  #Rush fare is $3.25 otherwise put $0.00 for other scenarios
    parser.add_argument('-vot', '--VALUE_OF_TIME', required=True, default=1803)  #Value of time in cents, i.e. $18.03 = 1803 based on USDOT
    parser.add_argument('-or', '--ORIGIN_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-pnr', '--PNR_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-dep', '--DEPTIME_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-dest', '--DESTINATION_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15




    args = parser.parse_args()

    DB_NAME = args.DB_NAME
    SCHEMA = args.SCHEMA_NAME
    TABLE1 = args.TABLE1_NAME
    TABLE2 = args.TABLE2_NAME
    JOBS = args.JOBS_TABLE_NAME
    COSTS = args.PATH_COST_TABLE_NAME
    LIMIT = int(args.CALC_LIMIT)
    SCENARIO = args.SCENARIO
    FARE = args.FARE
    if args.VALUE_OF_TIME:
        VOT = int(args.VALUE_OF_TIME)
    #Removed Transfer on 7/13/18. The true theoretical transfer min is 0 and max is 15 minutes due to rounding down the
    #depsumBin and aggregating the min travel time to the lower 15 min bin during the createMinTT.py process, written 9/7
    #TRANSFER = 300


    try:
        con = psycopg2.connect("dbname = '{}' user='aodbadmin' host='localhost' password='jr2Iv5AnQlxCi3L'".format(DB_NAME)) #replace host='localhost' with ip address of instance when running externally
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
    originList, pnrList, deptimeList, destination_list = makeLists()

    # originList = mod.readList(args.ORIGIN_LIST, str)
    # pnrList = mod.readList(args.PNR_LIST, str)
    # deptimeList = mod.readList(args.DEPTIME_LIST, int)
    # destination_list = mod.readList(args.DESTINATION_LIST, str)



    DEST_NUM = len(destination_list)
    print('Dest Num=', DEST_NUM)
    nullCounter = 0
    #Make threshold list to check travel times against.
    THRESHOLD_LIST = [300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000, 3300, 3600, 3900, 4200,
                      4500, 4800, 5100, 5400]

    THRESHOLD_LIST_MINUTE = [int(x/60) for x in THRESHOLD_LIST]

    #Use when not dealing with VOT
    THRESHOLD_COST_LIST = [200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000,
                           1050, 1100, 1150, 1200, 1250, 1300, 1350, 1400, 1450, 1500, 1550, 1600, 1650, 1700, 1750,
                           1800, 1850, 1900, 1950, 2000, 2050, 2100, 2150, 2200, 2250, 2300, 2350, 2400, 2450, 2500,
                           2550, 2600, 2650, 2700, 2750, 2800, 2850, 2900, 2950, 3000]
    #Use with VOT scenarios
    # THRESHOLD_COST_LIST = [200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000,
    #                        1050, 1100, 1150, 1200, 1250, 1300, 1350, 1400, 1450, 1500, 1550, 1600, 1650, 1700, 1750,
    #                        1800, 1850, 1900, 1950, 2000, 2050, 2100, 2150, 2200, 2250, 2300, 2350, 2400, 2450, 2500,
    #                        2550, 2600, 2650, 2700, 2750, 2800, 2850, 2900, 2950, 3000, 3050, 3100, 3150, 3200, 3250, 3300,
    #                        3350, 3400, 3450, 3500, 3550, 3600, 3650, 3700, 3750, 3800, 3850, 3900, 3950, 4000, 4050, 4100,
    #                        4150, 4200, 4250, 4300, 4350, 4400, 4450, 4500, 4550, 4600, 4650, 4700, 4750, 4800, 4850, 4900,
    #                        4950, 5000]

    #Make jobs dict in memory
    JOBS = createJobsDict()
    # Create pnr2d15 dict in memory
    PNR2D15 = createPNR2D15(pnrList, deptimeList)

    for origin in reversed(originList):

        for deptime in deptimeList:

            # Initiate travel time list to destination set, start with max time then update with min.
            destTTPrev = np.array([[2147483647, ]] * DEST_NUM)

            #Iteratively update destTTPrev with minimum travel times.
            for pnr in pnrList:

                #Return origin TT and the automobile cost in a numpy array

                orTT, a_cost, nullCounter = matchOrigin(origin, deptime, pnr, SCENARIO, nullCounter)

                #If origin cannot reach the chosen PNR in 3.5 hours, pick a new PNR
                if orTT is not None:

                    depsum = deptime + orTT
                    # If depsum is past the calculation window (8:45 AM), do not link paths.
                    if depsum < LIMIT:

                        # Choose dest_deptime based on closest 15 min bin.
                        depsumBin = linkBins(depsum, deptimeList)
                        if depsumBin != LIMIT:

                            #Time from origin and transfer (int)  = originTT (int) + transfer (int)
                            #Removing the transfer (7/13/18).
                            #orTT_trans = np.array([orTT + TRANSFER, a_cost])

                            #Create an array of tuples that contains the total tt for each destination, auto cost, associated PNR
                            total_tt_array = PNR2D15[pnr][depsumBin] + np.array([orTT, a_cost])

                            #Alternative for tuples:
                            #bestTT = [min(pair, key=lambda x:x[0]) for pair in zip(destTTPrev, total_tt_array)]
                            #Alternative for numpy list of lists
                            bestTT = np.minimum(destTTPrev, total_tt_array)

                            destTTPrev = bestTT

            calcTimeAccess(origin, deptime, destination_list, destTTPrev, writer_time)
            calcMonetaryAccess(origin, deptime, destination_list, destTTPrev, writer_cost)

        print('Origin {} finished'.format(origin), time.time() - t0)
        bar.next()
        print(" ")

    bar.finish()
    print('{} origin-deptime-pnr combos exceeded 3.5 hours of travel time thus do not have value in DB'.format(nullCounter))
    readable_end = time.ctime(time.time() - t0)
    print(readable_end)
