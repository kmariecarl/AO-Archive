#This script is a variation on the TTMatrixLink2JobsCost.py by connecting origins to destinations using the AUTO and TRANSIT TT matrices
#and the costs associated with time, fuel and transit fare. Result is a monetary accessibility file.

#Notes:
#All costs listed in cents. ie. $4.00 listed as 400

#Not all origins can be associated with all PNRs for a COST due to limits on path analysis file size.


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
def makeLists():
    print('Making input lists')
    #Faster way to select unique list of PNRs, deptimes, Destinations
    cur.execute("SELECT {}.{}.origin FROM {}.{} GROUP BY origin ORDER BY origin ASC;".format(SCHEMA,TABLE1, SCHEMA, TABLE1))
    origins = cur.fetchall()
    origin_list = [x[0] for x in origins]
    print('Origin list created', time.time() - t0)

    cur.execute("SELECT {}.{}.destination FROM {}.{} GROUP BY destination ORDER BY destination ASC;".format(SCHEMA, TABLE1, SCHEMA, TABLE1))
    pnrs = cur.fetchall()
    pnr_list = [x[0] for x in pnrs]
    print('PNR list created', time.time() - t0)

    cur.execute("SELECT {}.{}.deptime_sec FROM {}.{} GROUP BY deptime_sec ORDER BY deptime_sec;".format(SCHEMA,TABLE1, SCHEMA,TABLE1))
    or_dep = cur.fetchall()
    or_dep_list = [x[0] for x in or_dep]
    print('Deptime list created', time.time() - t0)

    cur.execute("SELECT {}.{}.destination FROM {}.{} GROUP BY destination ORDER BY destination ASC;".format(SCHEMA,TABLE2, SCHEMA,TABLE2))
    dest = cur.fetchall()
    dest_list = [x[0] for x in dest]
    print('Destination list created', time.time() - t0)

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

#This function relys on the db structure of the tt matrices already imported into postgresql.
#Once the tt array has been extracted, convert to numpy array.
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

            #Create a list of lists [tt, cost] then transform to numpy array
            tt_list = [[dest_tt[0], 10000] for dest_tt in tt]

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




#Query the o2pnr matrix for matching origin, deptime_sec, pnr combo
def matchOrigin(origin, deptime, pnr, scenario, null_counter):
    query = """SELECT traveltime
               FROM {}.{}
               WHERE origin = %s AND deptime_sec = %s AND destination = %s;"""
    cur.execute(query.format(SCHEMA,TABLE1), (origin, deptime, pnr))
    tt = cur.fetchall()

    query2 = """SELECT {}
               FROM {}.{}
               WHERE origin = %s AND deptime_sec = %s AND destination = %s;"""

    #In many cases an origin may have a tt but not a cost for the selected PNR, if the tt to that PNR is greater than 40 minutes
    #which is what was calculated by the path analyst program.
    try:
        cur.execute(query2.format(scenario, SCHEMA, COSTS), (origin, deptime, pnr))
        c = cur.fetchall()
        value = round(float(c[0][0]), 2)

    except:
        null_counter += 1
        value = 10000  #=$100.00

    return int(tt[0][0]), value, null_counter

#Place depsum values into a 15 minute bin. Ex. 6:05 will get placed in the 6:15 bin.
#Bins are rounded up to allow for transfer time between origin egress to destination ingress
def linkBins(depsum, deptime_list):
    ##Top portion remains commented out until larger matrices are calculated
    # more_time = [900*i for i in range(1, 7)]
    # last_time = deptime_list_sort[-1] #Grab last time in list : 9:00 = 32400
    # extend_deptime_list_sort = list(deptime_list_sort)
    # for k in more_time:
    #     extend_deptime_list_sort.append((last_time+k))
    # print('Extended deptime list:', extend_deptime_list_sort)
    # Assign depsum to the 15 minute bin
    for index, dptm in enumerate(deptime_list):
        next = index + 1
        if next < len(deptime_list):
            if depsum > dptm and depsum <=  deptime_list[next]:
                # Depsum_bin is in the form of seconds!
                depsum_bin = deptime_list[next] #extend_deptime_list_sort[next]
                return depsum_bin #Seconds, contains times in the 15 minutes less than this depsum_bin


#Place the current row's destination into threshold bins based on its travel time
#If destination cannot be reached, then do not include in output.
#Output is an ordered dictionary mapping threshold to list of GEOIDs that can be reached
# def calcAccess(origin, deptime, destination_list, destTTPrev, writer_time, writer_cost):
#     # Before you write an entry, calculate access by connecting jobs to destinations
#     # Make one query to jobs db per threshold:
#     # Create a new ordered dict structure for the origin_deptime combo
#     thresh_dict = OrderedDict()
#     cost_dict = OrderedDict()
#     for x in THRESHOLD_LIST_MINUTE:
#         thresh_dict[x] = []
#
#     for y in THRESHOLD_COST_LIST:
#         cost_dict[y] = []
#
#     # Iterate through the destinations and their respective TTs.
#     for dest, totalTT_tup in zip(destination_list, destTTPrev):
#         # Only assign jobs where destination can be reached
#         #totalTT_tup[0] is travel time, while tup[1] is auto cost
#         if totalTT_tup[0] < 2147483647:
#             # OTP puts TT in minutes then rounds down, the int() func. always rounds down.
#             # Think of TT in terms of 1 min. bins. Ex. A dest with TT=30.9 minutes should not be placed in the 30 min TT list.
#             minbin = totalTT_tup[0] / 60
#             print('minbin',  minbin)
#             print('time7', time.time() - t0)
#             thresh_dict = assign2Thresh(minbin, thresh_dict, dest)
#             print('Thresh dict', thresh_dict)
#             print('time8', time.time() - t0)
#
#
#             a_t_cost = totalTT_tup[1] + int(FARE)
#             print('a_t_cost', a_t_cost)
#             cost_dict = assign2Cost(a_t_cost, cost_dict, dest)
#             print('Cost dict', cost_dict)
#             print('time9', time.time() - t0)
#
#     writeAccessFile(origin, deptime, thresh_dict, 'threshold', writer_time)
#     writeAccessFile(origin, deptime, cost_dict, 'cost', writer_cost)
#
#
# def assign2Thresh(minbin, thresh_dict, dest):
#     for thresh in THRESHOLD_LIST_MINUTE:
#         # Place destination into only the first threshold that allows that destination to be reached
#         if minbin <= thresh:
#
#             thresh_dict[thresh].append(dest)
#
#             return thresh_dict
#
#
# def assign2Cost(a_t_cost, cost_dict, dest):
#     for cost in THRESHOLD_COST_LIST:
#         # Place destination into only the first cost threshold that allows that destination to be reached
#         if a_t_cost <= cost:
#
#             cost_dict[cost].append(dest)
#             return cost_dict

#Place the current row's destination into threshold bins based on its travel time
#If destination cannot be reached, then do not include in output.
#Output is an ordered dictionary mapping threshold to list of GEOIDs that can be reached
def calcTimeAccess(origin, deptime, destination_list, destTTPrev, writer_time):
    #print('time7', time.time() - t0)
    thresh_dict = OrderedDict()

    for x in THRESHOLD_LIST_MINUTE:
        thresh_dict[x] = []

    # Iterate through the destinations and their respective TTs.
    for dest, totalTT_tup in zip(destination_list, destTTPrev):
        # Only assign jobs where destination can be reached
        #totalTT_tup[0] is travel time, while tup[1] is auto cost
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
        #totalTT_tup[0] is travel time, while tup[1] is auto cost
        if totalTT_tup[0] < 2147483647:

            a_t_cost = totalTT_tup[1] + int(FARE)



            for cost in THRESHOLD_COST_LIST:
                # Place destination into only the first cost threshold that allows that destination to be reached
                if a_t_cost <= cost:

                    cost_dict[cost].append(dest)
                    break

    writeAccessFile(origin, deptime, cost_dict, 'cost', writer_cost)




#This function evaluates the threshold dictionary provided and returns the jobs that match with the given destination list
#then write to the provided writer file.
def writeAccessFile(origin, deptime, threshold_dict, threshold_type, writer):
    #print('time9', time.time() - t0)

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
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=54000)

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  #ENTER AS TTMatrixLink
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS ttmatrices
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #Table 1 in schema, i.e. o2pnr
    parser.add_argument('-table2', '--TABLE2_NAME', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-jobstab', '--JOBS_TABLE_NAME', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-costtab', '--PATH_COST_TABLE_NAME', required=True, default=None)  #Path cost table in schema, i.e. path_cost
    parser.add_argument('-lim', '--CALC_LIMIT', required=True, default=32400)  #Calculation cutoff, i.e. 32400 = 9:00 AM
    parser.add_argument('-scen', '--SCENARIO', required=True, default=None)  #Cost scenario to calc access from, i.e. A, B, C
    parser.add_argument('-fare', '--FARE', required=True, default=325)  #Rush fare is $3.35 otherwise put $0.00 for other scenarios
    parser.add_argument('-or', '--ORIGIN_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-pnr', '--PNR_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-dep', '--DEPTIME_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-dest', '--DESTINATION_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    #Optionally include the value of time on transit portion of the trip
    parser.add_argument('-vot', '--VALUE_OF_TIME', required=False, default=None)  #Value of time in cents, i.e. 2707 based on USDOT

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
    # VOT = int(args.VALUE_OF_TIME)


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
    originList, pnrList, deptimeList, destination_list = makeLists()
    # originList = mod.readList(args.ORIGIN_LIST, str)
    # pnrList = mod.readList(args.PNR_LIST, str)
    # deptimeList = mod.readList(args.DEPTIME_LIST, int)
    # destination_list = mod.readList(args.DESTINATION_LIST, str)


    DEP2_SEC_DICT = deptime2SecDict()
    DEST_NUM = len(destination_list)
    print('Dest Num=', DEST_NUM)
    nullCounter = 0
    #Make threshold list to check travel times against.
    THRESHOLD_LIST = [300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000, 3300, 3600, 3900, 4200,
                      4500, 4800, 5100, 5400]

    THRESHOLD_LIST_MINUTE = [int(x/60) for x in THRESHOLD_LIST]

    #THRESHOLD_COST_LIST = [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400]
    THRESHOLD_COST_LIST = [200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1050, 1100, 1150, 1200, 1250, 1300, 1350, 1400]
    #For Unit testing
    #THRESHOLD_COST_LIST = [320, 330, 340, 350, 360, 370, 380, 390, 400 ]

    # Create pnr2d15 dict in memory
    PNR2D15 = createPNR2D15(pnrList, deptimeList)
    #Make jobs dict in memory
    JOBS = createJobsDict()


    for origin in originList:


        for deptime in deptimeList:

            # Initiate travel time list to destination set, start with max time then update with min.
            destTTPrev = [(2147483647, 10000)] * DEST_NUM

            #Iteratively update destTTPrev with minimum travel times.
            for pnr in pnrList:
                #print('time1', time.time() - t0)

                #Return origin TT and the automobile cost
                orTT, a_cost, nullCounter = matchOrigin(origin, deptime, pnr, SCENARIO, nullCounter)

                #print('time2', time.time() - t0)

                #If origin cannot reach the chosen PNR in 90 minutes, pick a new PNR
                if orTT is not None:
                    depsum = deptime + orTT

                    # If depsum is past the calculation window (9:00 AM), do not link paths.
                    if depsum < LIMIT:

                        # Choose dest_deptime based on closest 15 min bin.
                        depsumBin = linkBins(depsum, deptimeList)
                        if depsumBin != LIMIT:

                            #Time from origin and transfer (int)  = originTT (int) + transfer (int)
                            orTT_trans = np.array([orTT + (depsumBin - depsum), a_cost])

                            #Create an array of tuples that contains the total tt for each destination and the auto cost
                            #print('time3', time.time() - t0)
                            total_tt_array = PNR2D15[pnr][depsumBin] + orTT_trans
                            #total_tt_array = [(destTT + orTT_trans, a_cost) for destTT in PNR2D15[pnr][depsumBin]]
                            #print('time4', time.time() - t0)

                            #In order to use the numpy minimum function, I cannot compare tuples :(
                            #bestTT = minimum(destTTPrev, total_tt_array)

                            #print('time5', time.time() - t0)
                            #Alternative for tuples:
                            #bestTT = [min(pair, key=lambda x:x[0]) for pair in zip(destTTPrev, total_tt_array)]
                            #Alternative for numpy list of lists
                            bestTT = np.minimum(destTTPrev, total_tt_array)
                            #print('time6', time.time() - t0)

                            destTTPrev = bestTT

            calcTimeAccess(origin, deptime, destination_list, destTTPrev, writer_time)
            calcMonetaryAccess(origin, deptime, destination_list, destTTPrev, writer_cost)

        print('Origin {} finished'.format(origin), time.time() - t0)
        bar.next()
        print(" ")

    bar.finish()
    print('{} origin-deptime-pnr combos did not have an assigned auto cost due to path analysis file size limitations'.format(nullCounter))
    readable_end = time.ctime(time.time() - t0)
    print(readable_end)
