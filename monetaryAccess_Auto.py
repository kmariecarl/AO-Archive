#This script is a variation on the monetaryAccess_PNR.py program by connecting origins to destinations using only the auto tt matrix and path cost aggregations
#and the costs associated with time, fuel and parking. Result is a monetary accessibility file.

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

# Query the o2pnr matrix to make lists of unique origins, deptimes, and destinations
def makeLists():
    print('Making input lists')
    #Faster way to select unique list of PNRs, deptimes, Destinations
    cur.execute("SELECT {}.{}.origin FROM {}.{} GROUP BY origin ORDER BY origin ASC;".format(SCHEMA,TABLE1, SCHEMA, TABLE1))
    origins = cur.fetchall()
    origin_list = [x[0] for x in origins]
    print('Origin list created', time.time() - t0)

    cur.execute("SELECT {}.{}.deptime_sec FROM {}.{} GROUP BY deptime_sec ORDER BY deptime_sec;".format(SCHEMA,TABLE1, SCHEMA,TABLE1))
    or_dep = cur.fetchall()
    or_dep_list = [x[0] for x in or_dep]
    print('Deptime list created', time.time() - t0)

    dest_list = origin_list.copy()
    print('Destination list created', time.time() - t0)

    return origin_list, or_dep_list, dest_list

#This function relys on the db structure of the tt matrices already imported into postgresql.
#Once the tt array has been extracted, convert to numpy array.
def createTAZ2TAZ(origin_list, deptime_list):
    print('Building OD dictionary in memory...')
    o2d = defaultdict(lambda: defaultdict(list))
    for origin in origin_list:
        for deptime in deptime_list:

            query_tt = """SELECT traveltime
                           FROM {}.{}
                           WHERE origin = %s AND deptime_sec = %s
                           ORDER BY destination ASC"""

            cur.execute(query_tt.format(SCHEMA, TABLE1), (origin, deptime))  # next_bin,

            tt = cur.fetchall()  # Listed by ordered destination

            query_cost = """SELECT {}
                            FROM {}.{}
                            WHERE origin = %s AND deptime_sec = %s
                            ORDER BY destination ASC"""
            cur.execute(query_cost.format(SCHEMA, COSTS), (origin, deptime))

            cost = cur.fetchall()  #Listed by ordered destination

            #Create a list of lists [tt, cost] then transform to numpy array
            #Initialize cost as an empty value
            tt_cost_list = [[dest_tt[0], dest_cost[0] ] for dest_tt, dest_cost in zip(tt, cost)]

            tt_array = np.array(tt_cost_list)

            o2d[origin][deptime] = tt_array
        print('origin {} has been added to o2d dictionary'.format(origin))
        mod.elapsedTime(start_time)
    return o2d

def createJobsDict():
    print('Building Jobs Dict', time.time() - t0)
    jobs_dict = {}
    query = """SELECT taz, sumc000
               FROM {}.{};"""
    cur.execute(query.format(SCHEMA, JOBS))
    jobs = cur.fetchall()
    for tup in jobs:
        jobs_dict[tup[0]] = tup[1]

    print('Jobs Dict Now in Memory', time.time() - t0)
    return jobs_dict


def createParkDict():
    print('Building parking dictionary', time.time() - t0)
    park_dict = {}
    query = """SELECT origin, avgcostbyorigin from {}.{};"""
    cur.execute(query.format(SCHEMA, PARK))
    park = cur.fetchall()
    for tup in park:
        park_dict[tup[0]] = tup[1]

    print('Capacity Weighted Average Cost Dictionary Now in Memory', time.time() - t0)
    return park_dict

# #Query the o2pnr matrix for matching origin, deptime_sec, pnr combo
# def origin2TimeCost(origin, deptime, scenario, null_counter):
#     query = """SELECT traveltime
#                FROM {}.{}
#                WHERE origin = %s AND deptime_sec = %s;"""
#     #This should return a list of tt that correspond with taz destinations
#     cur.execute(query.format(SCHEMA,TABLE1), (origin, deptime))
#     tt = cur.fetchall()
#
#     query2 = """SELECT {}
#                FROM {}.{}
#                WHERE origin = %s AND deptime_sec = %s;"""
#
#     #This should return a list of cost that correspond with taz destinations
#     cur.execute(query2.format(scenario, SCHEMA, COSTS), (origin, deptime))
#     c = cur.fetchall()
#     value = round(float(c[0][0]), 2)
#     print('Value:', value)
#
#
#     null_counter += 1
#     value = 10000  #=$100.00
#
#     return int(tt[0][0]), value, null_counter
#
# #Place depsum values into a 15 minute bin. Ex. 6:05 will get placed in the 6:15 bin.
# #Bins are rounded up to allow for transfer time between origin egress to destination ingress
# def linkBins(depsum, deptime_list):
#     ##Top portion remains commented out until larger matrices are calculated
#     # more_time = [900*i for i in range(1, 7)]
#     # last_time = deptime_list_sort[-1] #Grab last time in list : 9:00 = 32400
#     # extend_deptime_list_sort = list(deptime_list_sort)
#     # for k in more_time:
#     #     extend_deptime_list_sort.append((last_time+k))
#     # print('Extended deptime list:', extend_deptime_list_sort)
#     # Assign depsum to the 15 minute bin
#     for index, dptm in enumerate(deptime_list):
#         next = index + 1
#         if next < len(deptime_list):
#             if depsum > dptm and depsum <=  deptime_list[next]:
#                 # Depsum_bin is in the form of seconds!
#                 depsum_bin = deptime_list[next] #extend_deptime_list_sort[next]
#                 return depsum_bin #Seconds, contains times in the 15 minutes less than this depsum_bin


#Place the current row's destination into threshold bins based on its travel time
#If destination cannot be reached, then do not include in output.
#Output is an ordered dictionary mapping threshold to list of GEOIDs that can be reached
def calcTimeAccess(origin, deptime, destination_list, or_slice, writer_time):
    thresh_dict = OrderedDict()

    for x in THRESHOLD_LIST_MINUTE:
        thresh_dict[x] = []

    # Iterate through the destinations and their respective TTs.
    for dest, totalTT_tup in zip(destination_list, or_slice):
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

def calcMonetaryAccess(origin, deptime, destination_list, or_slice, writer_cost):
    #print('time8', time.time() - t0)
    cost_dict = OrderedDict()

    for y in THRESHOLD_COST_LIST:
        cost_dict[y] = []

    # Iterate through the destinations and their respective TTs.
    for dest, tt_cost_tup in zip(destination_list, or_slice):
        # Only assign jobs where destination can be reached
        #totalTT_tup[0] is travel time, while tup[1] is auto cost
        if tt_cost_tup[0] < 2147483647:
            time_cost = tt_cost_tup[0] * VOT/3600 #If VOT = 0, then VOT is not added to total cost
            #Auto costs along path plus parking cost associated with destination + time costs
            a_p_cost = tt_cost_tup[1] + PARKING[dest] + time_cost

            for cost in THRESHOLD_COST_LIST:
                # Place destination into only the first cost threshold that allows that destination to be reached
                if a_p_cost <= cost:

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

            for dest in dest_list:
                thresh_level_access += JOBS[dest]

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
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=3030)

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  #ENTER AS TTMatrixLink
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS ttmatrices
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #Table 1 in schema, i.e. o2d or auto_taz2taz_tt
    parser.add_argument('-jobstab', '--JOBS_TABLE_NAME', required=True, default=None)  #Jobs table, i.e. jobs, taz_jobs
    parser.add_argument('-costtab', '--PATH_COST_TABLE_NAME', required=True, default=None)  #Path cost table in schema, i.e. path_cost
    parser.add_argument('-parktab', '--PARK_COST_TABLE_NAME', required=True, default=None)  #Parking cost table in schema, i.e. wtavgcost_2018
    parser.add_argument('-lim', '--CALC_LIMIT', required=True, default=32400)  #Calculation cutoff, i.e. 32400 = 9:00 AM
    parser.add_argument('-scen', '--SCENARIO', required=True, default=None)  #Cost scenario to calc access from, i.e. A, B, C
    parser.add_argument('-vot', '--VALUE_OF_TIME', required=True, default=1803)  #Value of time in cents, i.e. $18.03 based on USDOT
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
    PARK = args.PARK_COST_TABLE_NAME
    LIMIT = int(args.CALC_LIMIT)
    SCENARIO = args.SCENARIO
    FARE = args.FARE
    if args.VALUE_OF_TIME:
        VOT = int(args.VALUE_OF_TIME)
    TRANSFER = 300


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
    originList, deptimeList, destination_list = makeLists()

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

    #THRESHOLD_COST_LIST = [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400]
    THRESHOLD_COST_LIST = [200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000,
                           1050, 1100, 1150, 1200, 1250, 1300, 1350, 1400, 1450, 1500, 1550, 1600, 1650, 1700, 1750,
                           1800, 1850, 1900, 1950, 2000, 2050, 2100, 2150, 2200, 2250, 2300, 2350, 2400, 2450, 2500,
                           2550, 2600, 2650, 2700, 2750, 2800, 2850, 2900, 2950, 3000]

    # Create pnr2d15 dict in memory
    O2D = createTAZ2TAZ(originList, deptimeList)
    #Make jobs dict in memory
    JOBS = createJobsDict()
    #Make parking dict in memory
    PARKING = createParkDict()


    for origin in reversed(originList):

        for deptime in deptimeList:
            or_slice = O2D[origin][deptime]

            calcTimeAccess(origin, deptime, destination_list, or_slice, writer_time)
            calcMonetaryAccess(origin, deptime, destination_list, or_slice, writer_cost)

        print('Origin {} finished'.format(origin), time.time() - t0)
        bar.next()
        print(" ")

    bar.finish()
    readable_end = time.ctime(time.time() - t0)
    print(readable_end)
