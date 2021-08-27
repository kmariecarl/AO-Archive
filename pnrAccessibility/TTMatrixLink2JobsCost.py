#This script is an extension of TTMatrixLinkSQL.py by connecting origins to a job array and calculating accessibility.
# Assuming that TT matrices are loaded into PostgreSQL db.

#Assumption: All origins can reach at least 1 PNR in 30 minutes of driving.

#Assumption: If an origin reaches a given PNR after the time window (i.e. 9:00) then the next PNR is selected

#Assumption: Depsums are placed in bins according to their rounded up time. I.e. 6:21 depsum goes to the 6:30
#bin. This ensures transit rides do not depart before origin trip arrives.

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
from collections import defaultdict, OrderedDict
import time
from numpy import minimum, array, fromiter

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


            tt_list = [dest_tt[0] for dest_tt in tt]
            tt_array = array(tt_list)

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
def matchOrigin(origin, deptime, pnr):
    query = """SELECT traveltime
               FROM {}.{}
               WHERE origin = %s AND deptime_sec = %s AND destination = %s;"""
    cur.execute(query.format(SCHEMA,TABLE1), (origin, deptime, pnr))
    tt = cur.fetchall()

    return int(tt[0][0])

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


#
# #Place destination into cost thresholds if both the origin can reach a PNR and from the PNR to a destination can be reached.
# def moneyBuckets(origin, deptime, dest, tup):
#     # Process for monetary thresholds
#     #Assuming that the or_deptime combo can reach at least one PNR within 30 minutes
#     a_cost = returnAutoCost(origin, deptime, tup[3])
#     print('Origin {} at deptime {} to PNR {} costs {}'.format(origin, deptime, tup[3], a_cost))
#     if a_cost is not None:
#         t_cost = returnTransitCost(tup[0])
#         print('PNR {} to destination {} using transit costs {}'.format(tup[3], dest, t_cost))
#         if t_cost is not None:
#             filterCost(dest, a_cost, t_cost)# Put destination into each of the monetary bins $2, $4, etc.
#         else:
#             pass
#     else:
#         pass
#
# #This function matches auto path cost info from the DB to the selected or_dep_pnr combo. The cost already has value of
# #time factored in, along with fuel price, driving environment, etc.
# def returnAutoCost(origin, deptime, pnr):
#     query = """SELECT {}
#                FROM {}.{}
#                WHERE CAST(origin as BIGINT) = %s
#                AND CAST(deptime AS INT) = %s
#                AND CAST(destination AS INT) = %s;"""
#     cur.execute(query.format(SCENARIO, SCHEMA, COSTS), (str(origin), deptime, pnr))
#     a = cur.fetchone()
#     #This is should only occur in test situation where the links I have path_costs for only start at 7:00 am.
#     if a is None:
#         pass
#         # a_cost = 200000 #No information at the deptime
#
#     else:
#         a_cost = a[0] #This should return a single value
#
#         return a_cost
#
# #This function returns the travel cost based on the constant fare and a VOT calculation done internally.
# #The time includes the transfer time.
# def returnTransitCost(destTT_transfer):
#     if destTT_transfer != 2147483647:
#         #If the optional VOT value is provided, the user would like to calculate transit costs including VOT.
#         if VOT:
#             # VOT for business travel is considered 100% of wage and for the US on average that value is $27.07 /person-hour, for MN: $18.03
#             person_hours = round((destTT_transfer / 3600), 4)
#             fare = 325  # $3.25 rush hour
#             t_cost = (person_hours * VOT) + fare  # Returns a value in cents
#             return t_cost
#         else:
#             fare = 325
#             t_cost = fare
#             return t_cost
#
#     else:
#         pass
#
# #This function places the PNR scenario path cost into appropriate monetary cost bins.
# def filterCost(dest, a_cost, t_cost):
#     total = a_cost + t_cost
#
#     applicable_cost_list = []
#     non_applicable_cost_list = []
#     for item in THRESHOLD_COST_LIST:
#         if total < item:
#             applicable_cost_list.append(item)
#         else:
#             non_applicable_cost_list.append(item)
#     for cost in sorted(applicable_cost_list):
#         COST_DICT[cost].append(dest)
#     for empty_cost in sorted(non_applicable_cost_list):
#         COST_DICT[empty_cost].append(None)

#Place the current row's destination into threshold bins based on its travel time
#If destination cannot be reached, then do not include in output.
#Output is an ordered dictionary mapping threshold to list of GEOIDs that can be reached
def calcAccess(destination_list, destTTPrev):
    # Before you write an entry, calculate access by connecting jobs to destinations
    # Make one query to jobs db per threshold:
    # Create a new ordered dict structure for the origin_deptime combo
    thresh_dict = OrderedDict()
    for x in THRESHOLD_LIST_MINUTE:
        thresh_dict[x] = []

    # Iterate through the destinations and their respective TTs.
    for dest, totalTT in zip(destination_list, destTTPrev):
        # moneyBuckets(origin, deptime, dest, tup)
        # Only assign jobs where destination can be reached
        if totalTT < 2147483647:
            # OTP puts TT in minutes then rounds down, the int() func. always rounds down.
            # Think of TT in terms of 1 min. bins. Ex. A dest with TT=30.9 minutes should not be placed in the 30 min TT list.
            minbin = totalTT / 60
            for thresh in THRESHOLD_LIST_MINUTE:
                # Place destination into only the first threshold that allows that destination to be reached
                if minbin <= thresh:
                    thresh_dict[thresh].append(dest)
                    break


    # writeAccessFile(origin, deptime, COST_DICT, 'cost', writer_cost)
    writeAccessFile(origin, deptime, thresh_dict, 'threshold', writer_time)

#This function evaluates the threshold dictionary provided and returns the jobs that match with the given destination list
#then write to the provided writer file.
def writeAccessFile(origin, deptime, threshold_dict, threshold_type, writer):
    #For each threshold, calculate accessibility
    access_prev = 0
    #Each destination list only contains the new destinations that can be reached at each successive threshold.
    for thresh, dest_list in threshold_dict.items():
        thresh_level_access = 0
        if len(dest_list) > 0:
            #access = 0

            # Destination , list actually needs to be a tuple to work with the WHERE IN query below
            #dest_tup = tuple(dest_list)


            # # Query DB for jobs that match destinations in the dest_list:
            # query = """SELECT sum(c000)
            #            FROM {}.{}
            #            WHERE geoid10 IN %s;"""
            # print('cur execute', time.time() - t0)
            # cur.execute(query.format(SCHEMA, JOBS), (dest_tup,))
            # jobs = cur.fetchone()
            # print('sum fetched', time.time() - t0)
            for geoid in dest_list:
                thresh_level_access += JOBS[geoid]

            access = thresh_level_access + access_prev


            entry = {'label': origin, 'deptime': mod.back2Time(deptime), '{}'.format(threshold_type): thresh * 60, 'jobs': access}
            writer.writerow(entry)
            access_prev = access
        else:
            access = access_prev
            entry = {'label': origin, 'deptime': mod.back2Time(deptime), '{}'.format(threshold_type): thresh * 60, 'jobs': access }
            writer.writerow(entry)


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    t0 = time.time()
    print(readable)

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  #ENTER AS TTMatrixLink
    parser.add_argument('-schema', '--SCHEMA_NAME', required=True, default=None)  # ENTER AS ttmatrices
    parser.add_argument('-table1', '--TABLE1_NAME', required=True, default=None)  #Table 1 in schema, i.e. o2pnr
    parser.add_argument('-table2', '--TABLE2_NAME', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-jobstab', '--JOBS_TABLE_NAME', required=True, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-lim', '--CALC_LIMIT', required=True, default=32400)  #Calculation cutoff, i.e. 32400 = 9:00 AM
    parser.add_argument('-or', '--ORIGIN_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-pnr', '--PNR_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-dep', '--DEPTIME_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    parser.add_argument('-dest', '--DESTINATION_LIST', required=False, default=None)  #Table 2 in schema, i.e. pnr2d15
    #Optional until monetary cost is fixed
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
    LIMIT = int(args.CALC_LIMIT)
    # COSTS = args.PATH_COST_TABLE_NAME
    # SCENARIO = args.SCENARIO
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

    # DEPTIME_SEC_LIST = [mod.convert2Sec(i) for i in deptimeList]

    DEP2_SEC_DICT = deptime2SecDict()
    DEST_NUM = len(destination_list)
    #Make threshold list to check travel times against.
    THRESHOLD_LIST = [300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000, 3300, 3600, 3900, 4200,
                      4500, 4800, 5100, 5400]

    THRESHOLD_LIST_MINUTE = [int(x/60) for x in THRESHOLD_LIST]

    THRESHOLD_COST_LIST = [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400]

    # Create pnr2d15 dict in memory
    PNR2D15 = createPNR2D15(pnrList, deptimeList)
    #Make jobs dict in memory
    JOBS = createJobsDict()


    for origin in originList:

        for deptime in deptimeList:
            # Initiate travel time list to destination set, start with max time then update with min.
            destTTPrev = array([2147483647] * DEST_NUM)

            #Iteratively update destTTPrev with minimum travel times.
            for pnr in pnrList:

                #KRISTIN: CHECK HOW THE O2PNR TT MATRIX WAS CREATED, IF ALL BATCH-ANALYST.MX WAS USED THEN ALL PNRS
                #CAN BE REACHED AND THERE MAY NOT BE A REASON FOR IF ORTT IS NOT NONE, MIGHT BE AN AFTIFACT OF MINI NETWORK
                orTT = matchOrigin(origin, deptime, pnr)


                #If origin cannot reach the chosen PNR in 90 minutes, pick a new PNR
                if orTT is not None:
                    depsum = deptime + orTT

                    # If depsum is past the calculation window (9:00 AM), do not link paths.
                    if depsum < LIMIT:

                        # Choose dest_deptime based on closest 15 min bin.
                        depsumBin = linkBins(depsum, deptimeList)
                        if depsumBin != LIMIT:

                            #Time from origin and transfer (int)  = originTT (int) + transfer (int)
                            orTT_trans = orTT + (depsumBin - depsum)

                            #Create a numpy array that contains the total tt for each destination
                            total_tt_array = PNR2D15[pnr][depsumBin] + orTT_trans


                            bestTT = minimum(destTTPrev, total_tt_array)


                                #Alternative for tuples:
                                #bestTT = [min(pair, key=lambda x:x[0]) for pair in zip(destTTPrev, destTTList)]

                            destTTPrev = bestTT

            calcAccess(destination_list, destTTPrev)


        print('Origin {} finished'.format(origin), time.time() - t0)

    readable_end = time.ctime(time.time() - t0)
    print(readable_end)
