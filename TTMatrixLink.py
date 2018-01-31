#This script links TT matrix data for an intermediate destination (park-and-rides in this case), in order to get a single travel time
#value from origin to destination

#LOGIC:
#1. Select Park-and-Ride
#2. Match origins dict to destinations dict using common PNR key.
#3. Create a dictionary containing all viable paths
#4. Narrow the all_paths_dict to shortest paths for all OD pairs.
#5. Print the shortest paths dictionary to file.


#NOTES:

#First, in order to use this script, TT by auto from O to PNR, TT by transit from PNR to D, must be calculated.

#Seconds, the incoming transit TT matrix needs to be aggregated up to the same resolution as the auto TT matrix, in
#this case, 15 minutes.


#Wait time at the PNR for transit service is accounted for by selecting the 15th percentile of travel times
# which more accurately reflects how PNR users arrive at PNR stations --the minimize their wait time with some buffer time
# thus the 15th percentile is used.

#Auto TT matrix resolution should correspond with linking PNR2D matrix criteria, i.e. 15 min deptimes means 15 min buffer.
#Auto TT does not use percentile to narrow down values

#Program assumes that each PNR is associated with all destinations whether or not the destinations can be reached
#or not in the given travel time. BUT I am implementing reader(next) on the origin to PNR and PNR to destination rows
#where the destination TT=2147483647 because these will never be viable paths (1-22-18).

#Despite the refactoring to segment processes by PNR, I am maintaining the data structure format where the PNR stays as
#the outter dict key even though that means there will only be one outter dict key.

#ASSUMPTIONS: Use the 15th percentile of travel times. Origin to PNR auto trips are less than 30 minutes.
# Transfer window is 10 minutes.

#EXAMPLE USAGE: kristincarlson$ python TTMatrixLink.py -pnr PNRList_xxx.txt -deptimes Deptimes_xxx.txt

#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
import numpy
import glob
from collections import defaultdict


#################################
#           FUNCTIONS           #
#################################

def convert2Sec(timeVal):
    #'timeVal' is a number like '0632' for 6:32 AM. The list comprehension breaks the into apart.
    list = [i for i in timeVal]
    #Grab the first two digits which are the hours -> convert to seconds
    hours = (int(list[0]) + int(list[1])) * 3600
    #Grab the third and fouth digits which are the minutes -> convert to seconds.
    minutes = int('{}{}'.format(list[2],list[3]))*60
    seconds = hours + minutes
    return seconds

#Use current_time for tracking elapsed runtime.
def startTimer():
    start_time = time.time()
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return start_time, currentTime

#A function that prints out the elapsed calculation time
def elapsedTime():
    elapsed_time = time.time() - start_time
    print("Elapsed Time: ", elapsed_time)

#This function reads in the PNRList and Deptimes files created by matrixBreaker.py
def readList(file):
    with open(file, 'r') as infile:
        csvreader = csv.reader(infile, delimiter=',')
        list = []
        for item in csvreader: #Each item is actually the entire list of PNRs or deptimes.
            for val in item: #Each val is the PNR or deptime
                if val != '': #The last val is empty due to the matrixBreaker output format
                    list.append(str(val))
    print("PNR or Deptimes List:", list)
    return list

#Standard for writing files out to disk
def mkOutput(currentTime, fieldnames, name):
    outfile = open('output_{}_{}.txt'.format(name,curtime), 'w')
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    return writer


#A triple nested dictionary is the product of this function
#{PNR:{destination/origin:{deptime:traveltime,
#                          deptime:traveltime,
#                          deptime:traveltime},
#     {destination/origin:{deptime:traveltime,
#                          deptime:traveltime,
#                          deptime:traveltime}}
#PNR:...
def makeNestedDict(pnr, name, outter_val, inner_val):
    with open ('PNR_{}_{}.txt'.format(pnr, name), 'r') as input:
        reader = csv.DictReader(input)

        #Initiate the triple nested dict structure
        nest = defaultdict(lambda: defaultdict(dict))

        for row in reader:
            nest[row[outter_val]][row[inner_val]][row['deptime']] = row['traveltime']
        #print('nest = ', nest)

        print("Created Nested Dictionary:", inner_val)
        return nest

#Create double nested dictionary to map destination to 15th percentile TT. Remake every PNR.
#{destination:{deptime:{15thTT,
#              deptime:{15thTT,
#              deptime:{15thTT}}
#destination:...
def makeCentileDict(p2d_dict, deptime_list):
    p2d15_dict = defaultdict(lambda: defaultdict(int))
    #Convert deptime_list into seconds
    deptime_list_sec = [convert2Sec(i) for i in deptime_list]
    #and sort in ascending order
    deptime_list_sort = sorted(deptime_list_sec)
    print("Sorted deptime list: ", deptime_list_sort)

    #Break off PNR
    for pnr_key, dest_dict in p2d_dict.items():
        #Break off destinations
        for dest, timing in p2d_dict[pnr_key].items():
            # Cycle through 15 minute departure time list from origin and apply to destinations
            for index, time_val in enumerate(deptime_list_sort):
                next = index + 1
                #Make sure not to exceed 15th item in list otherwise error
                if next < len(deptime_list_sort):
                    # Create list to store TT to calc 15th percentile from
                    bin15 = []
                    #Break out deptimes for each destination
                    count = 0
                    for deptime, tt in p2d_dict[pnr_key][dest].items():
                        deptime_sec = convert2Sec(deptime)
                        #print("time min:", time_val)
                        # print("deptime:", deptime_sec)
                        # print("time max:", deptime_list_sort[next])
                        #Check if deptime is within 15 minute bin. Ex. if 6:02 >= 6:00 and 6:02 < 6:15...
                        if deptime_sec >= time_val and deptime_sec < deptime_list_sort[next]:
                            count += 1
                            bin15.append(int(tt))
                    #Calc 15th percentile TT and add to centile_dict
                    # print("Number of times added to bin:", count)
                    new_tt = calcPercentile(bin15)
                    p2d15_dict[dest][time_val] = new_tt
    print("Centile Dict Created")
    elapsedTime()
    return p2d15_dict

#This function calculates the bottom 15th percentile travel time ~= 85th percentile for a particular OD Pair
def calcPercentile(bin_15):
    if allSame(bin_15) is True:
        tile = 2147483647
    #Interpolation=lower will round the percentile index down which helps the function
    #not incorporate the max value into the percentile calculation yet factor it into the lineup.
    else:
        tile = numpy.percentile(bin_15, 15, interpolation='lower')
    return int(tile)

#Check if list contains all Maxtime values (all the same values)
def allSame(items):
    return all(x == items[0] for x in items)

#Make a dictionary that stores origins, departure times, and a list of TT, depsum
def makeDepsumDict(p2o_dict):
    #Create depsum_dict
    depsum_dict = {}
    for or_key, orgn_dict in p2o_dict.items(): #Throw away or_key. Destination dictionary has already been extracted from
        #feasibleDest() function.
        orgn_list = [k for k in orgn_dict.keys()]
        for orgn in orgn_list:
            depsum_dict[orgn] = {}
            for or_deptime, or_traveltime in orgn_dict[orgn].items():
                depsum = convert2Sec(or_deptime) + int(or_traveltime)
                depsum_dict[orgn][or_deptime] = [int(or_traveltime), depsum]

    print("Depsum Dict Created")
    elapsedTime()
    return depsum_dict
    
#Every use of this function accounts for all paths connecting through the selected PNR.
#select PNR -> select destination -> select origin -> select destination departure time
#Order chosen to reduce time, first triage possible destinations because this set is much larger than iterating through
#the origin set.
def linkPaths(orgn, key_PNR, p2d15_dict, depsum_dict):
    #Use the depsum_dict as the origin info dict to iterate through origin deptimes.
    for or_deptime in depsum_dict[orgn].keys():
        #Pick one destination
        for dest in p2d15_dict:
            #Find a deptime and TT that fulfill the requirements below for the selected destination.
            for dest_deptime, dest_tt in p2d15_dict[dest].items():
                #Check that origin depsum falls within 10 minutes of the selected destination and its departure time.
                #also make sure that the destination can be reached.
                #Ex. if depsum < dest_deptime and depsum >= ten min. window prior to dest_deptime
                #and the destination can be reached...
                # print("Depsum:",depsum_dict[orgn][or_deptime][1] )
                # print("Dest_deptime:", dest_deptime)
                # print("10 min window:", (dest_deptime - 600))
                # print("Dest travel time:", dest_tt)
                if depsum_dict[orgn][or_deptime][1] < dest_deptime \
                    and depsum_dict[orgn][or_deptime][1] >= (dest_deptime - 600) and dest_tt != 2147483647:
                    #print("Connecting origin {} to destination {} at {}".format(orgn, dest, dest_deptime))
                    #Calculate transfer time from origin trip to destination trip
                    #print("PATH FEASIBLE!")
                    transfer_time = int(dest_deptime - depsum_dict[orgn][or_deptime][1])
                    #print('transfer time = ', transfer_time)
                    path_TT = depsum_dict[orgn][or_deptime][0] + int(dest_tt) + transfer_time
                    #print("Path TT:", path_TT)
                    # 7 This path is viable, add to list.
                    add2AllPathsDict(orgn, dest, key_PNR, or_deptime, path_TT)
    #print("Origin {} connected to feasible destinations at all deptimes".format(orgn))
    #elapsedTime()


#This function looks at the combo of origin + dest + PNR + deptime handed to it, and adds it to a 4x nested dict in the
#following structure:
#allPathsDict = {origin:{destination:{PNR1:{or_deptime:path_TT,
#                                        or_deptime:path_TT,
#                                        or_deptime:path_TT}
#                                    PNR2:{or_deptime:path_TT,
#                                        or_deptime:path_TT,
#                                        or_deptime:path_TT}}
#                        destination:{PNR1:{or_deptime:path_TT,
#                                        or_deptime:path_TT,
#                                        or_deptime:path_TT}
#                                    PNR2:{or_deptime:path_TT,
#                                        or_deptime:path_TT,
#                                        or_deptime:path_TT}}}
#               origin:{destination:{PNR1:{or_deptime:path_TT,
#                                        or_deptime:path_TT,
#                                        or_deptime:path_TT}
#                                    PNR2:{or_deptime:path_TT,
#                                        or_deptime:path_TT,
#                                        or_deptime:path_TT}}
#                        destination:{PNR1:{or_deptime:path_TT,
#                                        or_deptime:path_TT,
#                                        or_deptime:path_TT}
#                                    PNR2:{or_deptime:path_TT,
#                                        or_deptime:path_TT,
#                                        or_deptime:path_TT}}}}

#Function for nesting all viable path information
#Here I may need to add a way to write over existing content if a shorter path between OD pair is found.
#Each origin_destination_departuretime combo shoulc have a min TT and the associated PNR attached, thus the PnR and TT get
#overwritten if new info comes in at the next PNR iteration.
def add2AllPathsDict(origin, destination, PNR, or_deptime, path_TT):
    allPathsDict[origin][destination][PNR][or_deptime] = path_TT

#Take the all_paths_dict and find the shortest path between all OD pairs.
#The shortest path for an origin + dest + deptime combo can vary by PNR
def findSP(deptime_list, all_paths_dict):
    print("Finding shortest paths between OD pairs")
    #Each "row" is a separate origin
    for origin, outter in all_paths_dict.items():
        #print('origin', origin)
        #print('outter', outter)
        prev_time = time.time()
        for destination, inner in all_paths_dict[origin].items():
            #print("Destination", destination)
            #print('inner', inner)
            #print('origin:', origin, 'dest:', destination)
            #now use a method to select one deptime and compare across all PNRs and then for each departure time
            #you may get that different PNR result in the shortest travel time.
            for dptm in deptime_list:
                #print('dptm', dptm)
                dptm_dict = {} #where key=PNR: value=TT
                #For each deptime make a dictionary of all PNRs: tt, once the dict is created, run min().
                for PNR, timing in all_paths_dict[origin][destination].items():
                    #print('PNR', PNR)
                    #print("timing dictionary", timing)
                    if dptm in all_paths_dict[origin][destination][PNR]:
                        #print('here1')
                        dptm_dict[PNR] = all_paths_dict[origin][destination][PNR][dptm]
                        #print("dptm_dict", dptm_dict)
                    #If the particular OD + PNR combo does not have a path in the allPathsDict for the selected
                    #departure time, then move to the next PNR -- not to the next if statement
                    # print("origin:", origin)
                    # print("destination:",destination)
                    # print("PNR:", PNR)
                    # print("Origin Departure Time:", dptm)
                    # print("dptm_dict: ", dptm_dict)
                if any(dptm_dict) == True:
                    #print('here2')
                    #print("Found a OD + PNR combo that doesn't have a SP for the selected departure time")
                    minPNR = min(dptm_dict, key=dptm_dict.get)
                    #print("minPNR: ", minPNR)
                    minTT = dptm_dict[minPNR]
                    #minTT = all_paths_dict[origin][destination][minPNR][dptm]
                    #So far haven't addressed if two PNRs have equal and minimal TTs.
                    #print("SP between", origin, "and", destination, "uses PNR", minPNR, "TT=", minTT)
                    #elapsedTime()
                    add2SPDict(origin, dptm, destination, minPNR, minTT)

        rep_runtime = time.time() - prev_time
        #print("Origin {} connected to PNRs and Destinations in {} seconds".format(origin, rep_runtime))
    #print('Shortest paths dict:', spDict)
    print("Shortest Paths Are Found")
#Filter values into quadruple nested dictionary
def add2SPDict(origin, dptm, destination, pnr, sp_tt):
    spDict[origin][dptm][destination][pnr] = sp_tt

#Count shortest paths through each PNR
def countPNRS(spDict):
    print("Counting the number of shortest paths through each PNR")
    #Create a dictionary to store PNRS with the OD pairs that use each PNR in their SP.
    count_dict = {}
    for origin, outter in spDict.items():
        for deptime, inner in spDict[origin].items():
            for destination, locations in spDict[origin][deptime].items():
                for pnr, tt in spDict[origin][deptime][destination].items():
                    label = origin + "_" + destination
                    if pnr not in count_dict:
                        count_dict[pnr] = []
                    else:
                        count_dict[pnr].append(label)
    #Now print out the number of SPs that pass through each PNR
    for row_pnr, row_list in count_dict.items():
        print(row_pnr, len(row_list))
#Write the shortest paths dictionary to disk.
def writeSP(spDict):
    print("Writing shortest paths Result file...")
    for origin, outter in spDict.items():
        for deptime, inner in spDict[origin].items():
            for destination, locations in spDict[origin][deptime].items():
                for pnr, tt in spDict[origin][deptime][destination].items():
                    entry = {'origin': origin, 'deptime': deptime, 'destination': destination, 'PNR': pnr, 'traveltime': tt}
                    writer.writerow(entry)
    print("Shortest Paths Results File Written")


#################################
#           OPERATIONS          #
#################################


if __name__ == '__main__':

    start_time, curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-pnr', '--PNRLIST_FILE', required=True, default=None)
    parser.add_argument('-deptimes', '--DEPTIMES_FILE', required=True, default=None)
    args = parser.parse_args()

    #Read in the PNR_list file
    pnr_list = readList(args.PNRLIST_FILE)

    #Read in the deptimeList file
    deptimeList = readList(args.DEPTIMES_FILE)

    #Create a new inner dict structure
    nest1 = lambda:defaultdict(dict)
    nest2 = lambda: defaultdict(nest1)
    #Initiate all_paths_dict, quadruple nested
    allPathsDict = defaultdict(nest2)

    #Initiate shortest paths dict, quadruple nested
    # spDict = defaultdict(lambda: defaultdict(defaultdict(dict)) )
    spDict = defaultdict(nest2)

    #Then loop through the list and perform the following lines of code for each PNR...
    for pnr in pnr_list:
        #Make nested input dictionaries
        p2oDict = makeNestedDict(pnr,'origin', 'destination', 'origin')
        p2dDict = makeNestedDict(pnr, 'destination', 'origin', 'destination')

        #Map destinations to 15th percentile TT in 15 minute bins
        p2d15Dict = makeCentileDict(p2dDict, deptimeList)
        #print("p2d15dict = ", p2d15Dict)
        #Use the origin dict to calculate depsums and log in a new dict
        depsumDict = makeDepsumDict(p2oDict)
        #print('depsum dict = ', depsumDict)
        #Break out PNR from inner dict
        for key, value in p2oDict.items():
            #Loop through origin set attached to selected PNR
            time_start = time.time()
            count = 0
            for orgn, inner in p2oDict[key].items():
                count += 1
                #Link all possible paths for each origin + PNR combo
                linkPaths(orgn, pnr, p2d15Dict, depsumDict) #LinkPaths sends to AllPathsDict function
                time_end = time.time()
                if count == 1:
                    iter_runtime = time_end - time_start
                    print("Approximate time to connect all origins (sec) = ", (54000*iter_runtime))
            runtime = time.time() - start_time
            #This prints each time a PNR has been connected to all origins and destinations
            print("Paths linked through PNR: {} Runtime = ".format(pnr), runtime )
            #print("All paths dict=", allPathsDict)


    #Now that all PNRs have been considered, find the shortest paths between the OD set.
    findSP(deptimeList, allPathsDict)
    countPNRS(spDict)
    #Initiate the linked paths file.
    fieldnames = ['origin', 'deptime', 'destination', 'PNR', 'traveltime']
    writer = mkOutput(curtime, fieldnames, 'paths_linked')
    writeSP(spDict)
    elapsedTime()


# #Make a dictReader object for each input file, one for origin to PNR and another for PNR to destination
# def readFiles(pnr):
#     #Glob module is used to grab the file name despite the curtime extension from matrixBreaker
#     #with open(glob.glob(name_string*".txt"), 'r') as input_or:
#
#     # input_or = open('PNR_{}_origin.txt'.format(pnr), 'r')
#     # reader_or = csv.DictReader(input_or)
#     # input_or.close()
#     with open ('PNR_{}_origin.txt'.format(pnr), 'r') as input_or:
#         reader_or = csv.DictReader(input_or)
#     #
#     # input_dest = open('PNR_{}_destination.txt'.format(pnr), 'r')
#     # reader_dest = csv.DictReader(input_dest)
#     # input_dest.close()
#     with open('PNR_{}_destination.txt'.format(pnr), 'r') as input_dest:
#         reader_dest = csv.DictReader(input_dest)
#
#     return reader_or, reader_dest

#     #CATCH: Do not touch rows where destinations cannot be reached.
#     # if row['traveltime'] == 2147483647:
#     #     next(reader)
#     if row[outter_val] not in nest:
#         #Initiate inner dict
#         nest[row[outter_val]] = {}
#         if row[inner_val] not in nest[row[outter_val]]:
#             nest[row[outter_val]][row[inner_val]] = {}
#             if row['deptime'] not in nest[row[outter_val]][row[inner_val]]:
#                 nest[row[outter_val]][row[inner_val]][row['deptime']] = row['traveltime']
#             count += 1
#
#     elif row[inner_val] not in nest[row[outter_val]]:
#         nest[row[outter_val]][row[inner_val]] = {}
#         if row['deptime'] not in nest[row[outter_val]][row[inner_val]]:
#             nest[row[outter_val]][row[inner_val]][row['deptime']] = row['traveltime']
#         count += 1
#
#     elif row['deptime'] not in nest[row[outter_val]][row[inner_val]]:
#         nest[row[outter_val]][row[inner_val]][row['deptime']] = row['traveltime']
#
# time_est = (count * .91)/60
# if name == 'destination':
#     print("Number of {} to connect = ".format(name), count)
#     print("Estimated all paths connection time = ", time_est)
# else:
#     print("Number of {} to connect = ".format(name), count)

#Return a list of departure times for which each OD pair may have different total travel times based on
#the departure time from the origin.
# def makeList(p2o_dict):
#     list = []
#     for pnr, origins in p2o_dict.items():
#         for origin, timing in p2o_dict[pnr].items():
#             for deptime, tt in p2o_dict[pnr][origin].items():
#                 if deptime not in list:
#                     list.append(deptime)
#     print("All possible departure times list:", list)
#     return list

#
# #User information.
# def countDest(dest_dict):
#     count = 0
#     for dest in dest_dict:
#         #Find a deptime and TT that fulfill the requirements below for the selected destination.
#         for dest_deptime, dest_traveltime in dest_dict[dest].items():
#             if int(dest_traveltime) <= int(centileDict[dest]) and int(dest_traveltime) != 2147483647:
#                 count += 1
#     print("Modified set of destinations = ", count)

#if origin not in allPathsDict:
#     allPathsDict[origin] = {}
#
#     if destination not in allPathsDict[origin]:
#         allPathsDict[origin][destination] = {}
#
#         if PNR not in allPathsDict[origin][destination]:
#             allPathsDict[origin][destination][PNR] = {}
#
#             if or_deptime not in allPathsDict[origin][destination][PNR]:
#                 allPathsDict[origin][destination][PNR][or_deptime] = path_TT
#
# elif destination not in allPathsDict[origin]:
#     allPathsDict[origin][destination] = {}
#
#     if PNR not in allPathsDict[origin][destination]:
#         allPathsDict[origin][destination][PNR] = {}
#
#         if or_deptime not in allPathsDict[origin][destination][PNR]:
#             allPathsDict[origin][destination][PNR][or_deptime] = path_TT
#
# elif PNR not in allPathsDict[origin][destination]:
#     allPathsDict[origin][destination][PNR] = {}
#
#     if or_deptime not in allPathsDict[origin][destination][PNR]:
#         allPathsDict[origin][destination][PNR][or_deptime] = path_TT
#
# elif or_deptime not in allPathsDict[origin][destination][PNR]:
#     allPathsDict[origin][destination][PNR][or_deptime] = path_TT
#
# else:
#     allPathsDict[origin][destination][PNR][or_deptime] = path_TT

# if origin not in spDict:
#     spDict[origin] = {}
#
#     if dptm not in spDict[origin]:
#         spDict[origin][dptm] = {}
#
#         if destination not in spDict[origin][dptm]:
#             spDict[origin][dptm][destination] = {}
#
#             if pnr not in spDict[origin][dptm][destination]:
#                 spDict[origin][dptm][destination][pnr] = sp_tt
#
# elif dptm not in spDict[origin]:
#         spDict[origin][dptm] = {}
#
#         if destination not in spDict[origin][dptm]:
#             spDict[origin][dptm][destination] = {}
#
#             if pnr not in spDict[origin][dptm][destination]:
#                 spDict[origin][dptm][destination][pnr] = sp_tt
#
# elif destination not in spDict[origin][dptm]:
#             spDict[origin][dptm][destination] = {}
#
#             if pnr not in spDict[origin][dptm][destination]:
#                 spDict[origin][dptm][destination][pnr] = sp_tt

#Make a feasible destination dictionary where travel time for each destination and deptime combo is compared against
# #its 15th percentile value and discarded if it doesn't meet the criteria.
# def viableDest(dest_dict, centile_dict):
#     feasible_dest = defaultdict(lambda: defaultdict(int) )
#     for dest in dest_dict:
#         # Find a deptime and TT that fulfill the requirements below for the selected destination.
#         for dest_deptime, dest_traveltime in dest_dict[dest].items():
#             if int(dest_traveltime) <= int(centile_dict[dest]) and int(dest_traveltime) != 2147483647:
#                 feasible_dest[dest][dest_deptime] = dest_traveltime
#     return feasible_dest

