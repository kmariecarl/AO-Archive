#This script links TT matrix data for an intermediate destination (park-and-rides in this case), in order to get a single travel time
#value from origin to destination

#LOGIC:
#1. Select Park-and-Ride
#2. Match origins dict to destinations dict using common PNR key.
#3. Create a dictionary containing all viable paths
#4. Narrow the all_paths_dict to shortest paths for all OD pairs.
#5. Print the shortest paths dictionary to file.


#NOTES:

#In order to use this script, TT by auto from O to PNR, TT by transit from PNR to D, must be calculated.


#Wait time at the PNR for transit service is accounted for by selecting the 15th percentile of travel times
# which more accurately reflects how PNR users arrive at PNR stations --the minimize their wait time with some buffer time
# thus the 15th percentile is used.

#Auto TT matrix resolution should correspond with linking PNR2D matrix criteria, i.e. 15 min deptimes means 15 min buffer.
#Auto TT does not use percentile to narrow down values

#Program assumes that each PNR is associated with all destinations whether or not the destinations can be reachec
#or not in the given travel time.

#EXAMPLE USAGE: kristincarlson$ python TTMatrixLink.py -o2p o2pnr.txt -p2d pnr2d.txt

#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
import numpy
import timeit

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

def startTimer():
    # Start timing
    #Use start_time for tracking elapsed runtime.
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return start_time, currentTime

#A function that prints out the elapsed calculation time
def elapsedTime():
    elapsed_time = time.time() - start_time
    print("Elapsed Time: ", elapsed_time)

def mkOutput(currentTime, fieldnames, name):
    outfile = open('output_{}_{}.txt'.format(name,curtime), 'w')
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    return writer

#Make a dictReader object from input file
#A triple nested dictionary is the product of this function
#{origin:{destination:{deptime:traveltime,
#                      deptime:traveltime,
#                      deptime:traveltime},
#        {destination:{deptime:traveltime,
#                      deptime:traveltime,
#                      deptime:traveltime}}
#origins:...

def makeNestedDict(file, outter_val, inner_val):
    input = open(file, 'r')
    reader = csv.DictReader(input)
    #Initiate outter dict
    nest = {}
    for row in reader:
        if row[outter_val] not in nest:
            #Initiate inner dict
            nest[row[outter_val]] = {}
            if row[inner_val] not in nest[row[outter_val]]:
                nest[row[outter_val]][row[inner_val]] = {}
                if row['deptime'] not in nest[row[outter_val]][row[inner_val]]:
                    nest[row[outter_val]][row[inner_val]][row['deptime']] = row['traveltime']

        elif row[inner_val] not in nest[row[outter_val]]:
            nest[row[outter_val]][row[inner_val]] = {}
            if row['deptime'] not in nest[row[outter_val]][row[inner_val]]:
                nest[row[outter_val]][row[inner_val]][row['deptime']] = row['traveltime']

        elif row['deptime'] not in nest[row[outter_val]][row[inner_val]]:
                nest[row[outter_val]][row[inner_val]][row['deptime']] = row['traveltime']
    print("Created Nested Dictionary:", outter_val)
    return nest

#Return a list of departure times for which each OD pair may have different total travel times based on
#the departure time from the origin.
def makeList(p2o_dict):
    list = []
    for pnr, origins in p2o_dict.items():
        for origin, timing in p2o_dict[pnr].items():
            for deptime, tt in p2o_dict[pnr][origin].items():
                if deptime not in list:
                    list.append(deptime)
    print("All possible departure times list:", list)
    return list
#Check if list contains all the same values
def allSame(items):
    return all(x == items[0] for x in items)

#This function calculates the bottom 15th percentile travel time ~= 85th percentile for a particular OD Pair
def calcPercentile(dest_dict, dest):
    tt_list = []
    for deptime, tt in dest_dict[dest].items():
        tt_list.append(int(tt))
    if allSame(tt_list) is True:
        tile = 2147483647
    else:
        tile = numpy.percentile(tt_list, 15)
    return int(tile)


#Every use of this function accounts for all paths connecting through the selected PNR.
#select PNR -> select destination -> select origin -> select destination departure time
def linkPaths(key_PNR, dest_dict, p2o_dict):
    #Extract the inner dict with matching PNR from the p2o dict.
    orgn_dict = p2o_dict[key_PNR]
    #print('orgn_dict: ', orgn_dict)
    #3. Iterate through the different destination for the PNR that has been selected
    dest_list = [i for i in dest_dict.keys()]
    for dest in dest_list:
        #print("dest: ", dest)
        #Find 15th percentile TT for this destination
        dest_tile = calcPercentile(dest_dict, dest)
        #print("PNR:", key_PNR, "Destination:", dest, "Percentile:", dest_tile)
        #4. Iterate through origin paths for the selected PNR + destination path selected by outter for loop
        origin_list = [k for k in orgn_dict.keys()]
        for orgn in origin_list:
            for or_deptime, or_traveltime in orgn_dict[orgn].items():
                # 5. Check that origin deptime + tt ~= destination deptime
                depsum = convert2Sec(or_deptime) + int(or_traveltime)
                #6. Create PNR transfer window of 10 minutes
                for dest_deptime, dest_traveltime in dest_dict[dest].items():
                    # Check if path TT is in the 85th percentile of lowest TTs before
                    # checking that the deptime is within 5 min of depsum.
                    # Make sure to exclude paths where the destination is actually not reachable by the PNR
                    if int(dest_traveltime) <= dest_tile and int(dest_traveltime) != 2147483647:
                        #Make sure that O2PNR and PNR2D paths are within 15 minutes of PNR deptime.
                        windowMin = convert2Sec(dest_deptime)
                        windowMax = convert2Sec(dest_deptime) + 900
                        if windowMin <= depsum <= windowMax:
                            path_TT = int(or_traveltime) + int(dest_traveltime)
                            #7 This path is viable, add to list.
                            #print("Origin:",orgn, "PNR:", key_PNR, "Dest:", dest, "Deptime:", or_deptime, "TT:", path_TT)
                            #elapsedTime()
                            #print("send to all_paths_dict")
                            add2AllPathsDict(orgn, dest, key_PNR, or_deptime, path_TT)




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
def add2AllPathsDict(origin, destination, PNR, or_deptime, path_TT):
    if origin not in allPathsDict:
        allPathsDict[origin] = {}

        if destination not in allPathsDict[origin]:
            allPathsDict[origin][destination] = {}

            if PNR not in allPathsDict[origin][destination]:
                allPathsDict[origin][destination][PNR] = {}

                if or_deptime not in allPathsDict[origin][destination][PNR]:
                    allPathsDict[origin][destination][PNR][or_deptime] = path_TT

    elif destination not in allPathsDict[origin]:
        allPathsDict[origin][destination] = {}

        if PNR not in allPathsDict[origin][destination]:
            allPathsDict[origin][destination][PNR] = {}

            if or_deptime not in allPathsDict[origin][destination][PNR]:
                allPathsDict[origin][destination][PNR][or_deptime] = path_TT

    elif PNR not in allPathsDict[origin][destination]:
        allPathsDict[origin][destination][PNR] = {}

        if or_deptime not in allPathsDict[origin][destination][PNR]:
            allPathsDict[origin][destination][PNR][or_deptime] = path_TT

    elif or_deptime not in allPathsDict[origin][destination][PNR]:
        allPathsDict[origin][destination][PNR][or_deptime] = path_TT

    else:
        allPathsDict[origin][destination][PNR][or_deptime] = path_TT


#Take the all_paths_dict and find the shortest path between all OD pairs.
def findSP(deptime_list, all_paths_dict):
    #Each "row" is a separate origin
    for origin, outter in all_paths_dict.items():
        for destination, inner in all_paths_dict[origin].items():
            #now use a method to select one deptime and compare across all PNRs and then for each departure time
            #you may get that different PNR result in the shortest travel time.
            for dptm in deptime_list:
                dptm_dict = {} #where key=PNR: value=TT
                #For each deptime make a dictionary of all PNRs: tt, once the dict is created, run min().
                for PNR, timing in all_paths_dict[origin][destination].items():
                    if dptm in all_paths_dict[origin][destination][PNR]:
                        dptm_dict[PNR] = all_paths_dict[origin][destination][PNR][dptm]

                    #print("dptm_dict: ", dptm_dict)
                    if any(dptm_dict) == True:
                        #print("Found a OD + PNR combo that doesn't have a SP for the selected departure time")
                        minPNR = min(dptm_dict, key=dptm_dict.get)
                        #print("minPNR: ", minPNR)
                        minTT = dptm_dict[minPNR]
                        #minTT = all_paths_dict[origin][destination][minPNR][dptm]
                        #So far haven't addressed if two PNRs have equal and minimal TTs.
                        #print("SP between", origin, "and", destination, "uses PNR", minPNR, "TT=", minTT)
                        #elapsedTime()
                        add2SPDict(origin, dptm, destination, minPNR, minTT)
    print("Shortest Paths Are Found")

def add2SPDict(origin, dptm, destination, pnr, sp_tt):
    if origin not in spDict:
        spDict[origin] = {}

        if dptm not in spDict[origin]:
            spDict[origin][dptm] = {}

            if destination not in spDict[origin][dptm]:
                spDict[origin][dptm][destination] = {}

                if pnr not in spDict[origin][dptm][destination]:
                    spDict[origin][dptm][destination][pnr] = sp_tt

    elif dptm not in spDict[origin]:
            spDict[origin][dptm] = {}

            if destination not in spDict[origin][dptm]:
                spDict[origin][dptm][destination] = {}

                if pnr not in spDict[origin][dptm][destination]:
                    spDict[origin][dptm][destination][pnr] = sp_tt

    elif destination not in spDict[origin][dptm]:
                spDict[origin][dptm][destination] = {}

                if pnr not in spDict[origin][dptm][destination]:
                    spDict[origin][dptm][destination][pnr] = sp_tt

def countPNRS(spDict):
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



def writeSP(spDict):
    for origin, outter in spDict.items():
        for deptime, inner in spDict[origin].items():
            for destination, locations in spDict[origin][deptime].items():
                for pnr, tt in spDict[origin][deptime][destination].items():
                    entry = {'origin': origin, 'deptime': deptime, 'destination': destination, 'PNR': pnr, 'minTT': tt}
                    writer.writerow(entry)
    print("Shortest Paths Results File Written")
#Not in use currently
# def writeAvgSP(spDict):
#     for origin, outter in spDict.items():
#         for destination, inner in spDict[origin].items():
#             dest_label = destination
#             tt_list = []
#             for deptime, locations in spDict[origin][destination].items():
#                 for pnr, tt in spDict[origin][destination][deptime].items():
#                     while dest_label == destination:
#                         tt_list.append(tt)
#             or_dest_avg = sum(tt_list)/len(tt_list)
#             entry = {'origin': origin, 'destination': destination, 'avgTT': or_dest_avg}
#             writer2.writerow(entry)
                #elapsedTime()




#################################
#           OPERATIONS          #
#################################


if __name__ == '__main__':

    start_time, curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-o2p', '--O2PNR_FILE', required=True, default=None)
    parser.add_argument('-p2d', '--PNR2D_FILE', required=True, default=None)
    args = parser.parse_args()

    #Create two files, enumerated and averaged by deptime.
    fieldnames = ['origin', 'deptime', 'destination', 'PNR', 'minTT']
    writer = mkOutput(curtime, fieldnames, 'paths_linked')

    # fieldnames2 = ['origin', 'destination', 'avgTT']
    # writer2 = mkOutput(curtime, fieldnames1, 'averaged')

    p2oDict = makeNestedDict(args.O2PNR_FILE, 'destination', 'origin')
    p2dDict = makeNestedDict(args.PNR2D_FILE, 'origin', 'destination')
    deptimeList = makeList(p2oDict)

    #Initiate all_paths_dict
    allPathsDict ={}
    #Initiate shortest paths dict
    spDict = {}

    #1. Grab a PNR connected to destinations dict
    for key_PNR, dest_dict in p2dDict.items():
        #2. Grab the same PNR connected to origins and link paths if criteria are met
        linkPaths(key_PNR, dest_dict, p2oDict)
        runtime = time.time() - start_time
        #This prints each time a PNR has been connected to all origins and destinations
        print("Paths linked. Runtime =", runtime )
    findSP(deptimeList, allPathsDict)
    for row, value in spDict.items():
        print(row, value)
    countPNRS(spDict)
    writeSP(spDict)
