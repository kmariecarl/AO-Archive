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

#Despite the refactoring to segment processes by PNR, I am maintainin the data structure format where the PNR stays as
#the outter dict key even though that means there will only be one outter dict key.

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

def mkOutput(currentTime, fieldnames, name):
    outfile = open('output_{}_{}.txt'.format(name,curtime), 'w')
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    return writer

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


#A triple nested dictionary is the product of this function
#{origin:{destination:{deptime:traveltime,
#                      deptime:traveltime,
#                      deptime:traveltime},
#        {destination:{deptime:traveltime,
#                      deptime:traveltime,
#                      deptime:traveltime}}
#origins:...
def makeNestedDict(pnr, name, outter_val, inner_val):
    with open ('PNR_{}_{}.txt'.format(pnr, name), 'r') as input:
        reader = csv.DictReader(input)
        #
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
# def makeList(p2o_dict):
#     list = []
#     for pnr, origins in p2o_dict.items():
#         for origin, timing in p2o_dict[pnr].items():
#             for deptime, tt in p2o_dict[pnr][origin].items():
#                 if deptime not in list:
#                     list.append(deptime)
#     print("All possible departure times list:", list)
#     return list
#Check if list contains all Maxtime values (all the same values)
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
def linkPaths(key_PNR, p2d_dict, p2o_dict):
    #Extract the inner dict with matching PNR from the p2o dict.
    print('Connecting paths to PNR#', key_PNR)
    for or_key, orgn_dict in p2o_dict.items(): #Breaking the PNR key out from the values dictionary, we don't need to do
        #anything with the key because there is only one. Same for destination dictionary.
        for dest_key, dest_dict in p2d_dict.items():
            #print('Dest_key', dest_key)
            #print('dest_dict', dest_dict)
            #print('or_key and dest_key should match', or_key, dest_key)
        #3. Iterate through the different destinations for the PNR that has been selected
            dest_list = [i for i in dest_dict.keys()]
            #print('dest_list should be geoid10', dest_list)
            for dest in dest_list:
                #Find 15th percentile TT for this destination
                dest_tile = calcPercentile(dest_dict, dest)
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
                                #print('here1')
                                #Make sure that O2PNR and PNR2D paths are within 15 minutes of PNR deptime.
                                windowMin = convert2Sec(dest_deptime)
                                #windowMax = convert2Sec(dest_deptime) + 900
                                #print('min:', windowMin, 'depsum:', depsum) #, 'max', windowMax)
                                if windowMin <= depsum: #<= windowMax:
                                    #print('here2')
                                    path_TT = int(or_traveltime) + int(dest_traveltime)
                                    #7 This path is viable, add to list.
                                    add2AllPathsDict(orgn, dest, key_PNR, or_deptime, path_TT)
                #print("All possible origins connected to destination {} through PNR {}".format(dest, key_PNR))




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
    print("Finding shortest paths between OD pairs")
    #Each "row" is a separate origin
    for origin, outter in all_paths_dict.items():
        print('origin', origin)
        #print('outter', outter)
        prev_time = time.time()
        for destination, inner in all_paths_dict[origin].items():
            print("Destination", destination)
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

                #print("dptm_dict: ", dptm_dict)
                if any(dptm_dict) == True:
                    #print('here2')
                    #print("Found a OD + PNR combo that doesn't have a SP for the selected departure time")
                    minPNR = min(dptm_dict, key=dptm_dict.get)
                    #print("minPNR: ", minPNR)
                    minTT = dptm_dict[minPNR]
                    #minTT = all_paths_dict[origin][destination][minPNR][dptm]
                    #So far haven't addressed if two PNRs have equal and minimal TTs.
                    print("SP between", origin, "and", destination, "uses PNR", minPNR, "TT=", minTT)
                    #elapsedTime()
                    add2SPDict(origin, dptm, destination, minPNR, minTT)

        rep_runtime = time.time() - prev_time
        print("Origin {} connected to PNRs and Destinations in {} seconds".format(origin, rep_runtime))
    #print('Shortest paths dict:', spDict)
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



def writeSP(spDict):
    print("Writing shortest paths Result file...")
    for origin, outter in spDict.items():
        for deptime, inner in spDict[origin].items():
            for destination, locations in spDict[origin][deptime].items():
                for pnr, tt in spDict[origin][deptime][destination].items():
                    entry = {'origin': origin, 'deptime': deptime, 'destination': destination, 'PNR': pnr, 'traveltime': tt}
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
    parser.add_argument('-pnr', '--PNRLIST_FILE', required=True, default=None)
    parser.add_argument('-deptimes', '--DEPTIMES_FILE', required=True, default=None)
    #parser.add_argument('-o2p', '--O2PNR_FILE', required=True, default=None)
    #parser.add_argument('-p2d', '--PNR2D_FILE', required=True, default=None)
    args = parser.parse_args()

    #To handle large datasets, segment processes by PNR
    #Read in the PNR_list file
    pnr_list = readList(args.PNRLIST_FILE)
    #print("pnr list:", pnr_list)
    #Provide a way to create a list of departure times using matrixBreaker
    #Overarching deptimeList
    deptimeList = readList(args.DEPTIMES_FILE)
    #print('deptime list', deptimeList)

    #Initiate all_paths_dict
    allPathsDict ={}
    #Initiate shortest paths dict
    spDict = {}

    #Then loop through the list and perform the following lines of code for each PNR...
    for pnr in pnr_list:
        #or_dict, dst_dict = readFiles(pnr) #remeber to close these files at thge end of iteration
        #Make nested input dictionaries
        p2oDict = makeNestedDict(pnr,'origin', 'destination', 'origin')
        p2dDict = makeNestedDict(pnr, 'destination', 'origin', 'destination')

        #1. Grab a PNR connected to destinations dict
        #for key_PNR, dest_dict in p2dDict.items():
            #2. Grab the same PNR connected to origins and link paths if criteria are met
            #linkPaths(key_PNR, dest_dict, p2oDict)
        linkPaths(pnr, p2dDict, p2oDict) #LinkPaths sends to AllPathsDict function
        runtime = time.time() - start_time
        #This prints each time a PNR has been connected to all origins and destinations
        print("Paths linked. Runtime =", runtime )
        #Close PNR files


    findSP(deptimeList, allPathsDict)
    for row, value in spDict.items():
        print(row, value)
    countPNRS(spDict)
    #Initiate the linked paths file.
    fieldnames = ['origin', 'deptime', 'destination', 'PNR', 'traveltime']
    writer = mkOutput(curtime, fieldnames, 'paths_linked')
    writeSP(spDict)
    elapsedTime()