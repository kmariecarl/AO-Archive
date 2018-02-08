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

#Second, the incoming transit TT matrix needs to be aggregated up to the same resolution as the auto TT matrix, in
#this case, 15 minutes.


#Wait time at the PNR for transit service is accounted for by selecting the 15th percentile of travel times
# which more accurately reflects how PNR users arrive at PNR stations --the minimize their wait time with some buffer time
# thus the 15th percentile is used.

#Auto TT matrix resolution should correspond with linking PNR2D matrix criteria, i.e. 15 min deptimes means 15 min buffer.
#Auto TT does not use percentile to narrow down values

#ASSUMPTIONS: Use the 15th percentile of travel times. Origin to PNR auto trips are less than 30 minutes.
# Transfer window is 15 minutes.

#EXAMPLE USAGE: kristincarlson$ python TTMatrixLink.py -pnr PNRList_xxx.txt -deptimes Deptimes_xxx.txt


#################################
#           IMPORTS             #
#################################
import argparse
import csv
import time
from collections import defaultdict

import matrixLinkModule as mod


#################################
#           FUNCTIONS           #
#################################


#A dictionary is the product of this function
#{origin:[traveltime,depsum,depsum_bin],
#{origin:[traveltime,depsum,depsum_bin],
#{origin:[traveltime,depsum,depsum_bin],
#{origin:[traveltime,depsum,depsum_bin]}

#A function that is called for every PNR and departure time combination.
def makeOriginDict(prn, deptime, or_dep_list_sort, name_key, auto_limit):
    print('Open PNR_{}_{}_{}.txt'.format(pnr, deptime, name_key))
    with open ('PNR_{}_{}_{}.txt'.format(pnr, deptime, name_key), 'r') as input:
        reader = csv.DictReader(input)

        origins = {}
        for row in reader:
            if int(row['traveltime']) <= auto_limit:
                depsum = int(deptime) + int(row['traveltime'])
                #Assign depsum to the 15 minute bin
                for index, dptm in enumerate(or_dep_list_sort):
                    next = index + 1
                    if next < len(or_dep_list_sort):
                        if depsum > int(dptm) and depsum <= int(or_dep_list_sort[next]):

                            #Depsum_bin is in the form of seconds!
                            depsum_bin = int(or_dep_list_sort[next])
                            #All values are integers
                            origins[row['origin']] = [int(row['deptime']), int(row['traveltime']), depsum, depsum_bin]
    return origins

#A function that is called for every PNR and departure time combination.
def makeDestDict(pnr, name_key):
    # Initiate the triple nested dict structure
    nest = defaultdict(lambda: defaultdict(int))
    #Open the aggregated destination file. The destination file is NOT broken by Deptime, all need to be listed in order
    #to link with origin depsum_bins.
    with open ('PNR15_{}_{}.txt'.format(pnr, name_key), 'r') as input:
        reader = csv.DictReader(input)
        for row in reader:

            #Destinations cannot be more than 90 minutes away
            if int(row['traveltime']) <= 5400:
                #Name_key will correspond with the destination ID, usually a GeoID
                nest[int(row['deptime'])][row[name_key]] = int(row['traveltime'])

    print("Created Nested Dictionary:", name_key)
    return nest

#This function looks at the combo of origin + dest + PNR + deptime handed to it, and adds it to a 4x nested dict (if
#criteria are me) in the following structure:

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

def linkPaths(orgn, pnr, dest, timing_list, tt):
    #Transfer = depsum_bin - depsum

    transfer = timing_list[3] - timing_list[2]
    #path_tt = origin_tt + destination_tt + transfer

    path_TT = timing_list[1] + tt + transfer

    if path_TT <= 5400:
        #If the total path time is less than 90 minutes, add the path to the allPathsDict
        #timing_list[0] = or_deptime
        #This process does not write over previous paths if a shorter path is found. This could be implemented later.
        allPathsDict[orgn][dest][pnr][int(timing_list[0])] = path_TT


#Map deptimes to origins for p2o dict which will remove a dict layer. Then I can write results to disk by
#departure time.
#Take the all_paths_dict and find the shortest path between all OD pairs.
#The shortest path for an origin + dest + deptime combo can vary by PNR
def findSP(or_dep_list_sort, all_paths_dict):
    print("Finding shortest paths between OD pairs")
    #Each "row" is a separate origin
    for origin, outter in all_paths_dict.items():
        #Maybe try saying: for destination in outter:
        for destination, inner in all_paths_dict[origin].items():

            #now use a method to select one deptime and compare across all PNRs and then for each departure time
            #you may get that different PNR result in the shortest travel time.
            for dptm in or_dep_list_sort:

                dptm_dict = {} #where key=PNR: value=TT
                #For each deptime make a dictionary of all PNRs: tt, once the dict is created, run min().
                #Try: for PNR in inner:
                for PNR, timing in all_paths_dict[origin][destination].items():
                    print('my pnr:', PNR)
                    print('my dptim:', dptm)
                    print('info:', all_paths_dict[origin][destination][PNR].keys())
                    if dptm in all_paths_dict[origin][destination][PNR].keys():
                        print('hi again')
                        dptm_dict[PNR] = all_paths_dict[origin][destination][PNR][dptm]

                    #If the particular OD + PNR combo does not have a path in the allPathsDict for the selected
                    #departure time, then move to the next PNR -- not to the next if statement
                #Check if there are values in the dptm_dict for the particular Or_dest_dptm combo
                if any(dptm_dict) == True:
                    print('Aloha')
                    minPNR = min(dptm_dict, key=dptm_dict.get)

                    minTT = dptm_dict[minPNR]

                    print("SP between", origin, "and", destination, "uses PNR", minPNR, "TT=", minTT)
                    spDict[origin][dptm][destination][minPNR] = minTT
        #print("Origin {} connected to PNRs and Destinations in {} seconds".format(origin, rep_runtime))
    print("Shortest Paths Are Found")

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
                    entry = [origin, deptime, destination, pnr, tt]
                    writer.writerow(entry)
    print("Shortest Paths Results File Written")


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    START_TIME, curtime = mod.startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-pnr', '--PNRLIST_FILE', required=True, default=None)
    parser.add_argument('-or_dep', '--OR_DEP_FILE', required=True, default=None)
    parser.add_argument('-lim', '--AUTO_LIMIT', required=True, default=None) #Limit auto trip to 30 min: type 1800
    parser.add_argument('-calctime', '--CALCTIME_VALUE', required=False, default=None) #ENTER AS '0700' FOR 7:00
    args = parser.parse_args()

    #Read in the PNR_list file
    pnr_list = mod.readList(args.PNRLIST_FILE)

    #Read in the deptimeList file. Comes in random order like ['26100', '25200', '29700',..]
    orDepList = mod.readList(args.OR_DEP_FILE, str)

    #Sort in ascending order
    orDepListSort = sorted(orDepList)
    #In place changes list values from string to integer
    orDepListSort = list(map(int, orDepListSort))
    print("Sorted deptime list", orDepListSort)

    #Assign Auto Limit to integer variable
    autoLimit = int(args.AUTO_LIMIT)

    #Create a new inner dict structure
    nest1 = lambda:defaultdict(dict)
    nest2 = lambda: defaultdict(nest1)
    #Initiate all_paths_dict, quadruple nested
    allPathsDict = defaultdict(nest2)

    #Initiate shortest paths dict, quadruple nested
    spDict = defaultdict(nest2)

    #Then loop through the PNR and deptime lists and perform the following lines of code...
    for pnr in pnr_list:
        #For each deptime...
        for deptime in orDepListSort:
            #Make nested input dictionaries
            originDict = makeOriginDict(pnr, deptime, orDepListSort,'origin', autoLimit)

            destDict = makeDestDict(pnr, 'destination')
            #Timing is a list NOT a dictionary
            for orgn, timing in originDict.items():

                depsum_bin = int(timing[3])
                for dest, tt in destDict[depsum_bin].items():


                    linkPaths(orgn, pnr, dest, timing, tt)
        print('-----------------------------------------------------------')

    #Now that all PNRs have been considered, find the shortest paths between the OD set.
    findSP(orDepListSort, allPathsDict)
    countPNRS(spDict)
    #Initiate the linked paths file.
    fieldnames = ['origin', 'deptime', 'destination', 'PNR', 'traveltime']
    writer = mod.mkOutput('paths_linked', fieldnames)
    writeSP(spDict)
    mod.elapsedTime(START_TIME)
