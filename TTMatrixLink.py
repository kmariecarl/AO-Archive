#This script links TT matrix data for an intermediate destination (park-and-rides in this case), in order to get a single travel time
#value from origin to destination

#LOGIC:
#1. Select Park-and-Ride
#2. Match origins dict to destinations dict using common PNR key.
#3. Create a dictionary containing all viable paths
#4. Narrow the all_paths_dict to shortest paths for all OD pairs.
#5. Print the shortest paths dictionary to file.


#NOTES:
#Consider mainting a counter of the number of times each PNR was found to be in the shortest path

#By finding the pnr that results in the sp for each OD pair, there is potential for back-tracking
#  from the origin to PNR.

#Wait time at the PNR for transit service is accounted for by the PNR2D TT matrix whereby averaging the
# travel times from O to D across departure times, you have captured half of the headway of each route.


#EXAMPLE USAGE: kristincarlson$ python TTMatrixLink.py -o2p o2pnr.txt -p2d pnr2d.txt

#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse

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
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return currentTime

def mkOutput(currentTime, fieldnames):
    outfile = open('output_{}.txt'.format(curtime), 'w')  # , newline=''
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)  # , quotechar = "'"
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

    #print(nest)
    return nest

def makeList(p2o_dict):
    list = []
    for pnr, origins in p2o_dict.items():
        for origin, timing in p2o_dict[pnr].items():
            for deptime, tt in p2o_dict[pnr][origin].items():
                if deptime not in list:
                    list.append(deptime)
    print("Deptime list:", list)
    return list


#Every use of this function accounts for all paths connecting through the selected PNR.
#select PNR -> select destination -> select origin -> select destination departure time
def linkPaths(key_PNR, dest_dict, p2o_dict):
    #Extract the inner dict with matching PNR from the p2o dict.
    orgn_dict = p2o_dict[key_PNR]
    #3. Iterate through the different destination for the PNR that has been selected
    dest_list = [i for i in dest_dict.keys()]
    for dest in dest_list:
        #4. Iterate through origin paths for the selected PNR + destination path selected by outter for loop
        origin_list = [k for k in orgn_dict.keys()]
        for orgn in origin_list:
            for or_deptime, or_traveltime in orgn_dict[orgn].items():
                # 5. Check that origin deptime + tt ~= destination deptime
                depsum = convert2Sec(or_deptime) + int(or_traveltime)
                #6. Create PNR transfer window of 10 minutes
                for dest_deptime, dest_traveltime in dest_dict[dest].items():
                    windowMin = convert2Sec(dest_deptime) - 300
                    windowMax = convert2Sec(dest_deptime) + 300
                    if windowMin <= depsum <= windowMax:
                        path_TT = int(or_traveltime) + int(dest_traveltime)
                        #7 This path is viable, add to list.
                        print(orgn, dest, key_PNR, or_deptime, path_TT)
                        print("send to all_paths_dict")
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

def add2AllPathsDict(origin, destination, PNR, or_deptime, path_TT):
    if origin not in allPathsDict:
        allPathsDict[origin] = {}
        print('1')
        if destination not in allPathsDict[origin]:
            allPathsDict[origin][destination] = {}
            print('2')
            if PNR not in allPathsDict[origin][destination]:
                allPathsDict[origin][destination][PNR] = {}
                print('3')
                if or_deptime not in allPathsDict[origin][destination][PNR]:
                    allPathsDict[origin][destination][PNR][or_deptime] = path_TT
                    print('4')
                    print(origin, destination, PNR, or_deptime, path_TT)


    elif destination not in allPathsDict[origin]:
        allPathsDict[origin][destination] = {}
        print('5')
        if PNR not in allPathsDict[origin][destination]:
            allPathsDict[origin][destination][PNR] = {}
            print('6')
            if or_deptime not in allPathsDict[origin][destination][PNR]:
                allPathsDict[origin][destination][PNR][or_deptime] = path_TT
                print('7')
                print(origin, destination, PNR, or_deptime, path_TT)


    elif PNR not in allPathsDict[origin][destination]:
        allPathsDict[origin][destination][PNR] = {}
        print('8')
        if or_deptime not in allPathsDict[origin][destination][PNR]:
            allPathsDict[origin][destination][PNR][or_deptime] = path_TT
            print('9')
            print(origin, destination, PNR, or_deptime, path_TT)
    elif or_deptime not in allPathsDict[origin][destination][PNR]:
        allPathsDict[origin][destination][PNR][or_deptime] = path_TT
        print('10')
        print(origin, destination, PNR, or_deptime, path_TT)
    #Need to solve the issue with paths that hold the same info but different TTs that will not also be added to all-paths_dict
    else:
        allPathsDict[origin][destination][PNR][or_deptime] = path_TT
        print('10')
        print(origin, destination, PNR, or_deptime, path_TT)

#Take the all_paths_dict and find the shortest path between all OD pairs.
def findSP(deptime_list, all_paths_dict):
    #Each "row" is a separate origin
    for origin, outter in all_paths_dict.items():
        #print('all_paths_dict row:  ', origin, outter)

        for destination, inner in all_paths_dict[origin].items():
            #print('destin. and inner:', destination, inner)
            #now use a method to select one deptime and compare across all PNRs and then for each departure time
            #you may get that different PNR result in the shortest travel time.
            for dptm in deptime_list:
                dptm_dict = {} #where key=PNR: value=TT
                #for each deptime make a dictionary of all PNRs: tt, once the dict is created, run min().
                for PNR, timing in all_paths_dict[origin][destination].items():
                    dptm_dict[PNR] = all_paths_dict[origin][destination][PNR][dptm]
                print("dptm_dict: ", origin, destination, dptm_dict)

                minPNR = min(dptm_dict, key=dptm_dict.get)
                print("minPNR: ", minPNR)
                minTT = dptm_dict[minPNR]
                #minTT = all_paths_dict[origin][destination][minPNR][dptm]
                #So far haven't addressed if two PNRs have equal and minimal TTs.
                add2SPDict(origin, destination, dptm, minPNR, minTT)
                #Do something to collect sp PNRs for record to print out

def add2SPDict(origin, destination, dptm, pnr, sp_tt):
    if origin not in spDict:
        spDict[origin] = {}
        print('VALUE ADDED1')
        if destination not in spDict[origin]:
            spDict[origin][destination] = {}
            print('VALUE ADDED2')
            if dptm not in spDict[origin][destination]:
                spDict[origin][destination][dptm] = {}
                print('VALUE ADDED3')
                if pnr not in spDict[origin][destination][dptm]:
                    spDict[origin][destination][dptm][pnr] = sp_tt
                    print('VALUE ADDED4')
    elif destination not in spDict[origin]:
        spDict[origin][destination] = {}
        print('VALUE ADDED5')
        if dptm not in spDict[origin][destination]:
            spDict[origin][destination][dptm] = {}
            print('VALUE ADDED6')
            if pnr not in spDict[origin][destination][dptm]:
                spDict[origin][destination][dptm][pnr] = sp_tt
                print('VALUE ADDED7')
    elif dptm not in spDict[origin][destination]:
        spDict[origin][destination][dptm] = {}
        print('VALUE ADDED8')
        if pnr not in spDict[origin][destination][dptm]:
            spDict[origin][destination][dptm][pnr] = sp_tt
            print('VALUE ADDED9')
    # elif pnr not in spDict[origin][destination][dptm]:
    #     spDict[origin][destination][dptm][pnr] = sp_tt
    #     print('VALUE ADDED10')
    # else:
    #     spDict[origin][destination][dptm][pnr] = sp_tt
    #     print('VALUE ADDED5')
def countPNRS(spDict):
    #Create a dictionary to store PNRS with the OD pairs that use each PNR in their SP.
    count_dict = {}
    for origin, outter in spDict.items():
        for destination, inner in spDict[origin].items():
            for deptime, locations in spDict[origin][destination].items():
                for pnr, tt in spDict[origin][destination][deptime].items():
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
        for destination, inner in spDict[origin].items():
            for deptime, locations in spDict[origin][destination].items():
                for pnr, tt in spDict[origin][destination][deptime].items():
                    entry = {'origin': origin, 'destination': destination, 'deptime': deptime, 'PNR': pnr, 'minTT': tt}
                    print(entry)
                    writer.writerow(entry)


#################################
#           OPERATIONS          #
#################################


if __name__ == '__main__':

    curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-o2p', '--O2PNR_FILE', required=True, default=None)
    parser.add_argument('-p2d', '--PNR2D_FILE', required=True, default=None)
    args = parser.parse_args()

    fieldnames = ['origin', 'destination', 'deptime', 'PNR', 'minTT']
    writer = mkOutput(curtime, fieldnames)

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
    findSP(deptimeList, allPathsDict)
    countPNRS(spDict)
    writeSP(spDict)
