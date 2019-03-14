#THIS SCRIPT WAS ORIGINALLY CREATED FOR THE PNR ACCESSIBILITY PROCESS BUT WAS ABANDONED DUE TO THE CALCULATION TIME.

#REFER TO createMinTT.py 

#This script reads in a succession of travel time files that are broken up by departure time and origin and need to be
#aggregated up to 15 minute bins and where the 15th percentile is applied.

#Output: Series of files separated by origin and deptime and only contain the 15th percentile travel time for each 15 min bin

#EXAMPLE USAGE: kristincarlson$ python createPercentileTT.py -pnr 'PNRList_origin.txt' -or_dep 'Deptimes_origin.txt
#-dest_dep 'Deptimes_destination.txt' -od 'destination'


#################################
#           IMPORTS             #
#################################
import argparse
import csv
from collections import defaultdict
import numpy
from myToolsPackage import matrixLinkModule as mod


#################################
#           FUNCTIONS           #
#################################
#Create double nested dictionary to map destination to 15th percentile TT. Remake every PNR.
#{destination:{deptime:{15thTT,
#              deptime:{15thTT,
#              deptime:{15thTT}}
#destination:...
#This function reads in every PNR_deptime file and creates a nested dictionary for the given PNR
def makeNestedDict(pnr_select, dest_dep_list, connect_field):
    # Initiate the triple nested dict structure
    nest = defaultdict(lambda: defaultdict(int))
    #Add all deptime files to the PNR dict
    for deptime in dest_dep_list:

        with open ('{}PNR_{}_{}.txt'.format(DIR, pnr_select, deptime), 'r') as input:
            reader = csv.DictReader(input)

            for row in reader:
                nest[row[connect_field]][row['deptime']] = row['traveltime']

    print("Created Nested Dictionary for PNR {}".format(pnr_select))
    return nest

#A function to calculate the new travel time in 15 minute bins based on 15th percentile
def makeCentileFile(pnr_select, p2d_dict, or_dep_list, writer):
    #Sort in ascending order
    or_dep_list_sort = sorted(or_dep_list)

    #Break off destinations
    for dest, timing in p2d_dict.items():
        # Cycle through 15 minute departure time list from origin and apply to destinations
        for index, dep15 in enumerate(or_dep_list_sort):
            next = index + 1
            #Make sure not to exceed 15th item in list otherwise error
            if next < len(or_dep_list_sort):
                # Create list to store TT to calc 15th percentile from
                bin15 = []
                #Break out deptimes for each destination
                for deptime, tt in p2d_dict[dest].items():
                    #Check if deptime is within 15 minute bin. Ex. if 6:02 >= 6:00 and 6:02 < 6:15...
                    if deptime >= dep15 and deptime < or_dep_list_sort[next]:
                        bin15.append(int(tt))
                #Calc 15th percentile TT and add to centile_dict
                new_tt = calcPercentile(bin15)
                writer.writerow([pnr_select, dest, dep15, new_tt])

    mod.elapsedTime(START_TIME)

#This function calculates the bottom 15th percentile travel time ~= 85th percentile for a particular OD Pair
def calcPercentile(bin_15):
    if allSame(bin_15) is True:
        tile = 2147483647
    else:
        tile = numpy.percentile(bin_15, 15, interpolation='lower')
    return int(tile)

#Check if list contains all Maxtime values (all the same values)
def allSame(items):
    return all(x == items[0] for x in items)

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    START_TIME, curtime = mod.startTimer()

    parser = argparse.ArgumentParser()
    parser.add_argument('-pnrlist', '--PNRLIST_FILE', required=True, default=None)
    parser.add_argument('-or_dep', '--OR_DEP_FILE', required=True, default=None)
    parser.add_argument('-dest_dep', '--DEST_DEP_FILE', required=True, default=None)
    parser.add_argument('-connect', '--CONNECT_FIELD', required=True, default=None) #Origin or destination can be typed
    parser.add_argument('-dir', '--BREAKER_DIR', required=True, default=None) #Directory where the previously broken files are located
    args = parser.parse_args()

    #Output file fieldnames
    fieldnames = ['origin', 'destination', 'deptime', 'traveltime']

    #May not be necessary to read in arguments to string variables
    PNRListVar = str(args.PNRLIST_FILE)
    orDepVar = str(args.OR_DEP_FILE)
    destDepVar = str(args.DEST_DEP_FILE)
    connectField = str(args.CONNECT_FIELD)
    DIR = str(args.BREAKER_DIR)

    #Create lists from the files read into the program
    PNRList = mod.readList(PNRListVar)
    orDepList = mod.readList(orDepVar)
    destDepList = mod.readList(destDepVar)

    #Create a separate percentileTT file for each PNR
    for pnr_select in PNRList:
        #Open all PNR files from 6-9 AM and make a {dest:{0600:TT} nested dictionary
        p2dDict = makeNestedDict(pnr_select, destDepList, connectField)

        file_name = 'PNR15_{}_destination'.format(pnr_select)
        writer = mod.mkOutput(file_name, fieldnames )

        # Map destinations to 15th percentile TT in 15 minute bins
        makeCentileFile(pnr_select, p2dDict, orDepList, writer)

        print("Percentile file created for PNR {}".format(pnr_select))
        print('-----------------------------')
    mod.elapsedTime(START_TIME)
