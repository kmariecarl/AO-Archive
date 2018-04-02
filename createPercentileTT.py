#This script reads in a succession of travel time files that are broken up by departure time and origin and need to be
#aggregated up to 15 minute bins and where the 15th percentile is applied.

#EXAMPLE USAGE: kristincarlson$ python createPercentileTT.py -pnr 'PNRList_origin.txt' -or_dep 'Deptimes_origin.txt
#-dest_dep 'Deptimes_destination.txt' -od 'destination'


#################################
#           IMPORTS             #
#################################
import argparse
import csv
from collections import defaultdict
import numpy
import matrixLinkModule as mod


#################################
#           FUNCTIONS           #
#################################
#Create double nested dictionary to map destination to 15th percentile TT. Remake every PNR.
#{destination:{deptime:{15thTT,
#              deptime:{15thTT,
#              deptime:{15thTT}}
#destination:...
#This function reads in every PNR_deptime file and creates a nested dictionary for the given PNR
def makeNestedDict(pnr, dest_dep_list, file_name_od):
    # Initiate the triple nested dict structure
    nest = defaultdict(lambda: defaultdict(int))
    #Add all deptime files to the PNR dict
    for deptime in dest_dep_list:

        with open ('PNR_{}_{}_{}.txt'.format(pnr, deptime, file_name_od), 'r') as input:
            reader = csv.DictReader(input)

            for row in reader:
                nest[row[file_name_od]][row['deptime']] = row['traveltime']

    print("Created Nested Dictionary for PNR {} of {} set".format(pnr, file_name_od))
    return nest

#A function to calculate the new travel time in 15 minute bins based on 15th percentile
def makeCentileFile(pnr, p2d_dict, or_dep_list, writer):
    #Sort in ascending order
    or_dep_list_sort = sorted(or_dep_list)
    print("Sorted deptime list", or_dep_list_sort)

    #Break off destinations
    for dest, timing in p2d_dict.items():
        # Cycle through 15 minute departure time list from origin and apply to destinations
        for index, time15 in enumerate(or_dep_list_sort):
            next = index + 1
            #Make sure not to exceed 15th item in list otherwise error
            if next < len(or_dep_list_sort):
                # Create list to store TT to calc 15th percentile from
                bin15 = []
                #Break out deptimes for each destination
                for deptime, tt in p2d_dict[dest].items():
                    #Check if deptime is within 15 minute bin. Ex. if 6:02 >= 6:00 and 6:02 < 6:15...
                    if deptime >= time15 and deptime < or_dep_list_sort[next]:
                        bin15.append(int(tt))
                #Calc 15th percentile TT and add to centile_dict
                new_tt = calcPercentile(bin15)
                #DON'T INCLUDE ANY DESTINATIONS THAT CANNOT BE REACHED.
                #if new_tt != 2147483647:
                writer.writerow([pnr, dest, time15, new_tt])

    print("Centile File Created")
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
    parser.add_argument('-pnr', '--PNRLIST_FILE', required=True, default=None)
    parser.add_argument('-or_dep', '--OR_DEP_FILE', required=True, default=None)
    parser.add_argument('-dest_dep', '--DEST_DEP_FILE', required=True, default=None)
    parser.add_argument('-od', '--OD', required=True, default=None)
    args = parser.parse_args()

    #Output file fieldnames
    fieldnames = ['origin', 'destination', 'deptime', 'traveltime']

    #May not be necessary to read in arguments to string variables
    PNRListVar = str(args.PNRLIST_FILE)
    orDepVar = str(args.OR_DEP_FILE)
    destDepVar = str(args.DEST_DEP_FILE)
    file_name_od = str(args.OD)
    #Create lists from the files read into program
    PNRList = mod.readList(PNRListVar)
    orDepList = mod.readList(orDepVar, str)
    destDepList = mod.readList(destDepVar, str)

    #Create a separate percentileTT file for each PNR
    for pnr in PNRList:

        p2dDict = makeNestedDict(pnr, destDepList, file_name_od)

        file_name = 'PNR15_{}_{}'.format(pnr, file_name_od)
        writer = mod.mkOutput(file_name, fieldnames )

        # Map destinations to 15th percentile TT in 15 minute bins
        makeCentileFile(pnr, p2dDict, orDepList, writer)

        print("Percentile file created for PNR {}".format(pnr))
    mod.elapsedTime(START_TIME)