#This script reads in a sequence of travel time matrix files that have been "split" then rejoined into files broken
# up by PNR and deptime to find the minimum TT in each 15 minute window.

#Process:
#In each 15 minute window across the tt matrix, the minimum travel time is kept.
#The final result is a minimum travel time matrix aggregated to the 15 minute level
#The program assumes that the original TT matrix was calculated without retaining unreachable destinations (0.2.2). Thus no maxtime
#value is listed.

#Navigate to folder or create new folder where you want all of these PNR_min time files to be located

#EXAMPLE USAGE: kristincarlson$ python createMinTT.py -pnrlist PNRList_all.txt -or_dep Deptime_list_origin.txt -dest_dep
#Deptime_list_destination.txt -connect destination -dir matrixbreakerfiles/

#################################
#           IMPORTS             #
#################################
import argparse
import time
import csv
from collections import defaultdict
from myToolsPackage import matrixLinkModule as mod
from myToolsPackage.progress import bar


#################################
#           FUNCTIONS           #
#################################

#This function reads in every PNR_deptime file and creates a nested dictionary for the given PNR
def makeNestedDict(pnr_select, dep_list, connect_field):
    # Initiate the triple nested dict structure
    nest = defaultdict(lambda: defaultdict(int))
    #Add all deptime files to the PNR dict
    for deptime in dep_list:

        with open ('{}PNR_{}_{}.csv'.format(DIR, pnr_select, deptime), 'r') as input:
            reader = csv.DictReader(input)

            for row in reader:
                nest[row[connect_field]][row['deptime']] = row['traveltime']

    print("Created Nested Dictionary for PNR {}".format(pnr_select))
    return nest
# A function to calculate the minimum travel time in 15 minute bins
def makeMinFile(pnr_select, p2d_dict, dep_list, writer):
    # Sort in ascending order
    or_dep_list_sort = sorted(dep_list)


    # Break off destinations
    for dest, timing in p2d_dict.items():
        #print('destination', dest)
        # Cycle through 15 minute departure time list from origin and apply to destinations
        for index, dep15 in enumerate(or_dep_list_sort):
            next = index + 1
            # Make sure not to create a bin range that is beyond the departure time line-otherwise error
            if next < len(or_dep_list_sort):
                # Create list to store TT to calc min value from
                bin15 = []
                # Break out deptimes for each destination
                for deptime, tt in p2d_dict[dest].items():
                    # Check if deptime is within 15 minute bin. Ex. if 6:02 >= 6:00 and 6:02 < 6:15...

                    if mod.convert2Sec(deptime) >= mod.convert2Sec(dep15) and mod.convert2Sec(deptime) < mod.convert2Sec(or_dep_list_sort[next]):

                        bin15.append(int(tt))
                # Calc minimum TT and add to min_tt
                #This version assumes that destinations that cannot be reached are not recorded, thus in some cases, at no
                #departure time can a destination be reached from the selected PNR.
                if bin15:
                    min_tt = min(bin15)
                   # print('min tt:', min_tt)
                    writer.writerow([pnr_select, dest, dep15, min_tt])


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    t0 = time.time()
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=114) #114 files to load and process

    parser = argparse.ArgumentParser()
    parser.add_argument('-pnrlist', '--PNRLIST_FILE', required=True, default=None)
    parser.add_argument('-or_dep', '--OR_DEP_FILE', required=True, default=None) #The deptimes that are the final aggreagation bin
    parser.add_argument('-dest_dep', '--DEST_DEP_FILE', required=True, default=None) #The deptimes that are min-by-min
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
    PNRList = mod.readList(PNRListVar, 'integer')
    orDepList = mod.readList(orDepVar, 'str')
    destDepList = mod.readList(destDepVar, 'str')

    #Create a separate percentileTT file for each PNR
    for pnr_select in PNRList:
        #Open all PNR files across the calculations window (i.e. 6-9 AM) and make a {dest:{0600:TT} nested dictionary
        p2dDict = makeNestedDict(pnr_select, destDepList, connectField)

        file_name = 'PNR_{}_MinTT'.format(pnr_select)
        writer = mod.mkOutput(file_name, fieldnames )

        # Map destinations to minimum TT in 15 minute bins
        makeMinFile(pnr_select, p2dDict, orDepList, writer)
        bar.next()



        print("Minimum file created for PNR {}".format(pnr_select))
        bar.finish()
        print('-----------------------------')
        print("End Time:", time.time())