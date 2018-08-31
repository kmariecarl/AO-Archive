
#Application is used to break large Origin to PNR and PNR to Destination matrices into files by PNR and departure time.
#This program shall be run after the creation of a TT matrix and before the linking of matrices by monetaryAccess_PNR.py
#PNRs, origin_deptimes, destination_deptimes, these are used for comparison within the program.
#Next the pnr2d matrix is passed as a split directory along with the pre-made pnr and deptime list files. At this time
#there is no way to efficiently make a destination list file using Matrix Breaker, it's possible but would be slow.

#LOGIC:
#1. import a split file
#2. Determine if the PNR has been visited before, if not, add to dictionary
#3. Determine if the destination has been visited for that PNR before, if not, add to value list in dictionary.
#4. If both PNR and Deptime have been visited, does a new file need to be opened?
#5. Else, write current row to currently open writer object.
#6. Compare the finalized list of PNRs with the externally provided list of PNRs to make sure that all were visited.

#TT files come in with following header:
#origin,destination,deptime,traveltime


# EXAMPLE USAGE: kristincarlson$ python matrixBreaker.py -pnrlist PNRList_xxx.txt, -deplist Deptimes_dest_xxx.txt
# -split ./UnitTest_Split/

#################################
#           IMPORTS             #
#################################
import argparse
import csv
from myToolsPackage import matrixLinkModule as mod
import glob

#################################
#           FUNCTIONS           #
#################################

#Assumptions for processing split directory:
#1. Assume a pre-made pnr list is given
#2. Assume that the split directory files have been given the .csv extension

def processSplit(location):
    #Initiate global dictionary
    PNR_2_DEPTIME_DICT = {}
    for name in glob.glob('{}*'.format(args.SPLIT_DIR)):
        print('name', name)  # Dynamically assign indicies depending input file
        # Using 'with open' reads into memory one line at a time and discards the line when it moves to the next one.
        with open('{}'.format(name)) as f:
            reader = csv.reader(f)
            #Skip first line of the first file, this is where the headers are listed.
            if name == '{}xaa.csv'.format(args.SPLIT_DIR):
                next(reader)
            count = 0
            for row in reader:
                #Has PNR been visited before?
                if int(row[location]) not in PNR_2_DEPTIME_DICT:
                    #Do stuff to initiate new pnr and deptime, scenario 1
                    #Initiate a key and list value for this new pnr
                    PNR_2_DEPTIME_DICT[int(row[location])] = []
                    #Set the pnr value until the next new one is found
                    set_pnr = row[location]
                    #Append the first departure time found for this pnr.
                    PNR_2_DEPTIME_DICT[int(row[location])].append(row[2])
                    #Set this deptime until a new one is found.
                    set_deptime = row[2]
                    #If both the PnR and deptime are new, then initiate a new file
                    writer = mod.mkOutput('PNR_{}_{}'.format(set_pnr, set_deptime), FIELDNAMES)
                    writer.writerow(row)

                elif row[2] not in PNR_2_DEPTIME_DICT[int(row[location])]:
                    #Do stuff for new deptime, scenario 2
                    #Append the unvisited departure time to existing pnr list
                    PNR_2_DEPTIME_DICT[int(row[location])].append(row[2])
                    #Reset the PNR
                    set_pnr = row[location]
                    #Set this deptime until a new one is found.
                    set_deptime = row[2]
                    #The deptime is new so initiate a new file
                    writer = mod.mkOutput('PNR_{}_{}'.format(set_pnr, set_deptime), FIELDNAMES)
                    writer.writerow(row)

                else:
                    #Check if a file has already been started for this pnr + deptime combo, but only check on first line
                    if int(row[location]) in PNR_2_DEPTIME_DICT and row[2] in PNR_2_DEPTIME_DICT[int(row[location])] and count == 0:
                        #open previous file
                        reopen = open('PNR_{}_{}.txt'.format(row[location], row[2]), 'a')
                        writer = csv.writer(reopen)
                        set_pnr = row[location]
                        set_deptime = row[2]
                        writer.writerow(row)

                    else:
                        #Continue adding rows to either the new file created above, or the reopend file.
                        if row[location] == set_pnr:
                            if row[2] == set_deptime:
                                writer.writerow(row)
                count += 1

        print("Split file {} has been processed".format(name))
        mod.elapsedTime(START_TIME)
        print('-------------------------')
    return list(PNR_2_DEPTIME_DICT.keys())

def compareLists(list1, list2):
    list1 = sorted(list1)
    list2 = sorted(list2)
    if list1 == list2:
        print("Success, lists match!")
        return True
    else:
        print("Not all values processed. Go back and check.")
        return False

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    START_TIME, curtime = mod.startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    #Tell the program which field contains the pnr values, answer is either origin or destination
    parser.add_argument('-pnr', '--PNR_FIELD', required=True, default=None) #Origin or destination depending on which matrix is given
    parser.add_argument('-pnrlist', '--PNR_LIST', required=False, default=None) #File that contain pre-made PNR list
    parser.add_argument('-split', '--SPLIT_DIR', required=False, default=None) #Provide when using a split directory for large matrices (any matrix over 1 GB)
    args = parser.parse_args()

    #Create fieldnames for output files
    FIELDNAMES = ['origin', 'destination', 'deptime', 'traveltime']

    #Read in pre-made lists used for comparison within this program
    EXTERNAL_PNR_LIST = mod.readList(args.PNR_LIST, 'integer')

    #Assign variable field entry according to the PNR field
    if args.PNR_FIELD == 'origin':
        location = 0
    else:
        location = 1
    #Process the split directory into PNR by minute files
    internalPNRList = processSplit(location)
    #At the end, verify that all PNRs in the external list match the internally generated list.
    compareLists(EXTERNAL_PNR_LIST, internalPNRList)
    mod.elapsedTime(START_TIME)


