#This program breaks a Travel Time matrix into separate .csv/.txt files for each origin or destination.
#Application is used to break large Origin to PNR and PNR to Destination matrices into files by PNR and departure time.
#This program shall be run after the creation of a TT matrix and before the linking of matrices by TTMatrixLink.py

#LOGIC:
#1. import one matrix
#2. Row by row, check if PNR has been added to PNR list, and if deptime has been added to deptime list.
#3. Create pnr-list and deptime-list then seek to top of dictreader object
#4. Loop through pnr_list and deptime-list and the entire matrix to find rows that match selected pnr and deptime
#5. Seek to the top of the matrix dict object for each new pnr item
#6. Close matrix file
#NOTES
#Add an optional arguement to only break out one PNR

#TT files come in with following header:
#origin,destination,deptime,traveltime


#EXAMPLE USAGE: kristincarlson$ python matrixBreaker.py -matrix o2pnr_tt-results.csv -pnr 'destination' -connect 'origin'
#-pick '217' -split ./split/
#The last two arguments are optional.
#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
import glob

#################################
#           FUNCTIONS           #
#################################

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

#Use this function to process a single PNR given a "split" directory
#COME BACK AND FIX FOR WRITING OUT DEPTIME FILES
#Try not to use "split" directory if possible.
# def processSingle():
#     print("Picked PNR:", args.PICKED_PNR)
#     pick = str(args.PICKED_PNR)
#     pnr_list.append(pick)
#
#     for name in glob.glob('{}*'.format(args.SPLIT_DIR)):  # .format(args.SPLIT_DIR)
#         print('name', name)  # Dynamically assign indicies depending input file
#         if pnr_name_field == 'origin':
#             location = 0
#             # location = 'origin'
#             print('location:', location)
#         else:
#             location = 1
#             # location = 'destination'
#             print('location:', location)
#
#             # Using 'with open' reads into memory one line at a time and discards the line when it moves to the next one.
#         with open('{}'.format(name)) as f:
#             reader = csv.reader(f)
#             # The Terminal 'split' function will always assign the first of the broken file to the xaa name
#             # which will have the headers.
#             if name == 'xaa.csv':
#                 print('HEADER ROW')
#                 next(reader)
#
#             # Create Deptime lists
#             for row in reader:
#                 if row[2] not in deptimes_list:
#                     deptimes_list.append(row[2])
#             f.seek(0)
#
#             # Cycle through the entire input matrix and add rows to mkOutput file that match the selected PNR
#             for deptime in deptimes_list:
#                 writer = mkOutput(fieldnames, pick, connect, deptime)
#                 for row in reader:
#                     # print(row_again[location], pick)
#                     if row[location] == pick and row[2] == deptime:
#                         writer.writerow(row)
#
#             print("Split file {} has been processed".format(name))
#             elapsedTime()
#             print('-------------------------')
#     print("File written for PNR {}".format(pick))

def processMultiple():
    #Open Matrix file
    with open(args.MATRIX_FILE) as f:
        reader = csv.DictReader(f)
        #Create PNR and Deptime lists
        for row in reader:
            #Check if PNR has been added to pnr_list.
            if row[pnr_name_field] not in pnr_list:
                pnr_list.append(row[pnr_name_field])
                #print('pnr_list:', pnr_list)
            if row['deptime'] not in deptimes_list:
                deptimes_list.append(row['deptime'])
        elapsedTime()
        f.seek(0)
        #Make new files for each PNR_deptime combo
        for PNR in pnr_list:
            for deptime in deptimes_list:
                # Initiate a new file
                writer = mkOutput(fieldnames, PNR, connect, deptime)
                # Cycle through the entire input matrix and add rows to mkOutput file that match the selected PNR and deptime
                for row_again in reader:
                    if row_again[pnr_name_field] == PNR and row_again['deptime'] == deptime:
                        entry = [row_again['origin'], row_again['destination'], row_again['deptime'], row_again['traveltime']]
                        writer.writerow(entry)
                print("File written for PNR {} at deptime {}".format(PNR, deptime))
                elapsedTime()
                f.seek(0)

#This consecutively writes out .txt files pertaining to each PNR
def mkOutput(fieldnames, pnr_num, name, deptime):
    outfile = open('PNR_{}_{}_{}.txt'.format(pnr_num, name, deptime), 'w', newline='')
    writer = csv.writer(outfile, delimiter=',')
    writer.writerow(['origin', 'destination', 'deptime', 'traveltime'])
    return writer

#This function writes out a list of PNR labels that will be used by TTMatrixLink.py as an argument
def writePNRList(list, currentTime):
    with open('PNRList_{}.txt'.format(currentTime), 'w') as outlist:
        for item in list:
            outlist.write(item)
            outlist.write(',')

#This function writes out a list of departure times that will be used by TTMatrixLink.py as an argument
def writeDeptimes(list, currentTime):
    with open('Deptimes_{}.txt'.format(currentTime), 'w') as outlist:
        for item in list:
            outlist.write(item)
            outlist.write(',')


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    #Tell the program which field contains the pnr values, answer is either origin or destination
    parser.add_argument('-matrix', '--MATRIX_FILE', required=True, default=None)
    parser.add_argument('-pnr', '--PNR_FIELD', required=True, default=None)
    parser.add_argument('-connect', '--CONNECT_FIELD', required=True, default=None)
    parser.add_argument('-pick', '--PICKED_PNR', required=False, default=None)
    parser.add_argument('-split', '--SPLIT_DIR', required=False, default=None)
    args = parser.parse_args()


    #Create fieldnames for output files
    fieldnames = ['origin', 'destination', 'deptime', 'traveltime']

    #Create pnr_list
    pnr_list = []

    #Create deptimes list
    deptimes_list = []

    #Reassign PNR_FIELD to string label
    pnr_name_field = str(args.PNR_FIELD) #Will be either 'origin' or 'destination'
    print('pnr-field-name:', pnr_name_field)
    #Reassign connecting field name to either origin or destination
    connect = str(args.CONNECT_FIELD)
    print('connect-field:', connect)

    # Add a statement to do the process for only one "picked" PNR
    if args.PICKED_PNR: #Same as saying if PICKED_PNR is not None:
        processSingle()
    else:
        processMultiple()

    writePNRList(pnr_list, curtime)
    writeDeptimes(deptimes_list, curtime)


