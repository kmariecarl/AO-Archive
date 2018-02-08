
#Application is used to break large Origin to PNR and PNR to Destination matrices into files by PNR and departure time.
#This program shall be run after the creation of a TT matrix and before the linking of matrices by TTMatrixLink.py

#LOGIC:
#1. import one matrix
#2. Row by row, check if PNR has been added to PNR list, and if deptime has been added to deptime list.
#3. Create pnr-list and deptime-list then seek to top of dictreader object
#4. Loop through pnr_list and deptime-list and the entire matrix to find rows that match selected pnr and deptime
#5. Seek to the top of the matrix dict object for each new pnr item and deptime item
#6. Close matrix file

#NOTES
#Add an optional arguement to only break out one PNR

#TT files come in with following header:
#origin,destination,deptime,traveltime



# EXAMPLE USAGE: kristincarlson$ python matrixBreaker.py -matrix o2pnr_tt-results.csv -pnr 'destination' -connect 'origin'
# -pick '217' -split ./split/
# The last two arguments are optional.
#################################
#           IMPORTS             #
#################################
import argparse
import csv
import matrixLinkModule as mod


#################################
#           FUNCTIONS           #
#################################

#A function to find all PNR and deptimes unique to the matrix file.
def makeLists(matrix, pnr_name_field):
    #Initiate Lists
    pnr_list = []
    deptime_list = []

    #Open Matrix file
    with open(matrix) as f:
        reader = csv.DictReader(f)

        #Create PNR and Deptime lists
        for row in reader:
            #Check if PNR has been added to pnr_list.
            if row[pnr_name_field] not in pnr_list:
                pnr_list.append(row[pnr_name_field])
            #Check if departure time in seconds has been added to deptime_list
            dep_sec = mod.convert2Sec(row['deptime'])
            if dep_sec not in deptime_list:
                deptime_list.append(dep_sec)

    return pnr_list, deptime_list

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
#A function to extract and make files for every PNR listed in the original TT matrix
def processMultiple(fieldname_list, pnr, deptime_select, matrix, pnr_name_field, connect):
    # Open Matrix file for every PNR and deptime combo which makes the program run long (40 min for unit test)
    with open(matrix) as f:
        reader = csv.DictReader(f)

        #Initiate output file
        file_name = 'PNR_{}_{}_{}'.format(pnr, deptime_select, connect)
        writer = mod.mkOutput(file_name, fieldname_list)

        # Cycle through the entire input matrix and add rows to mkOutput file that match the selected PNR and deptime
        for row_again in reader:
            # #Permanantly convert deptime to seconds
            deptime_sec = mod.convert2Sec(row_again['deptime'])
            #If PNR_row == PNR_fromlist and deptime_sec == deptime_select_fromlist:
            if int(row_again[pnr_name_field]) == int(pnr) and int(deptime_sec) == int(deptime_select):

                entry = [row_again['origin'], row_again['destination'], deptime_sec, row_again['traveltime']]
                writer.writerow(entry)

        print("File written for PNR {} at deptime {}".format(pnr, deptime_select))
        mod.elapsedTime(START_TIME)





#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    START_TIME, curtime = mod.startTimer()
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
    fieldnameList = ['origin', 'destination', 'deptime', 'traveltime']

    #Reassign PNR_FIELD to string label
    pnrNameField = str(args.PNR_FIELD) #Will be either 'origin' or 'destination'
    print('pnr-field-name:', pnrNameField)
    #Reassign connecting field name to either origin or destination
    connect = str(args.CONNECT_FIELD)
    print('connect-field:', connect)

    pnr_list, deptime_list = makeLists(args.MATRIX_FILE, pnrNameField)

    # Add a statement to do the process for only one "picked" PNR
    if args.PICKED_PNR: #Same as saying if PICKED_PNR is not None:
        processSingle()
    else:
        #Make new files for each PNR_deptime combo
        for PNR in pnr_list:
            for deptime_select in deptime_list:
                processMultiple(fieldnameList, PNR, deptime_select, args.MATRIX_FILE, pnrNameField, connect)

    mod.writeList('PNRList_{}'.format(connect), pnr_list)
    print('PNR List written to file')
    mod.writeList('Deptimes_{}'.format(connect), deptime_list)
    print('Deptime List written to file')


