
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
from myToolsPackage import matrixLinkModule as mod


#################################
#           FUNCTIONS           #
#################################

#A function to find all PNR and deptimes unique to the matrix file. Also makes list files for origin, deptimes, pnrs, and destinations.
def makeLists(matrix, connect_field, pnr_name_field):
    #Initiate Lists
    connect_list = []
    pnr_list = []
    deptime_list = []

    #Open Matrix file
    with open(matrix) as f:
        reader = csv.DictReader(f)

        #Create Connect, PNR and Deptime lists
        for row in reader:
            # Check if origin or destination list needs to be made:
            if connect_field == 'origin':
                if row[connect_field] not in connect_list:
                    connect_list.append(row[connect_field])

                #Check if PNR has been added to pnr_list.
                if row[pnr_name_field] not in pnr_list:
                    pnr_list.append(row[pnr_name_field])
                #Check if departure time in regular time has been added to deptime_list

                if row['deptime'] not in deptime_list:
                    deptime_list.append(row['deptime'])

            #Assuming this program is first run on the origin matrix, only the destination list needs to be created.
            if connect_field == 'destination':
                if row[connect_field] not in connect_list:
                    connect_list.append(row[connect_field])

        #Write out lists to files if list has been filled
        if len(connect_list) > 0:
            mod.writeList('{}_List'.format(connect_field), connect_list, curtime)
            print('{} List written to file'.format(connect_field))
        if len(pnr_list) > 0:
            mod.writeList("PNR_List", pnr_list, curtime)
            print('PNR List written to file')
        if len(deptime_list) > 0:
            mod.writeList("Deptime_List", deptime_list, curtime)
            print('Deptime List written to file')


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
    # Open Matrix file for every PNR and deptime combo
    with open(matrix) as f:
        reader = csv.DictReader(f)

        #Initiate output file
        file_name = 'PNR_{}_{}_{}'.format(pnr, deptime_select, connect)
        writer = mod.mkOutput(file_name, fieldname_list)

        # Cycle through the entire input matrix and add rows to mkOutput file that match the selected PNR and deptime
        for row_again in reader:
            #If PNR_row == PNR_fromlist and deptime == deptime_select_fromlist:
            if int(row_again[pnr_name_field]) == int(pnr) and int(row_again['deptime']) == int(deptime_select):

                entry = [row_again['origin'], row_again['destination'], row_again['deptime'], row_again['traveltime']]
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
    parser.add_argument('-matrix', '--MATRIX_FILE', required=True, default=None) #Name of singluar matrix file if small enough
    parser.add_argument('-pnr', '--PNR_FIELD', required=True, default=None) #Origin or destination depending on which matrix is given
    parser.add_argument('-connect', '--CONNECT_FIELD', required=True, default=None) #Origin or destination depending on which matrix is given
   #Below are all of the optional arguments
    parser.add_argument('-pnrlist', '--PNR_LIST', required=False, default=None) #File that contain pre-made PNR list
    parser.add_argument('-deplist', '--DEPTIME_LIST', required=False, default=None)  #File that contains pre-made departure time list
    parser.add_argument('-pick', '--PICKED_PNR', required=False, default=None)  #Provide when only looking at 1 pnr facility
    parser.add_argument('-split', '--SPLIT_DIR', required=False, default=None) #Provide when using a split directory for large matrices
    args = parser.parse_args()

    #Create fieldnames for output files
    fieldnameList = ['origin', 'destination', 'deptime', 'traveltime']

    #Reassign PNR_FIELD to string label
    pnrNameField = str(args.PNR_FIELD) #Will be either 'origin' or 'destination'
    print('pnr-field-name:', pnrNameField)
    #Reassign connecting field name to either origin or destination
    connect = str(args.CONNECT_FIELD)
    print('connect-field:', connect)

    #If the pnr and deptime pre-made files are not provided, make them from the imported matrix
    if not args.PNR_LIST:
        pnr_list, deptime_list = makeLists(args.MATRIX_FILE, connect, pnrNameField)
    else:
        pnr_list = mod.readList(args.PNR_LIST)
        deptime_list = mod.readList(args.DEPTIME_LIST)
        #Run makeList() function just to make the destination file
        makeLists(args.MATRIX_FILE, connect, pnrNameField)

    # Add a statement to do the process for only one "picked" PNR
    if args.PICKED_PNR: #Same as saying if PICKED_PNR is not None:
        processSingle()
    else:
        #Make new files for each PNR_deptime combo
        for PNR in pnr_list:
            for deptime_select in deptime_list:
                processMultiple(fieldnameList, PNR, deptime_select, args.MATRIX_FILE, pnrNameField, connect)


