#This program breaks a Travel Time matrix into separate .csv/.txt files for each origin or destination.
#Application is used to break large Origin to PNR and PNR to Destination matrices into files by PNR.
#This program shall be run after the creation of a TT matrix and before the linking of matrices by TTMatrixLink.py

#LOGIC:
#1. import one matrix, only one argument
#2. Row by row, check if PNR has been added to PNR list
#3. Look for that PNR
#NOTES


#EXAMPLE USAGE: kristincarlson$ python...

#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
import numpy
import timeit
import zipfile
#import bz2

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

def mkOutput(fieldnames, pnr_num, name, currentTime):
    outfile = open('PNR_{}_{}_{}.txt'.format(pnr_num, name, currentTime), 'w')
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    return writer

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-matrix', '--MATRIX_FILE', required=True, default=None)
    #Tell the program which field contains the pnr values, answer is either origin or destination
    parser.add_argument('-pnr', '--PNR_FIELD', required=True, default=None)
    parser.add_argument('-connect', '--CONNECT_FIELD', required=True, default=None)
    args = parser.parse_args()

    #Create fieldnames for output files
    fieldnames = ['origin', 'destination', 'deptime', 'traveltime']

    #Create pnr_list
    pnr_list = []

    #Reassign PNR_FIELD to string label
    pnr_name_field = str(args.PNR_FIELD)
    print('pnr-field-name:', pnr_name_field)
    #Reassign connecting field name to either origin or destination
    connect = str(args.CONNECT_FIELD)
    print('connect-field:', connect)

#Using 'with open' reads into memory one line at a time and discards the line when it moves to the next one.
    f = open(args.MATRIX_FILE)
    reader = csv.DictReader(f)
    for row in reader:
        #Check if PNR has been added to pnr_list.
        if row[pnr_name_field] not in pnr_list:
            pnr_list.append(row[pnr_name_field])
            print('pnr_list:', pnr_list)

    f.seek(0)
    for item in pnr_list:
        # Initiate a new file
        writer = mkOutput(fieldnames, item, connect, curtime)
        # Cycle through the entire input matrix and add rows to mkOutput file that match the selected PNR
        for row_again in reader:
            if row_again[pnr_name_field] == item:
                writer.writerow(row_again)
        f.seek(0)
    f.close()




