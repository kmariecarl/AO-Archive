#This script reads in a csv of rows to be extracted from the larger TT matrix file then writes the rows out
#to a subset file.


#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
from createString import makeDict


#################################
#           FUNCTIONS           #
#################################

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
#################################
#           OPERATIONS          #
#################################


if __name__ == '__main__':

    curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-tt', '--TTMATRIX_FILE', required=True, default=None)
    parser.add_argument('-set', '--SELECTFROM_FILE', required=True, default=None)
    args = parser.parse_args()

    fieldnames = ['origin', 'destination', 'deptime', 'traveltime']
    writer = mkOutput(curtime, fieldnames)

    ttmatrix_dict = makeDict(args.TTMATRIX_FILE)
    pnr_dict = makeDict(args.SELECTFROM_FILE)

    for rowTT in ttmatrix_dict:
        for rowPNR in pnr_dict:
            print('thinking.....')
            if int(rowTT['destination']) == int(rowPNR['PnRNumInt']):
                writer.writerow(rowTT)
                print('writing')

