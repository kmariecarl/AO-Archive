#This script will take a .csv file and grab information from a single column and create a new .csv file with a single line of text strings. No carriage returns.


#################################
#           IMPORTS             #
#################################
import csv
import datetime
import os
import time
import argparse

#################################
#           FUNCTIONS           #
#################################

#Make a dictReader object from input file
def makeDict(file):
    input = open(file, 'r')
    reader = csv.DictReader(input)
    return reader
#Make a list of strings from a single column of input file
def makeList(dictionary):
    outputList = []
    for row in dictionary:
        outputList.append('\"'+ row['trappedSto'] +'\"')
    return outputList
#Open a new file, write the output, close the file.
def writeOutput(output):
    # Open file to write string out to.
    outfile = open('trappedStopsString_{}.txt'.format(currentTime), 'w')
    writer = csv.writer(outfile, quotechar = "'")
    writer.writerow(output)
    outfile.close()

#################################
#           OPERATIONS          #
#################################


if __name__=="__main__":
     # Start timing
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-csv', '--CSV_FILE', required=True, default=None)
    args = parser.parse_args()

    #Main operations
    dictionary = makeDict(args.CSV_FILE)
    outputList = makeList(dictionary)
    writeOutput(outputList)


