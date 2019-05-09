#This script will take a .csv file and grab information from a single column and create a new .csv file with a single line of text strings.
# No carriage returns.

#Useful for creating the input files for PNR accessibility process, like createMinTT.py which needs a list or destinations

#Example Usage
#kristincarlson$ -python createString.py -csv your_file -field myfieldname


#################################
#           IMPORTS             #
#################################
import csv
import datetime
import os
import time
import argparse
from myToolsPackage import matrixLinkModule as mod

#################################
#           FUNCTIONS           #
#################################


#Make a list of strings from a single column of input file
def makeList(dictionary, field):
    outputList = []
    for row in dictionary:
        outputList.append('\"'+ row['{}'.format(field)] +'\"')
    return outputList
#Open a new file, write the output, close the file.
def writeOutput(output):
    # Open file to write string out to.
    outfile = open('out_{}.txt'.format(currentTime), 'w')
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
    parser.add_argument('-field', '--FIELD', required=True, default=None) #Provide the field for string creation
    args = parser.parse_args()

    #Main operations
    dictionary = (args.CSV_FILE)
    outputList = makeList(dictionary, args.FIELD)
    writeOutput(outputList)


