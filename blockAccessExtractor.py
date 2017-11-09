#Example use case:

#~krist> python blockAccessExtractor.py -bsaccess base_accessibility_file.csv -updateaccess update_accessibility_file.csv

#This program should be placed in the folder where the .csv files reside.


####################################
## Libraries and global variables ##
####################################

import csv
import datetime
import os
import time
import argparse

#Start timing
start_time = time.time()
#Make a variable for the current time for use in writing files.
currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
#First 5 are the blockID's of blocks expected to see accessibility changes, the last five are not
#expected to see a change.
# Prompt user for a list of comma separated label IDs.
blockID = input(['Comma separated list of labels'])

#blockID = [270030507042007, 270531097002008, 270531261003037, 270530220002002, 271230333001014]
#Read in the file of the baseline accessibility.
def makeAccessOutput(access_file, list, filename):
    access_list = csv.reader(access_file, delimiter=',')
    with open("{0}_{1}.csv".format(filename, currentTime), 'wt') as access_extract: #, newline=''
        extract = csv.writer(access_extract, delimiter= ',')
        #Write the header in for the file. If there are other jobs categories, add them after "W_C000."
        extract.writerow(["label", "deptime", "threshold", "W_C000"])
        #Skips the first line of input file which is a header
        access_file.readline()
        for row in access_list:
            for i in list:
                if i == int(row[0]):
                    #If there are other jobs categories, add them after "W_C000."
                    entry = [int(row[0]), int(row[1]), int(row[2]), int(row[3])]
                    extract.writerow(entry)

if __name__ == "__main__":
    # Start timing
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    # Parameterize file paths: use argparse so that when blockAccessExtractor is run, the user adds additional arguments for the
    # necesarry file paths.
    parser = argparse.ArgumentParser()
    parser.add_argument('-bsaccess', '--BASE_ACCESS', required=True, default=None)
    parser.add_argument('-updateaccess', '--UPDATE_ACCESS', required=True, default=None)
    args = parser.parse_args()

    #Produces a new file where the "labels" or blockIDs that match the input above are extracted from the baseline
    #accessibility calc file for the given scenario.
    with open(os.path.join(args.BASE_ACCESS)) as base_file: #, newline=''
        makeAccessOutput(base_file, blockID, "base")
        print("Baseline accessibility for select block groups")
    #Produces a new file with accessibility calcs extracted for the comparable blocks examined in the baseline extract.
    with open(os.path.join(args.UPDATE_ACCESS)) as update_file: #, newline=''
        makeAccessOutput(update_file, blockID, "update")
        print("Changed accessibility for select block groups")

