#This program is used to extract accessibility results for a set of user specified labels. This program is generally used
#to make a plot of accessibility versus departure time for transit but can be used in other contexts to look at label specific
#accessibility taken from the raw accessibility results

#INPUT:
#1. baseline raw accessibility
#2. scenario raw accessibility

#OUTPUT:
#Two .csv files with label specific accessibility for the baseline and scenario

#Example use case:

#~krist> python blockAccessExtractor.py -bsaccess base_accessibility_file.csv -updateaccess update_accessibility_file.csv

#This program should be placed in the folder where the .csv files reside.

#Input for labels should look like this:
#enter like 270530234002015 271230425012030 270530240033002

####################################
## Libraries and global variables ##
####################################

import csv
import datetime
import os
import time
import argparse
from myToolsPackage.progress import bar

####################################
## FUNCTIONS
####################################

#Read in the file of the baseline and updated min-by-min accessibility.
def makeAccessOutput(access_file, list, bar, filename):
    access_list = csv.reader(access_file, delimiter=',')
    with open("{0}_{1}.csv".format(filename, currentTime), 'wt') as access_extract: #, newline=''
        extract = csv.writer(access_extract, delimiter= ',')
        #Write the header in for the file. If there are other jobs categories, add them after "W_C000."
        extract.writerow(["label", "deptime", "threshold", "W_C000"])
        #Skips the first line of input file which is a header
        access_file.readline()
        for row in access_list:
            bar.next()
            for i in list:
                if i == int(row[0]):
                    print('Found 1!')
                    #If there are other jobs categories, add them after "W_C000."
                    entry = [int(row[0]), int(row[1]), int(row[2]), int(row[3])]
                    extract.writerow(entry)

if __name__ == "__main__":
    # Start timing
    start_time = time.time()
    print(start_time)
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    #Progress bar
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=351864000) #54000 origins x 18 thresholds x 181 departure times x 2 files, 1.78 hours run time

    parser = argparse.ArgumentParser()
    parser.add_argument('-bsaccess', '--BASE_ACCESS', required=True, default=None)
    parser.add_argument('-updateaccess', '--UPDATE_ACCESS', required=True, default=None)
    args = parser.parse_args()

    # Prompt user for a list of comma separated label IDs.
    input_string = input('space separated labels') #enter like 270530234002015 271230425012030 270530240033002
    list = input_string.split()
    print('list', list)
    blockID = []
    for i in list:
        blockID.append(int(i))
    print('Block IDs', blockID)

    #Produces a new file where the "labels" or blockIDs that match the input above are extracted from the baseline
    #accessibility calc file for the given scenario.
    with open(os.path.join(args.BASE_ACCESS)) as base_file:
        makeAccessOutput(base_file, blockID, bar, "base")
        print("Baseline accessibility for select block groups")
    #Produces a new file with accessibility calcs extracted for the comparable blocks examined in the baseline extract.
    with open(os.path.join(args.UPDATE_ACCESS)) as update_file:
        makeAccessOutput(update_file, blockID, bar, "update")
        print("Changed accessibility for select block groups")
        bar.finish()
    print('End Time:', time.time())

