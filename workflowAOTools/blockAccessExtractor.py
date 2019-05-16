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
from progress import bar

####################################
## FUNCTIONS
####################################
def createBlockList(label_file):
    # Option 1: Grab access for user specified blocks
    # Prompt user for a list of comma separated label IDs.
    if label_file == 'NONE':
        input_string = input('space separated labels')  # enter like 270530234002015 271230425012030 270530240033002
        list = input_string.split()
        print('list', list)
        blockID = []
        for i in list:
            blockID.append(int(i))
        count = len(blockID)
        print('Block IDs', blockID)
        return blockID, count

    # Option 2: input file with all blocks to extract
    else:
        with open('{}'.format(label_file)) as input_list:
            blockID = []
            count = 0
            for i in input_list:
                blockID.append(int(i))
                count += 1
            print('Number of origins to select from original (raw file)', count)
            return blockID, count

#Incorporate this function into matrixLinkModule.py
def createAccessibilityDict(access_file, count):
    access = csv.DictReader(access_file)

    access_dict = {}
    for row in access:

        if int(row['label']) not in access_dict:

            access_dict[int(row['label'])] = {}
            access_dict[int(row['label'])][int(row['deptime'])] = {}
            access_dict[int(row['label'])][int(row['deptime'])][int(row['threshold'])] = int(row['C000'])

        if int(row['deptime']) not in access_dict[int(row['label'])]:

            access_dict[int(row['label'])][int(row['deptime'])] = {}
            access_dict[int(row['label'])][int(row['deptime'])][int(row['threshold'])] = int(row['C000'])


        elif int(row['threshold']) not in access_dict[int(row['label'])][int(row['deptime'])]:

             access_dict[int(row['label'])][int(row['deptime'])][int(row['threshold'])] = int(row['C000'])

    print('Number of blocks from original accessibility results file: ', len(access_dict))
    # print('Original accessibility results to create subset from: ')
    # print(access_dict)
    return access_dict

#Read in the file of the baseline and updated min-by-min accessibility.
def makeAccessOutput(access_dict, list, bar, filename):
    #Initiate water body block list
    water_blocks = []

    with open("{0}_{1}.csv".format(filename, currentTime), 'wt') as access_extract:
        extract = csv.writer(access_extract, delimiter= ',')
        #Write the header in for the file. If there are other jobs categories, add them after "W_C000."
        extract.writerow(["label", "deptime", "threshold", "C000"])

        #Grab label from the subset list
        for label in list:
            #Make sure the label is in the orginal accessibility results file
            if label in access_dict.keys():
                for dep in access_dict[label].keys():
                    for thresh in access_dict[label][dep].keys():
                        entry = [label, dep, thresh, access_dict[label][dep][thresh]]

                        extract.writerow(entry)
            else:
                water_blocks.append(label)
            bar.next()
    return water_blocks

if __name__ == "__main__":
    # Start timing
    start_time = time.time()
    print(start_time)
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

    parser = argparse.ArgumentParser()
    parser.add_argument('-bsaccess', '--BASE_ACCESS', required=True, default=None)
    parser.add_argument('-updateaccess', '--UPDATE_ACCESS', required=True, default=None)
    parser.add_argument('-labels', '--LABEL_FILE', required=True, default=None) #csv containing list of labels to strip or say NONE
    args = parser.parse_args()

    blockID, count = createBlockList(args.LABEL_FILE)


    # Progress bar
    bar = bar.Bar(message='Processing', fill='@', suffix='%(percent)d%%',
                  max=count)  # 54000 origins x 18 thresholds x 181 departure times x 2 files, 1.78 hours run time

    #Produces a new file where the "labels" or blockIDs that match the input above are extracted from the baseline
    #accessibility calc file for the given scenario.
    with open(os.path.join(args.BASE_ACCESS)) as base_file:
        base_dict = createAccessibilityDict(base_file, count)


        missing = makeAccessOutput(base_dict, blockID, bar, "base")
        print('Blocks that could not be found in the main file from the list of input origins')
        print(missing)
        print("Finished: Baseline accessibility for select block groups")
    #Produces a new file with accessibility calcs extracted for the comparable blocks examined in the baseline extract.
    with open(os.path.join(args.UPDATE_ACCESS)) as update_file:
        update_dict = createAccessibilityDict(update_file, count)
        missing = makeAccessOutput(update_dict, blockID, bar, "update")
        print('Blocks that could not be found in the main file from the list of input origins')
        print(missing)
        print("Finished: Alternative accessibility for select block groups")

    bar.finish()
    print('End Time:', time.time())


