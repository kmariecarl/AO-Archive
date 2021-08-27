#This script is meant for adding accessibility values that have been averaged by averageTransitLocal.py or the transpose programs.

#The original use case for this program was to add accessibility from PNR and transit modes to show the full accessibility
#at an origin. We determined that this would double count some destinations. The script is saved in case there is a use
#in the future.



# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------
import csv
import argparse
import time
from myToolsPackage import matrixLinkModule as mod
from myToolsPackage.progress import bar
import numpy as np

# --------------------------
#       FUNCTIONS
# --------------------------

# Create a dictionary of dictionaries for mapping thresholds to blockIDs to accessibility values.
def makeInputDict(access_file):
    with open(access_file, 'r') as input:
        reader = csv.DictReader(input)
        # Create dictionary containing all data
        outerDict = {}
        # Step through the averaged access file
        for row in reader:
            # Check if the threshold value for that particular row has already been added as an outer key to the dictionary
            if int(row['label']) not in outerDict:
                # If not, add threshold to dictionary
                outerDict[int(row['label'])] = {}
            outerDict[int(row['label'])][int(row['threshold'])] = int(row['jobs'])
        print("Access file:{}".format(access_file))
        print(outerDict)
        return outerDict

# This function makes a list of labels found in each file
def makeLabelList(data_dict):
    label_list = list(data_dict.keys())


    print('Label list:', label_list)
    return label_list

# This function confirms that labels from both the base and change results are the same. If one file has a value for a
# label and the other doesn't, that label will be skipped.
def cleanLabelList(list_base, list_change):
    missing_label_list = np.setdiff1d(list_base, list_change)
    final_label_list = []
    for x in list_base:
        if x not in missing_label_list:
            final_label_list.append(x)
    print('Missing label list:', missing_label_list)
    return final_label_list

# For simplification, make lists of the block labels and thresholds. Could be condensed to one function...
def makeThrshldList(data_dict):
    # Grab the first key of the dictionary in order to grab one set of the thresholds.
    first_key = list(data_dict)[0]
    inner_dict = data_dict[first_key]
    thresh_list = []
    # thresh is a string due to makeInputFile
    for thresh in inner_dict:
        thresh_list.append(int(thresh))

    sorted_thrshlds = sorted(thresh_list, reverse=False)
    print('Threshold list:', sorted_thrshlds)
    return sorted_thrshlds



#################################
#           OPERATIONS          #
#################################

if __name__ == "__main__":

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    t0 = time.time()
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=3030)

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--TRANSIT_FILE', required=True, default=None) #i.e. Transit2016_Access_BlockAvg2TAZ
    parser.add_argument('-pnr', '--PNR_FILE', required=True, default=None) #i.e. Linked_wTTAccess
    args = parser.parse_args()

    T_FILE = args.TRANSIT_FILE
    PNR_FILE = args.PNR_FILE

    fieldnames = ['label', 'threshold', 'jobs']
    writer = mod.mkDictOutput("sum_access_{}".format(curtime), fieldnames)

    t = makeInputDict(T_FILE)
    pnr = makeInputDict(PNR_FILE)
    labellist_t = makeLabelList(t)
    labellist_pnr = makeLabelList(pnr)
    labellist = cleanLabelList(labellist_t, labellist_pnr)
    thresholdlist = makeThrshldList(t)

    outputDict = {}
    for label in labellist:
        for thresh in thresholdlist:
            access_sum = t[label][thresh] + pnr[label][thresh]
            entry = {'label': label, 'threshold': thresh, 'jobs': access_sum}
            writer.writerow(entry)
            bar.next()
    bar.finish()
    print("Accessibility results summed")


