#This program reads in averaged accessibility results that have thresholds listed as field names and transposes them to
#read like the averageTransitLocal output where thresholds are listed vertically.

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

import argparse
import time
from myToolsPackage import matrixLinkModule as mod
from collections import OrderedDict
from myToolsPackage.progress import bar

# --------------------------
#       FUNCTIONS
# --------------------------
def createInternalDict(input_file, bar):
    transpose_dict = OrderedDict()

    counter = 0
    for row in input:
        if counter == 0:
            threshold_list = createThresholdList(row)

        transpose_dict[row['label']] = OrderedDict()
        for thresh in threshold_list:
            if row[thresh] is '':
                value = 0
            else:
                value = row[thresh]
            transpose_dict[row['label']][thresh] = value

        counter += 1
        bar.next()

    bar.finish()

    return transpose_dict

def createThresholdList(row):
    threshold_list = []
    for label in row.keys():
        if label != ID:
            threshold_list.append(label)
    threshold_list_sort = sorted([int(x) for x in threshold_list])

    threshold_list_abc = [str(x) for x in threshold_list_sort]
    print("Threshold List:", threshold_list_abc)
    return threshold_list_abc

def write2File(transpose_dict, fieldnames):

    writer = mod.mkDictOutput('access_results_transposed_2vert_{}'.format(curtime), fieldname_list=fieldnames)

    for id, outter in transpose_dict.items():

        for threshold, jobs in transpose_dict[id].items():

            entry = {'label': id, 'threshold': threshold, 'jobs': jobs}
            writer.writerow(entry)
    print("")
    print("Accessibility results transposed to vertical orientation")

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
    parser.add_argument('-r', '--INPUT_FILE', required=True, default=None)
    parser.add_argument('-id', '--ID_FIELD', required=True, default=None) #i.e. GEOID10 or TAZ or label

    args = parser.parse_args()

    FILE = args.INPUT_FILE
    ID = args.ID_FIELD
    print("Id field:", ID)

    input = mod.readInToDict(FILE)


    fieldnames = ['label', 'threshold', 'jobs']

    transposeDict = createInternalDict(input, bar)
    write2File(transposeDict, fieldnames)


