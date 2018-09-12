#This program reads in averaged accessibility results and transposes them to have thresholds listed as field names

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

import argparse
import time
from myToolsPackage import matrixLinkModule as mod
from collections import defaultdict, OrderedDict
from myToolsPackage.progress import bar

# --------------------------
#       FUNCTIONS
# --------------------------

#################################
#           OPERATIONS          #
#################################

if __name__ == "__main__":

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    t0 = time.time()
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=972000) #54000 origins by 18 thresholds

    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--INPUT_FILE', required=True, default=None)
    parser.add_argument('-id', '--ID_FIELD', required=True, default=None) #i.e. GEOID10 or TAZ or label
    parser.add_argument('-thresh', '--THRESHOLD_FIELD', required=True, default=None) #i.e. threshold or cost
    parser.add_argument('-access', '--ACCESS_FIELD', required=True, default=None) #i.e. jobs or C000
    parser.add_argument('-fname', '--FILE_NAME', required=True, default=None) #i.e. Transit16_access
    args = parser.parse_args()

    FILE = args.INPUT_FILE
    ID = args.ID_FIELD
    THRESHOLD = args.THRESHOLD_FIELD
    ACCESS = args.ACCESS_FIELD
    NAME = args.FILE_NAME

    input = mod.readInToDict(FILE)

    transpose_dict = defaultdict(lambda: OrderedDict())
    fieldnames = [ID]
    for row in input:
        transpose_dict[row[ID]][row[THRESHOLD]] = row[ACCESS]
        if row[THRESHOLD] not in fieldnames:
            fieldnames.append(row[THRESHOLD])
        bar.next()

    print("")
    print("Accessibility results transposed")
    bar.finish()

    writer = mod.mkOutput('{}_transposed_2hor{}'.format(NAME, curtime), fieldname_list=fieldnames)

    for id, outter in transpose_dict.items():
        entry = [id]
        for thresh, value in transpose_dict[id].items():
            entry.append(value)
        writer.writerow(entry)