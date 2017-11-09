#This script reads in a file that lists MN blocks and the associated job and worker values (taken from
#MN_WAC_2014 and MN_RAC_2014 datasets that were joined in QGIS "Origins_852_156_TC_jobs_workers."

#This program stays in the "Programs" file because it imports "average" from numpy.

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

import csv
import os
from numpy import average
import argparse

# --------------------------
#       FUNCTIONS
# --------------------------

def makeInputDict(access_file, access_field, worker_field):
    with open(access_file, 'r') as input:
        reader = csv.DictReader(input)
        access_list = []
        worker_list = []
        print('Building Lists...')



        for row in reader:
        #This configuration ensures that the access and worker values are in the same order for each list.
        #if row['raw_base_3'] not in access_list:
            if row[access_field] is '':
                access_list.append(0)
            else:
                access_list.append(float(row[access_field]))
        #if row['rac_C000'] not in worker_list:
            if row[worker_field] is '':
                worker_list.append(0)
            else:
                worker_list.append(int(row[worker_field]))
        print('Lists Returned')
        return access_list, worker_list




#Give the path of the file that contains the pre-calculated accessibility values and the RAC values at the block level.
ACCESS = os.path.join('..', 'TestScenarios', 'mnpass_crnt_xpres', 'Post_process_access','tl_27_55_mnpass_crnt_xpres_impact_zones.csv')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--ACCESS_FILE', required=True, default=None)
    args = parser.parse_args()

    # Prompt user for a time window.
    access_field = input('Type field name that you want to find the worker weighted accessibility value for:')
    worker_field = input('Type field name containing number of workers, i.e. rac_C000:')

    accessList, workerList = makeInputDict(args.ACCESS_FILE, access_field, worker_field)
    weighted_avg = average(accessList, weights=workerList)
    print('Worker-weighted Avg.:', weighted_avg)
