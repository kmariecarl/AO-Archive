#This script reads in an accessibility file that has been processed by 'processAccessResults4.py' and another file that
#maps the analysis zone to RAC values and computes the worker weighted average for the selected accessibility threshold

#This program stays in the "Programs" file because it imports "average" from numpy.

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

from numpy import average
import argparse
from myToolsPackage import matrixLinkModule as mod

# --------------------------
#       FUNCTIONS
# --------------------------

def lineUpLists(access_file, label1, access_field, worker_field):

    access_list = []
    worker_list = []
    print('Building Lists...')


    #This is an inefficient way to cycle through .csv info but I don't feel like making a dictionary structure beyond the DictReader obj.
    for row in access_file:

        #account for missing accessibility value
        if row[access_field] is '' and row[worker_field] is '':
            access_list.append(0)
            worker_list.append(0)
        elif row[access_field] is '':
            access_list.append(0)
            worker_list.append(int(row[worker_field]))
            #Account for missing RAC value
        elif row[worker_field] is '':
            access_list.append(float(row[access_field]))
            worker_list.append(0)
            #No missing value option
        else:
            access_list.append(float(row[access_field]))
            worker_list.append(int(row[worker_field]))

    print('Lists Returned')
    return access_list, worker_list



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-access', '--ACCESS_FILE', required=True, default=None)
    parser.add_argument('-rac', '--RAC_FILE', required=False, default=None) #Not required, program not setup for seperate RAC
    args = parser.parse_args()

    #Read in files to dict
    access_file = mod.readInToDict(args.ACCESS_FILE)

    print("Accessibility (and/or RAC) fields to choose from:")
    print(access_file.fieldnames)

    # Prompt user for a time window.
    label1 = input('Type the name of the field that corresponds with the row labels, i.e. TAZ, label, ID ')
    access_field = input('Type field name that you want to find the worker weighted accessibility value for, i.e. rwbs1800: ')

    worker_field = input('Type field name containing number of workers, i.e. rac_C000: ')


    accessList, workerList = lineUpLists(access_file, label1, access_field, worker_field)
    #Formula for weighted access. sum of interaction terms of access values by population divided by the sum of population (total pop).
    weighted_avg = average(accessList, weights=workerList)
    print('Worker-weighted Avg.:', weighted_avg)
