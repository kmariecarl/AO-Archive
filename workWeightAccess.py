#This script reads in an accessibility file that has been processed by 'processAccessResults4.py' and another file that
#maps the analysis zone to RAC values and computes the worker weighted average for 1. a selected accessibility threshold
#2. an aggregated areas such as by township 3. metro-wide all thresholds (-all)

#This program stays in the "Programs" file because it imports "average" from numpy.

#This script was checked for accuracy in calculating worker weighted values on April 8th 2019. The complementary excel
#file can be found within the dropbox/programs folder.
# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

from numpy import average
import sys
import argparse
import csv
from progress import bar

from myToolsPackage import matrixLinkModule as mod

# --------------------------
#       FUNCTIONS
# --------------------------

def main():

    start, curtime = mod.startTimer()


    parser = argparse.ArgumentParser()
    parser.add_argument('-access', '--ACCESS_FILE', required=True, default=None)
    parser.add_argument('-rac', '--RAC_FILE', required=True, default=None) #/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2015/Joined_RAC_FILE_20181003095127.csv
    parser.add_argument('-group','--MAP_FIlE', required = False, help = 'aggregation flag') #Flag to group WW averages by municipality or other level
    parser.add_argument('-all', action = 'store_true', required=False, help = 'write to file flag') #Flag to find all worker-weighted values
    args = parser.parse_args()

    #Read in files to dict (make into function)
    access_file = mod.readInToDict(args.ACCESS_FILE)
    access_flds = list(access_file.fieldnames)
    print("Accessibility fields to choose from:", access_flds)
    print('----------------------------------')
    rac_file = mod.readInToDict(args.RAC_FILE)
    print("Rac field to choose from:", rac_file.fieldnames)
    print('----------------------------------')

    if '-group' in sys.argv:
        mapFile = mod.readInToDict(args.MAP_FIlE)
        print('fields from map file', mapFile.fieldnames)
        map_lab = input('Type the field of the map label: ') or 'GEOID10'
        group_id = input('type the field of the group id (i.e. GNIS_ID) ') or 'GNIS_ID'

    #Prompt user for field names
    orlab = input('Type the name of the field that corresponds with the accessibility file row labels, i.e. TAZ, label, ID ') or 'label'
    access_flds.remove('{}'.format(orlab))
    raclab = input('Type the field name for the origin label of the RAC file: ') or 'GEOID10'
    worker_fld = input('Type field name containing number of workers, i.e. rac_C000: ') or 'racC000'

    #Make input dictionaries
    accessDict = makeNestedDict(access_file, orlab)
    racDict = makeRacDict(rac_file, raclab, worker_fld)

    #If the -all flag is given, make a file with all of the worker weighted values
    if '-all' in sys.argv:
        allWWAccess(accessDict, racDict, access_flds, curtime)
    elif '-group' in sys.argv:
        #create dictionary of values that are relevent from the map file
        map, missing = makeMapDict(mapFile, map_lab, group_id)
        groupWWAccess(accessDict, racDict, access_flds, curtime, map, group_id, missing,bar)
        # output(curtime, outputDict, access_flds)
    #Prompt user for a single field to calc ww access
    else:
        singleWWAccess(accessDict, racDict)

def makeNestedDict(access_file, or_lab):
    access_dict = {}
    for row in access_file:
        #grab the id string
        origin = int(row[or_lab])
        #remove the name 'label' from list
        row.pop('{}'.format(or_lab))
        dict_with_ints = dict((k, v) for k, v in row.items())
        access_dict[origin] = dict_with_ints

    return access_dict
def makeRacDict(rac_file, rac_lab, worker_fld):
    racDict = {}
    for row in rac_file:
        racDict[int(row['{}'.format(rac_lab)])] = int(row['{}'.format(worker_fld)])

    return racDict

def makeMapDict(map_file, map_lab, group_id ):
    map = {}
    origins_missing_GNIS_ID =[]
    for row in map_file:
        if row['{}'.format(group_id)] is not '':
            map[int(row['{}'.format(map_lab)])] = int(row['{}'.format(group_id)])
        else:
            origins_missing_GNIS_ID.append(int(row['{}'.format(map_lab)]))
            print(row['{}'.format(map_lab)])
    return map, origins_missing_GNIS_ID

#Option 1 no flag
def singleWWAccess(accessDict, rac_dict):

    access_field = input('Type field name that you want to find the worker weighted accessibility value for, i.e. rwbs1800: ')
    accessList, workerList, noRacCount = lineUpLists(accessDict, rac_dict, access_field)
    #Formula for weighted access. sum of interaction terms of access values by population divided by the sum of population (total pop).
    weighted_avg = average(accessList, weights=workerList)
    print('Worker-weighted Avg:', weighted_avg)
    end(noRacCount)

#Option 2, flag -group
def groupWWAccess(access_dict, rac_dict, access_flds, curtime, map, group_id, missing, bar):
    #make the new file name derived from the input file name
    file_name = str(sys.argv[2]).replace(".csv", "")
    outfile = open('{}_ww_grouped_{}.csv'.format(file_name, curtime), 'w', newline='')
    writer = csv.writer(outfile, delimiter=',')
    writer.writerow(['label', 'measure', 'value'])
    output_dict = {}

    #List of unique ids for the new aggregation level
    unique = []
    for i in list(map.values()):
        if i not in unique:
            unique.append(i)
    print('list of {}'.format(group_id), unique)
    iter = len(unique)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=iter)
    #for each unique id, perform the ww calculations
    for id in unique:
        output_dict[id] = {}
        print('Find origins that belong in the {} aggregation (city)'.format(id))
        #create a subset of access_dict with only the origins that match the unique_id
        origin_subset = []
        for origin in access_dict:
            #Check if access was calc'd for origins outside of the met council boundary
            if origin in map.keys():

                if map[origin] == id:
                    origin_subset.append(origin)
                # print('origin subset for city {}'.format(id))
        access_dict_subset = dict((k, access_dict[k]) for k in origin_subset)
        # print('access dict subset', access_dict_subset)
        for field in access_flds:
            print(field)
            accessList,workerList, noRacCount = lineUpLists(access_dict_subset,rac_dict, field)
            # if sum(workerList) is not 0 and len(accessList) > 1 and len(workerList) > 1:
            if sum(workerList) is not 0:
                # print(workerList)
                if id == 663529:
                    print("access list", accessList)
                    print('worker list', workerList)
                weighted_avg = average(accessList, weights=workerList)
                print(weighted_avg)
            else:
                print('This city has no workers?',id)
                weighted_avg = 'NA'
            output_dict[id]['{}'.format(field)] = weighted_avg
            row = id, field, output_dict[id]['{}'.format(field)]
            writer.writerow(row)
        bar.next()
    print('Grouped WW Accessibility Results Finished')


#Option 3, flag -all
def allWWAccess(access_dict, rac_dict, access_flds, curtime):
    #make the new file name derived from the input file name
    file_name = str(sys.argv[2]).replace(".csv", "")

    outfile = open('{}_ww_{}.csv'.format(file_name, curtime), 'w', newline='')
    writer = csv.writer(outfile, delimiter=',')

    for field in access_flds:
        print(field)
        accessList, workerList, noRacCount = lineUpLists(access_dict, rac_dict, field)
        weighted_avg = average(accessList, weights=workerList)
        print(weighted_avg)
        row = [field, weighted_avg]
        writer.writerow(row)
    end(noRacCount)

# Match the labels from the processed accessibility file to the rac file
def lineUpLists(access_dict, rac_dict, access_field):

    access_list = []
    worker_list = []

    no_rac_count = 0
    miss = []
    for i in access_dict:
        origin = int(i)

        #account for missing accessibility value
        if access_dict[origin][access_field] is '' and rac_dict[origin] is '':
            access_list.append(0)
            worker_list.append(0)

        elif access_dict[origin][access_field] is '':
            access_list.append(0)
            worker_list.append(int(rac_dict[origin]))

        #Account for blocks that do not have RAC value (no worker population)
        elif int(origin) not in rac_dict.keys():
            access_list.append(float(access_dict[origin][access_field]))
            worker_list.append(0)
            no_rac_count += 1
            miss.append(origin)


        elif rac_dict[origin] is '':
            access_list.append(float(access_dict[origin][access_field]))
            worker_list.append(0)
            no_rac_count += 1

        else:
            access_list.append(float(access_dict[origin][access_field]))
            worker_list.append(int(rac_dict[origin]))

    assert len(access_list) == len(worker_list), "The access_list and worker_list are not of equal length"

    return access_list, worker_list, no_rac_count

def end(no_rac_count):
    print('blocks without rac value (ie. no worker population)', no_rac_count)
    print('Calculation finished')

if __name__ == "__main__":
    main()









