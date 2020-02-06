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
from collections import defaultdict, OrderedDict

# --------------------------
#       FUNCTIONS
# --------------------------

def main():

    start, curtime = mod.startTimer()

    parser = argparse.ArgumentParser()
    parser.add_argument('-access', '--ACCESS_FILE', required=True, default=None)
    parser.add_argument('-rac', '--RAC_FILE', required=True, default=None) #/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2015/Joined_RAC_FILE_20181003095127.csv
    parser.add_argument('-or_lab', '--ORIGIN_LABEL_FIELD', required=True, default='label') # accessibility file row labels, i.e. TAZ, label, ID, GEOID10
    parser.add_argument('-rac_lab', '--RAC_LABEL_FIELD', required=True, default='GEOID10')
    parser.add_argument('-worker_lab', '--WORKER_LABEL_FIELD', required=True, default='racC000')
    parser.add_argument('-all', action = 'store_true', required=False, help = 'write to file flag') #Flag to find all worker-weighted values
    parser.add_argument('-group','--MAP_FIlE', required = False, help = 'aggregation flag') #Flag to group WW averages by municipality or other level, needs file
    parser.add_argument('-map_lab', '--MAP_LABEL_FIELD', required=False, default='GEOID10') # Type the master field of the map label (3rd list shown)
    parser.add_argument('-group_id', '--GROUP_ID_FIELD', required=False, default='GNIS_ID')


    args = parser.parse_args()

    #Declare variables
    mapFile = None
    map_lab = None
    group_id = None

    #Read in files to dict (make into function)
    access_file = mod.readInToDict(args.ACCESS_FILE)
    access_flds_all = list(access_file.fieldnames)
    # print("Accessibility fields to choose from:", access_flds_all)
    # print('----------------------------------')
    access_flds_use = ['wt_bs','wt_alt','abschgtmwt','pctchgtmwt','bs_5400','alt_5400','abschg5400','pctchg5400',
                   'pctbs5400','bs_5100','alt_5100','abschg5100','pctchg5100','pctbs5100','bs_4800','alt_4800','abschg4800',
                   'pctchg4800','pctbs4800','bs_4500','alt_4500','abschg4500','pctchg4500','pctbs4500','bs_4200','alt_4200',
                   'abschg4200','pctchg4200','pctbs4200','bs_3900','alt_3900','abschg3900','pctchg3900','pctbs3900','bs_3600',
                   'alt_3600','abschg3600','pctchg3600','pctbs3600','bs_3300','alt_3300','abschg3300','pctchg3300','pctbs3300',
                   'bs_3000','alt_3000','abschg3000','pctchg3000','pctbs3000','bs_2700','alt_2700','abschg2700','pctchg2700',
                   'pctbs2700','bs_2400','alt_2400','abschg2400','pctchg2400','pctbs2400','bs_2100','alt_2100','abschg2100',
                   'pctchg2100','pctbs2100','bs_1800','alt_1800','abschg1800','pctchg1800','pctbs1800','bs_1500','alt_1500',
                   'abschg1500','pctchg1500','pctbs1500','bs_1200','alt_1200','abschg1200','pctchg1200','pctbs1200','bs_900','alt_900',
                   'abschg900','pctchg900','pctbs900','bs_600','alt_600','abschg600','pctchg600','pctbs600','bs_300','alt_300',
                   'abschg300','pctchg300','pctbs300']

    rac_file = mod.readInToDict(args.RAC_FILE)
    # print("Rac field to choose from:", rac_file.fieldnames)
    # print('----------------------------------')

    if '-group' in sys.argv:
        mapFile = mod.readInToDict(args.MAP_FIlE)
        # print('fields from file that maps GEOID10 to City (GNIS_ID)', mapFile.fieldnames)
        # map_lab = input('Type the master field of the map label (3rd list shown) (GEOID10): ') or 'GEOID10'
        # group_id = input('Type the field of the group id (GNIS_ID): ') or 'GNIS_ID'
        map_lab = args.MAP_LABEL_FIELD
        group_id = args.GROUP_ID_FIELD


    #Prompt user for field names
    # orlab = input('Type the name of the field that corresponds with the accessibility file row labels, i.e. TAZ, label, ID, GEOID10: ') or 'label'
    # raclab = input('Type the field name for the origin label of the RAC file (GEOID10): ') or 'GEOID10'
    # worker_fld = input('Type field name containing number of workers (racC000): ') or 'racC000'

    orlab = args.ORIGIN_LABEL_FIELD
    raclab = args.RAC_LABEL_FIELD
    worker_fld = args.WORKER_LABEL_FIELD

    #Make input dictionaries
    accessDict = makeNestedDict(access_file, orlab)
    racDict = makeRacDict(rac_file, raclab, worker_fld)

    #If the -all flag is given, make a file with all of the worker weighted values
    if '-all' in sys.argv:
        allWWAccess(accessDict, racDict, access_flds_use, curtime)
    elif '-group' in sys.argv:
        #create dictionary of values that are relevent from the map file
        map, missing = makeMapDict(mapFile, map_lab, group_id)
        groupWWAccess(accessDict, racDict, access_flds_use, map, group_id, bar)

    #Prompt user for a single field to calc ww access
    else:
        singleWWAccess(accessDict, racDict)


def makeNestedDict(access_file, or_lab):
    access_dict = {}
    for row in access_file:
        # grab the id string
        origin = int(row[or_lab])
        # remove the name 'label' from list
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
def groupWWAccess(access_dict, rac_dict, access_flds_use, map, group_id, bar):
    #make the new file name derived from the input file name
    file_name = str(sys.argv[2]).replace(".csv", "")
    file_name2 = file_name.replace("2-", "3-")
    outfile = open('{}-ww-grouped.csv'.format(file_name2), 'w', newline='')
    writer = csv.writer(outfile, delimiter=',')
    writer.writerow([f'{group_id}', 'measure', 'value'])
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
        output_dict[id] = OrderedDict()
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
        for field in access_flds_use:
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

            #row = id, field, output_dict[id]['{}'.format(field)]
            row = id, field, weighted_avg
            writer.writerow(row)
        bar.next()
    print('Grouped WW Accessibility Results Finished')


#Option 3, flag -all
def allWWAccess(access_dict, rac_dict, access_flds_use, curtime):
    #make the new file name derived from the input file name
    file_name = str(sys.argv[2]).replace(".csv", "")
    file_name2 = file_name.replace("2-", "3-")
    outfile = open('{}-ww-all.csv'.format(file_name2), 'w', newline='')
    writer = csv.writer(outfile, delimiter=',')
    writer.writerow(['measure', 'value'])

    for field in access_flds_use:
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
        #Check for waterbody blocks
        if int(origin) in access_dict.keys() and int(origin) in rac_dict.keys():

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









