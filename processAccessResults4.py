# This program performs mathematical operations to calculate five forms of accessibility values per origin ID.
# The program can compare a baseline accessibility file with a access file where perhaps not all origins are included from
# the baseline so the processing assumes no change to that origin and assigns the value from the baseline.
# The thresholds that are considered are taken from the change file so if there are fewer thresholds in the change dict,
# the program will still work.

#This program should be used after converting raw access output to averages using Average_transit_local.py

# TO DO:
#Clean up internal variable names to match naming convention of write out.

#Updates 5/8/19
#Output naming convention changed and percent change values are now reported in decimals, example below
#Ex. Percent change of 10% used to be 10.0 but now is reported as 0.10


# --------------------------
#       FUNCTIONS
# --------------------------

# Create a dictionary of dictionaries for mapping thresholds to blockIDs to accessibility values.
def makeInputDict(access_file, access_field):
    with open(access_file, 'r') as input:
        reader = csv.DictReader(input)
        # Create dictionary containing all data
        outerDict = {}
        # Step through the averaged access file
        for row in reader:
            # Check if the threshold value for that particular row has already been added as an outer key to the dictionary
            if int(row['threshold']) not in outerDict:
                # If not, add threshold to dictionary
                outerDict[int(row['threshold'])] = {}
            outerDict[int(row['threshold'])][int(row['label'])] = int(row[access_field])
        return outerDict


# For simplification, make lists of the block labels and thresholds. Could be condensed to one function...
def makeThrshldList(data_dict):
    list0 = list(data_dict)
    list1 = []
    for item in list0:
        list1.append(item)
    sorted_thrshlds = sorted(list1, reverse=True)
    # print('Threshold list:', list1)
    return sorted_thrshlds

#This function makes a list of labels found in each file
def makeLabelList(data_dict):
    # Grab the first key of the dictionary in order to grab one set of the labels. Ex. literally just grabs the number 1800.
    first_key = list(data_dict)[0]
    inner_dict = data_dict[first_key]
    label_list = []
    # label is a string due to makeInputFile
    for label in inner_dict:
        label_list.append(int(label))


    # print('Label list:', label_list)
    return label_list


#This function confirms that labels from both the base and change results are the same. If one file has a value for a
#label and the other doesn't, that label will be skipped.
def cleanLabelList(list_base, list_change):
    missing_label_list = np.setdiff1d(list_base, list_change)
    final_label_list = []
    for x in list_base:
        if x not in missing_label_list:
            final_label_list.append(x)
    return final_label_list


# This function combines all calculated values for each label (different data structures used).
def calcAccessValues(label_list, thrshld_list, base_dict, change_dict, bar):
    # This dictionary initiation structure allows to fill in by column name!
    output_dict = {}
    for label in label_list:
        for thrshld in thrshldlist:
            if label not in output_dict:
                # Add a nested dict for each label which will be filled with all values
                output_dict[label] = {}
            # This is where each label becomes a row that is comprised of a dictionary
            column = output_dict[label]
            # Assign the column name label
            column['label'] = label
            #Here I need to add a statement to assign zero to all labels in the change dict where the label doesn't
            #actually exist due to upstream calculation differences.
            #if the change dict doesn't have information on the particular label, then add a entry and assign the
            #threshold + label combo the jobs value from the base dict so that when change is calculated, no change occurs.
            if label not in change_dict[thrshld]:
                change_dict[thrshld] = {'label': label, 'jobs': baseDict[thrshld][label]}
            # Obtain the weighted accessibility values for the baseline and changed files, and print out
            base_access_wt = calcWeightedAccess(label, label_list, thrshld_list, base_dict)
            chg_access_wt = calcWeightedAccess(label, label_list, thrshld_list, change_dict)
            column['wt_bs'], column['wt_alt'] = base_access_wt, chg_access_wt
            # Calculate the raw (as in the abs difference) and percent change between the WEIGHTED accessibility values
            column['abschgtmwt'], column['pctchgtmwt'] = rawPctWeightAccess(label, base_access_wt, chg_access_wt)
            # Iteratively make names for columns
            name1 = 'bs_{}'.format(thrshld)
            name2 = 'alt_{}'.format(thrshld)
            name3 = 'abschg{}'.format(thrshld)
            name4 = 'pctchg{}'.format(thrshld)
            name5 = 'pctbs{}'.format(thrshld)
            # Find the raw and percent differences for the UNWEIGHTED accessibility values
            column[name3], column[name5] = rawDiff(label, thrshld, base_dict, change_dict)
            column[name4] = pctDiff(column[name1], label, thrshld, base_dict)
            # Add a column for the raw_base and raw_change accessibility values calculated for each threshold
            column[name1], column[name2] = rawValues(label, thrshld, base_dict, change_dict)
            bar.next()
    # The result is a nested dictionary. Each label has a column name: calculated accessibility value.
    # Now make nested dict into list of dictionaries.
    list_of_dicts = []
    for key in output_dict.keys():
        list_of_dicts.append(output_dict[key])
    bar.finish()
    return list_of_dicts


def calcWeightedAccess(label, label_list, thrshld_list, access_dict):
    # Calculate weighted values per threshold pair.
    # Create a list of each difference threshold per label and per base and change.
    diff_list = []
    for thrshld in range(0, len(thrshld_list)):
        if thrshld < len(thrshld_list) - 1:
            # Forward/backward operator
            current = thrshld_list[thrshld]
            next = thrshld_list[thrshld + 1]
            # Do the weighted accessibility math for base first, the change
            diff = access_dict[current][label] - access_dict[next][label]
            exp = math.exp(-0.08 * (current / 60))
            term = diff * exp
            diff_list.append(term)
    # Sum up the weighted values across all thresholds for each label.
    total = 0
    for j in diff_list:
        total += int(j)
    return total


def rawPctWeightAccess(label, base_acs_wt, change_acs_wt):
    diff = round(float(change_acs_wt) - float(base_acs_wt), 3)
    if base_acs_wt == 0:
        pct = 0  # 'Not Defined'
    else:
        pct = round((diff / float(base_acs_wt)), 6)
        #Old: pct = round((diff / float(base_acs_wt))*100, 6)
    return diff, pct


# These two functions are applied to the UNWEIGHTED accessibility values
def rawDiff(label, thrshld, base_dict, chg_dict):
    diff = round(chg_dict[thrshld][label] - base_dict[thrshld][label], 3)
    # add a new output column for access of update mode as a percent of base mode
    if int(base_dict[thrshld][label]) != 0:
        pctbs = round((chg_dict[thrshld][label]/base_dict[thrshld][label])*100, 3)
    else:
        pctbs = 0
    return diff, pctbs

# This function was added to simply put the raw base and change values for each block in the new dataset. This was
# not originally part of this program but was greatly needed.
def rawValues(label, thrshld, base_dict, chg_dict):
    raw_base = base_dict[thrshld][label]
    raw_chg = chg_dict[thrshld][label]
    return raw_base, raw_chg


def pctDiff(diff, label, thrshld, base_dict):
    if int(base_dict[thrshld][label]) != 0:
        pct = round((diff / int(base_dict[thrshld][label]))*100, 6)
        #Old: pct = round((diff / int(base_dict[thrshld][label]))*100, 6)
    # If the base accessibility is zero, pct is undefined
    else:
        pct = 0  # "Not Defined"
    return pct


# This function writes the results out to a single csv file
def output(final_results, thrshld_list):
    with open('processed_results_{}.csv'.format(currentTime), 'w') as outfile:
        # Create fieldnames list iteratively to eliminate user input.
        fieldnames = ['label', 'wt_bs', 'wt_alt', 'abschgtmwt', 'pctchgtmwt']
        for thrshld in thrshld_list:
            fieldnames.append('bs_{}'.format(str(thrshld)))
            fieldnames.append('alt_{}'.format(str(thrshld)))
            fieldnames.append('abschg{}'.format(str(thrshld)))
            fieldnames.append('pctchg{}'.format(str(thrshld)))
            fieldnames.append('pctbs{}'.format(str(thrshld)))
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()
        for label in final_results:
            # Use write row based on the final_results list_of_dicts outcome.
            writer.writerow(label)
        print('Accessibility Results Finished')


# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

import csv
import math
import os
import datetime
import argparse
import numpy as np
from myToolsPackage.progress import bar


if __name__ == "__main__":

    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=972000) #54000 origins x 18 thresholds

    parser = argparse.ArgumentParser()
    parser.add_argument('-bs', '--BASE_FILE', required=True, default=None)
    parser.add_argument('-updt', '--UPDATE_FILE', required=True, default=None)
    parser.add_argument('-access', '--ACCESS_FIELD', required=True, default=None) #i.e. jobs or C000 or jobspdol
    args = parser.parse_args()

    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    baseDict = makeInputDict(args.BASE_FILE, args.ACCESS_FIELD)
    changeDict = makeInputDict(args.UPDATE_FILE, args.ACCESS_FIELD)
    labellist_base = makeLabelList(baseDict)
    labellist_change = makeLabelList(changeDict)
    labellist = cleanLabelList(labellist_base, labellist_change)
    thrshldlist = makeThrshldList(changeDict)
    final_results = calcAccessValues(labellist, thrshldlist, baseDict, changeDict, bar)
    output(final_results, thrshldlist)
