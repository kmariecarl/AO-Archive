# This program reads in csv files from dual2_assembleResults.py along with the
# LEHD RAC values to begin aggregating the dual accessibility results over the metro worker population by different
# worker groups, i.e. racC000, racCA01, racCA02, etc.

# NOTE: User must have a demographics.json file prepared for the configuration of worker groups and output csv files.
# The demographics.json file should be in the folder this program is run out of.
# See: /Users/kristincarlson/Dropbox/AO_Projects/TIRP/2_Dual/BDE/demographics.json

#################################
#           IMPORTS             #
#################################

import argparse
from datetime import datetime
from progress import bar
import os
import json
import csv
import fnmatch
import operator



#################################
#           CLASSES           #
#################################

class ProgressBar:
    def __init__(self, lines):
        self.bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=lines)
    def add_progress(self):
        self.bar.next()
    def end_progress(self):
        self.bar.finish()

class FolderObject:
    def __init__(self, dir, name):
        self.dir = dir
        self.scenario_name = name
        self.db = None
        self.tables = []
        self.rac = None

    def move_into_folder(self):
        os.chdir(f"{self.dir}{self.scenario_name}")
        print("Working directory: ", os.getcwd())

    def move_outof_folder(self):
        os.chdir(f"{self.dir}")
        print("Working directory: ", os.getcwd())

    # Place the results in poi-specific folders for easier navigation
    def make_dirs(self, poi_list):
        print("Make directories for ", poi_list)
        for p in poi_list:
            if not os.path.exists(f"{self.dir}{self.scenario_name}/5_{p}"):
                os.makedirs(f"{self.dir}{self.scenario_name}/5_{p}")

    def load_data(self, file_str, prefix):
        data = {}
        with open(f"{file_str}") as csvin:
            reader = csv.DictReader(csvin)
            for row in reader:
                data[row['origin']] = {}
                for k, v in row.items():
                    if '_' in k:
                        dest_num = k.split('_')[1]
                        if 'bs' in k:
                            header = f"{prefix[0]}_{dest_num}"  #rewrite bs to base
                        else:
                            header = f"{prefix[1]}_{dest_num}"  # rewrite updt to bde, de, etc.
                        data[row['origin']][header] = float(v)
                    else:
                        data[row['origin']][k] = float(v)
        return data

    def make_demographics_table(self, subsets, rac_dict, demo_config):
        for s in subsets:  # e.x. METRO, BLINE, DLINE, ELINE
            out = {}
            file = find_file(f"3_*_{p}_{s}_*.csv")
            prefix = [x for x in file.split('_')[2:4]]  #grabs the scenarios included in the file name, 'base', 'bde'
            file_str = f"{self.dir}{self.scenario_name}/{file}"
            data = self.load_data(file_str, prefix)
            for name, rac_group in demo_config.items():  # new file should be generated for each 'name'
                out[f'{name}'] ={}
                for rac_field, rac_text in rac_group.items():
                    rac_total = total_rac(data, rac_field, rac_dict, s)
                    out[f'{name}'][f'{rac_text}'] = rac_total
            write_demographics(self.dir, self.scenario_name, file, out, p)

    # output data should be:
    # [base,age,<=29,8,15,22,60]
    # [bde,age,<=29,7,14,20,60]
    def calc_rac_quartiles(self, subsets, rac_dict, demo_config, dest_num, p):
        for s in subsets:  # e.x. METRO, BLINE, DLINE, ELINE
            out = {}
            file = find_file(f"3_*_{p}_{s}_*.csv")
            prefix = [x for x in file.split('_')[2:4]]  #grabs the scenarios included in the file name, 'base', 'bde'
            file_str = f"{self.dir}{self.scenario_name}/{file}"
            data = self.load_data(file_str, prefix)
            for name, rac_group in demo_config.items():  # new file should be generated for each 'name'
                for prx in prefix:
                    out[f'{prx}'] = {}
                    out[f'{prx}'][name] = {}
                    for rac_field, rac_text in rac_group.items():
                        rac_total = total_rac(data, rac_field, rac_dict, s)
                        quart_dict = make_quartiles(rac_total)
                        out[f'{prx}'][name][f"{rac_text}"] = {}
                        tup_list = []
                        for k, v in data.items():
                            if k in rac_dict.keys():  # In case water bodies were included in original set of origins
                                tup_list.append((data[k][f'{prx}_{dest_num}'], rac_dict[k][rac_field]))  # appending the tt found in the baseline_2 dest and the matching rac value
                        tup_list.sort(key=operator.itemgetter(0))  # sort tuple order by travel time
                        # select quartile, search tuple list until the sum of the rac values equals or exceeds the quartile
                        for q_name, q in quart_dict.items():
                            rac_sum = 0
                            for i in tup_list:  # i is the travel time
                                rac_sum = rac_sum + i[1]
                                if rac_sum >= q:
                                    # tt value needed for 25%, etc. of workers to reach 1 grocery on the baseline network...
                                    out[f'{prx}'][name][f"{rac_text}"][q_name] = int(i[0]/60)  #convert to minutes and round to nearest int
                                    break
                write_quartiles(self.dir, self.scenario_name, file, name, out, p)
                print(f"Wrote rac quartile travel times for {name} worker group and file: ", file)

    # output data should be:
    # [age, <=29, -5min, -10min, -15min, +5min, +10min, +15min]
    # [age, 30–54, 1, 450 (27%), 20 (0.1%), 2 (0.01%), 49 (13%), 15 (4%), 1 (0.3%)]
    # [age, ...]
    def calc_rac_impact(self, subsets, rac_dict, demo_config, dest_num, p):
        for s in subsets:  # e.x. METRO, BLINE, DLINE, ELINE
            file = find_file(f"3_*_{p}_{s}_*.csv")
            prefix = [x for x in file.split('_')[2:4]]  # grabs the scenarios included in the file name, 'base', 'bde'
            file_str = f"{self.dir}{self.scenario_name}/{file}"
            data = self.load_data(file_str, prefix)
            for name, rac_group in demo_config.items():  # new file should be generated for each 'name'
                out = {}
                out[name] = {}
                for rac_field, rac_text in rac_group.items():
                    rac_total = total_rac(data, rac_field, rac_dict, s)
                    out[name][f"{rac_text}"] = {}
                    # sum all rac associated with blocks that changed by +0–5 min, -0–5 min, etc
                    tt_buckets = [300, 600, 900]
                    for tt in tt_buckets:
                        rac_sum_pos = 0
                        rac_sum_neg = 0
                        for origin in data.keys():
                            if origin in rac_dict.keys():  # rac_dict does not contain water body origins, so this checks removes water bodies from the access impact calculations
                                if tt == 300:  # The first bucket should be adjusted to 1–5 minutes (showing less than 1 min change isn't useful)
                                    # Look for negative tt change between the range given, i.e. -60 – -300
                                    if data[origin][f'abschg{dest_num}'] < ((tt + 60) * -1) + 300 and data[origin][f'abschg{dest_num}'] >= tt * -1:
                                        rac_sum_neg = rac_sum_neg + rac_dict[origin][rac_field]
                                    # Look for positive tt change between the range given, i.e. -300 – -600
                                    if data[origin][f'abschg{dest_num}'] > (tt + 60) - 300 and data[origin][f'abschg{dest_num}'] <= tt:
                                        rac_sum_pos = rac_sum_pos + rac_dict[origin][rac_field]
                                else:
                                    # Look for negative tt change between the range given, i.e. -301 – -600
                                    if data[origin][f'abschg{dest_num}'] < (tt * -1) + 300 and data[origin][f'abschg{dest_num}'] >= tt * -1:
                                        rac_sum_neg = rac_sum_neg + rac_dict[origin][rac_field]
                                    # Look for positive tt change between the range given, i.e. -300 – -600
                                    if data[origin][f'abschg{dest_num}'] > tt - 300 and data[origin][f'abschg{dest_num}'] <= tt:
                                        rac_sum_pos = rac_sum_pos + rac_dict[origin][rac_field]
                        out[name][f"{rac_text}"][f'{int(tt/60)} min closer'] = f"{rac_sum_neg:,}" + " (" + f"{round((rac_sum_neg/rac_total)*100, 2)}" + "%)"
                        out[name][f"{rac_text}"][f'{int(tt/60)} min farther'] = f"{rac_sum_pos:,}" + " (" + f"{round((rac_sum_pos/rac_total)*100, 2)}" + "%)"
                write_rac_impact(self.dir, self.scenario_name, file, name, out, p)



#################################
#           FUNCTIONS           #
#################################
def make_lists(input):
    out = []
    for i in input:
        out.append(str(i))
    return out

def load_demographic_config(dir):
    with open(f'{dir}/demographics.json') as json_file:
        demo_config = json.load(json_file)
        for i, j in demo_config.items():
            print(i, j)
    return demo_config

# Solution from https://kite.com/python/examples/4293/os-get-the-relative-paths-of-all-files-and-subdirectories-in-a-directory
def find_file(string):
    my_file = None
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, f"{string}"):
            my_file = file
    return my_file

def make_folder_list(dir):
    with open(f'{dir}/folder_list.json') as json_file:
        folder_names = json.load(json_file)
        folder_list = []
        for key, name in folder_names.items():
            folder_list.append(FolderObject(dir, name))
    return folder_list

def make_subset_list(dir):
    # Makes a list of path strings to each SUBSET file
    with open(f'{dir}/subset_list.json') as json_file:
        subset_names = json.load(json_file)
        subset_list = []
        subset_short_name = []
        for key, name in subset_names.items():
            subset_list.append(name)
            subset_short_name.append(key)
    return subset_list, subset_short_name


# Solution from: https://stackoverflow.com/questions/7125467/find-object-in-list-that-has-attribute-equal-to-some-value-that-meets-any-condi
def return_folder_obj(folder_list, row):
    for f in folder_list:
        if row[0] in f.scenario_name:
            out_f = f
            break
    else:
        out_f = None
        print(f"Could not match {row[0]} from comparisons file to a folder in the folder object list")

    return out_f

# rac_dict should be a nested dictionary
# {GEOID10: {rC000: 100, rCA01: 40, rCA02: 36}}
# This method allows flexible creation of the worker dictionary, regardless of fieldnames, but the block id should be in
# the first column.
def load_rac(rac):
    rac_dict = {}
    with open(rac) as csvin:
        reader = csv.DictReader(csvin)
        column_names = reader.fieldnames  # returns header names in a list form
        print("rac fieldnames: ", column_names)
        for row in reader:
            rac_dict[row[f"{column_names[0]}"]] = {}
            for c in column_names[1:]:  # each of the worker groups needs to be added to the nested dictionary
                if row[f"{c}"] == '':  # check if there are missing values, replace with 0
                    rac_dict[row[f"{column_names[0]}"]][c] = 0
                else:
                    rac_dict[row[f"{column_names[0]}"]][c] =int(row[f"{c}"])
    return rac_dict

def total_rac(data, rac_field, rac_dict, s):
    # Only count workers that are in the analysis area and in the worker group specified by rac_field
    total = 0
    for k, v in data.items():
        if k in rac_dict.keys():  # In case the calculations were originally done with water bodies in the origins
            total = total + rac_dict[k][rac_field]
    # print(f"Total workers of group {rac_field} found in analysis region {s}: {total}")
    return total

# This group_dict does not have headers
def load_group_data(id, group_data_path):
    out = {}
    with open(f"{group_data_path}") as csvin:
        reader = csv.DictReader(csvin)
        for row in reader:
            out[row['GEOID10']] = row[f'{id}']
    return out


def make_quartiles(total_rac):
    qt = {'25%': 0.25 * total_rac, '50%': 0.50 * total_rac, '75%': 0.75 * total_rac, '95%': 0.95 * total_rac}
    return qt  # return dictionary of keys and values

def write_demographics(dir, scenario_name, file, out, p):
    name_out0 = file.replace('3_', '5_')
    name_out = name_out0.replace('.csv', "")
    fieldnames = ['Category','Division', 'Population']  # hard code the fieldnames
    with open(f"{dir}{scenario_name}/5_{p}/{name_out}_demographics.csv", 'w', newline='') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()
        for k1, v1 in out.items():
            for k2, v2 in v1.items():
                entry = {}
                entry['Category'] = k1  # Add the scenario name to output
                entry['Division'] = k2  # Add the demographic group name to output
                entry['Population'] = f"{v2:,}"
                writer.writerow(entry)

def write_quartiles(dir, scenario_name, file, group_name, out, p):
    name_out0 = file.replace('3_', '5_')
    name_out = name_out0.replace('.csv', "")
    fieldnames = ['Scenario',f'{group_name}', '25%', '50%','75%','95%']  # hard code the fieldnames
    with open(f"{dir}{scenario_name}/5_{p}/{name_out}_{group_name}.csv", 'w', newline='') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()
        for k1, v1 in out.items():
            for k2, v2 in v1.items():
                entry = {}
                for k3, v3 in v2.items():
                    entry['Scenario'] = k1  # Add the scenario name to output
                    entry[f'{group_name}'] = k3  # Add the demographic group name to output
                    entry.update(v3)  # Append the results dictionary to the entry dictionary
                    writer.writerow(entry)

def write_rac_impact(dir, scenario_name, file, group_name, out, p):
    name_out0 = file.replace('3_', '5_')
    name_out = name_out0.replace('.csv', "")
    fieldnames = [f'{group_name}', '15 min closer', '10 min closer', '5 min closer',
                  '5 min farther', '10 min farther', '15 min farther']
    with open(f"{dir}{scenario_name}/5_{p}/{name_out}_{group_name}_impact.csv", 'w', newline='') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()
        for k1, v1 in out.items():
            for k2, v2 in v1.items():
                entry = {}
                entry[f'{group_name}'] = k2
                v2_reorder = {x: v2[x] for x in fieldnames[1:]}  # reorder output dictionary based on fieldname order
                entry.update(v2_reorder)
                writer.writerow(entry)

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    print(datetime.now())

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-dir', '--DIRECTORY_PATH', required=True,
                        default=None)  # Path from root to where the folders are
    args = parser.parse_args()
    dir = args.DIRECTORY_PATH

    print("---Notes about this program---")
    print("-0-Default RAC file is from 2017")
    print("-0-User should be prepared with a demographics.json file which associates categories with worker groups and output field naming")
    # print("-0-Assumes dual access is calculated for 10 destination")
    print("-0-User should select the number of destinations to calculate worker statistics on")
    print("-0-User should select the subset of blocks to apply worker statistics calculation on")
    print("-0-TT for each worker quartile (25%, 50%, etc) is calculated")
    print("-0-TT change is hard coded at +/- 5, 10, 15 minutes")
    print("-0-Assumes GEOID10 is the unit of analysis, check input files")
    print("\n")

    p = input('List of pois to calculate dual accessibility for, i.e. grocery, health, es, ms, hs, etc. : ').split()
    print("\n")
    pois = make_lists(p)
    print(pois)

    dest_num = input("Type number of destinations to calculate worker statistics on (2): ") or 2

    print(f"-----Loading RAC data----- \n")
    rac = '/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2017/Joined_RAC_FILE_2017_C000_CS02.csv'
    print("Loading RAC data from: ", rac)
    rac_dict = load_rac(rac)
    print('\n')

    print(f"-----Loading demographic.json config----- \n")
    demo_config = load_demographic_config(dir)

    print("-----Making list of folder objects----- \n")
    folder_list = make_folder_list(dir)

    print("-----Making list of subset files----- \n")
    subset_list, subset_short_name = make_subset_list(dir)
    print(subset_short_name)
    s = input("Select one or more subsets for which worker statistics tables will be produced (METRO): ").split()
    subsets = make_lists(s)

    # Main program
    # Provide file input to tell the program which folders should be accessed for the results
    with open(f'{dir}/comparisons.csv') as csv_file:
        comparisons = csv.reader(csv_file)
        next(comparisons)
        for row in comparisons:
            print("comparison row: ", row)
            f = return_folder_obj(folder_list, row)
            f.move_into_folder()
            f.make_dirs(pois)
            for p in pois:  # All of the aggregations are repeated separately for each POI type
                f.make_demographics_table(subsets, rac_dict, demo_config)
                f.calc_rac_quartiles(subsets, rac_dict, demo_config, dest_num, p)
                f.calc_rac_impact(subsets, rac_dict, demo_config, dest_num, p)
            f.move_outof_folder()

    print(f"-----Aggregation Complete----- \n")
    print(datetime.now())
    os.system('say "Aggregation complete"')

