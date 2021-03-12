# TIRP non-work destination workflow

# This program reads in csv files from dual2_assembleResults.py along with the
# LEHD RAC C000 (all worker) values to begin aggregating the dual accessibility results over the metro worker population
# and other groupings such as per city, per county groupings.

# This script was updated on 12/16/2020 to be compatible with the output of 'dual3b_aggregateByWorkers.py' program
# additions include revised 10%-90% distribution output, and 'Total workers', 'No change' 'Total closer', 'Total farther'
# and 'Average change (min)' additions.


# This program can be run from the outer-most directory of your project and it will ask you for additional information
# using prompts.

#################################
#           IMPORTS             #
#################################

import argparse
from datetime import datetime
from progress import bar
import os
import json
import subprocess
import sys
import csv
import fnmatch
import operator
from numpy import average



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
        # print("Make directories for ", poi_list)
        for p in poi_list:
            if not os.path.exists(f"{self.dir}{self.scenario_name}/4_{p}"):
                os.makedirs(f"{self.dir}{self.scenario_name}/4_{p}")

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

    def make_subset_files(self, subset_list, p):
        f = find_file(f"2_*_{p}.csv")  # Locate original results file to break up into subsets
        for s in subset_list:
            print(f"Using subset file {s} for {f}")
            bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/ttMatrixTools/0_processResultsSubset.py" \
                f" -access {f} -subset {s}"
            run_command(bash_command)

    def make_population(self):
        for id, path_list in group_dict.items():
            pop_out = []
            group_data = load_group_data(id, path_list[0])
            group_names = load_group_name(id, path_list[1])
            file = find_file(f"2_*_grocery.csv")  # <- this is a bad setup
            prefix = [x for x in file.split('_')[2:4]]  # grabs the scenarios included in the file name, 'base', 'bde'
            data = self.load_data(f'{self.dir}{self.scenario_name}/{file}', prefix)
            unique_group = locate_unique_groups(data, group_data)

            for u in unique_group:
                if u in group_names:
                    rac_local = calc_rac_local(u, group_data, rac_dict, rac_field, group_names[u])
                    pop_out.append([group_names[u], rac_local])  # create population table
            write_population(self.dir, self.scenario_name, pop_out, id)

    # output data should be:
    # [1,bs,8,15,22,60]
    # [1,updt,7,14,20,60]
    # [2,bs,12,20,26,60]
    def calc_rac_percentiles(self, subset_short_name, rac_dict, rac_field, p):
        dest_limit = list(range(1,4,1))  # Limiting output to 3 destinations
        for s in subset_short_name:  # e.x. METRO, BLINE, DLINE, ELINE
            out = {}
            file = find_file(f"3_*_{p}_{s}_*.csv")
            prefix = [x for x in file.split('_')[2:4]]  #grabs the scenarios included in the file name, 'base', 'bde'
            file_str = f"{self.dir}{self.scenario_name}/{file}"
            data = self.load_data(file_str, prefix)
            rac_total = total_rac(data, rac_field, rac_dict, s)
            tile_dict = make_percentiles(rac_total)
            for prx in prefix:
                out[f'{prx}'] = {}
                for d in dest_limit:
                    out[f'{prx}'][d] = {}
                    tup_list = []
                    for k, v in data.items():
                        if k in rac_dict.keys():
                            tup_list.append((data[k][f'{prx}_{d}'], rac_dict[k][rac_field]))  # appending the tt found in the baseline_1 dest and the matching rac value
                    tup_list.sort(key=operator.itemgetter(0))  # sort tuple order by travel time
                    # select quartile, search tuple list until the sum of the rac values equals or exceeds the quartile
                    for tile_name, tile_val in tile_dict.items():
                        rac_sum = 0
                        for i in tup_list:  # i is the travel time
                            rac_sum = rac_sum + i[1]
                            if rac_sum >= tile_val:  # rac_sum should equal or just exceed the percentile value
                                # tt value needed for 25%, etc. of workers to reach 1 grocery on the baseline network...
                                out[f"{prx}"][d][tile_name] = int(i[0]/60)  #convert to minutes and round to nearest int
                                break
            write_percentiles(self.dir, self.scenario_name, file, out, p)
            # print("Wrote total rac percentile travel times for file: ", file)

    # output data should be:
    # [dest_num, -5min, -10min, -15min, +5min, +10min, +15min]
    # [1, 450 (27%), 20 (0.1%), 2 (0.01%), 49 (13%), 15 (4%), 1 (0.3%)]
    # [2, ...]
    def calc_rac_impact(self, subset_short_name, rac_dict, rac_field, p):
        dest_limit = list(range(1,4,1))  # Limiting output to 3 destinations
        # print("Dest limit vals: ", dest_limit)
        for s in subset_short_name:  # e.x. METRO, BLINE, DLINE, ELINE
            out = {}
            file = find_file(f"3_*_{p}_{s}_*.csv")
            prefix = [x for x in file.split('_')[2:4]]  # grabs the scenarios included in the file name, 'base', 'bde'
            file_str = f"{self.dir}{self.scenario_name}/{file}"
            data = self.load_data(file_str, prefix)
            rac_total = total_rac(data, rac_field, rac_dict, s)
            # sum all rac associated with blocks that changed by +0–5 min, -0–5 min, etc
            tt_buckets = [300, 600, 900]
            for d in dest_limit:
                out[d] = {}
                rac_sum_neg_total = 0
                rac_sum_pos_total = 0
                origins_in_region = []   # this list is used in the 'calc_avg_chg_tt' function
                for tt in tt_buckets:
                    rac_sum_pos = 0
                    rac_sum_neg = 0
                    for origin in data.keys():
                        if origin in rac_dict.keys():  # rac_dict does not contain water body origins, so this checks removes water bodies from the access impact calculations
                            origins_in_region.append(origin)  # this list is used in the 'calc_avg_chg_tt' function
                            if tt == 300:  # The first bucket should be adjusted to 1–5 minutes (showing less than 1 min change isn't useful)
                                # Look for negative tt change between the range given, i.e. -60 – -300
                                if data[origin][f'abschg{d}'] < ((tt + 60) * -1) + 300 and data[origin][f'abschg{d}'] >= tt * -1:
                                    rac_sum_neg = rac_sum_neg + rac_dict[origin][rac_field]
                                # Look for positive tt change between the range given, i.e. -300 – -600
                                if data[origin][f'abschg{d}'] > (tt + 60) - 300 and data[origin][f'abschg{d}'] <= tt:
                                    rac_sum_pos = rac_sum_pos + rac_dict[origin][rac_field]
                            else:
                                # Look for negative tt change between the range given, i.e. -301 – -600
                                if data[origin][f'abschg{d}'] < (tt * -1) + 300 and data[origin][f'abschg{d}'] >= tt * -1:
                                    rac_sum_neg = rac_sum_neg + rac_dict[origin][rac_field]
                                # Look for positive tt change between the range given, i.e. -300 – -600
                                if data[origin][f'abschg{d}'] > tt - 300 and data[origin][f'abschg{d}'] <= tt:
                                    rac_sum_pos = rac_sum_pos + rac_dict[origin][rac_field]
                    out[d][f'{int(tt/60)} min closer'] = f"{rac_sum_neg:,}" + " (" + f"{round((rac_sum_neg/rac_total)*100, 1)}" + "%)"
                    out[d][f'{int(tt/60)} min farther'] = f"{rac_sum_pos:,}" + " (" + f"{round((rac_sum_pos/rac_total)*100, 1)}" + "%)"
                    rac_sum_neg_total = rac_sum_neg_total + rac_sum_neg
                    rac_sum_pos_total = rac_sum_pos_total + rac_sum_pos
                out[d]["No change"] = f"{round((1 - (rac_sum_neg_total / rac_total) - (rac_sum_pos_total / rac_total)) * 100, 1)}" + "%"
                out[d]["Total workers"] = f"{rac_total:,}"
                out[d]["Total closer"] = f"{round((rac_sum_neg_total / rac_total) * 100, 1)}" + "%"
                out[d]["Total farther"] = f"{round((rac_sum_pos_total / rac_total) * 100, 1)}" + "%"
                out[d]["Average change (min)"] = calc_avg_chg_tt(data, origins_in_region, rac_dict, rac_field, d)  # calculate the average tt change for only the blocks that experience change, i.e. exclude "no change"
            write_rac_impact(self.dir, self.scenario_name, file, out, p)

    # output data should be:
    # [dest_num, name, -5min, -10min, -15min, +5min, +10min, +15min]
    # [1, Fridley, 450 (27%), 20 (0.1%), 2 (0.01%), 49 (13%), 15 (4%), 1 (0.3%)]
    # [1, Minneapolis ...]
    # [2, Fridley, 450 (27%), 20 (0.1%), 2 (0.01%), 49 (13%), 15 (4%), 1 (0.3%)]
    # [2, Minneapolis ...]
    def ctu_county_table(self, group_dict, rac_dict, rac_field, p):
        dest_limit = list(range(1,4,1))  # Limiting output to 3 destinations
        # for each group type, load the data and names
        for id, path_list in group_dict.items():
            group_data = load_group_data(id, path_list[0])
            group_names = load_group_name(id, path_list[1])

            out = {}
            file = find_file(f"2_*_{p}.csv")
            prefix = [x for x in file.split('_')[2:4]]  # grabs the scenarios included in the file name, 'base', 'bde'
            data = self.load_data(f'{self.dir}{self.scenario_name}/{file}', prefix)
            unique_group = locate_unique_groups(data, group_data)
            # sum all rac associated with blocks that changed by +0–5 min, -0–5 min, etc
            tt_buckets = [300, 600, 900]
            for u in unique_group:
                if u in group_names:
                    out[group_names[u]] = {}
                    for d in dest_limit:
                        out[group_names[u]][d] = {}
                        rac_local = calc_rac_local(u, group_data, rac_dict, rac_field, group_names[u])
                        origins_in_group = get_matching_origins(u, group_data, data)
                        rac_sum_neg_total = 0
                        rac_sum_pos_total = 0
                        for tt in tt_buckets:
                            rac_sum_pos = 0
                            rac_sum_neg = 0
                            for origin in origins_in_group:
                                if origin in rac_dict.keys():  # In case water bodies were included in original set of origins
                                    if tt == 300:  # The first bucket should be adjusted to 1–5 minutes (showing less than 1 min change isn't useful)
                                        # Look for negative tt change between the range given, i.e. -301 – -600
                                        if data[origin][f'abschg{d}'] < ((tt + 60) * -1) + 300 and data[origin][f'abschg{d}'] >= tt * -1:
                                            rac_sum_neg = rac_sum_neg + rac_dict[origin][rac_field]
                                        # Look for positive tt change between the range given, i.e. -300 – -600
                                        if data[origin][f'abschg{d}'] > (tt + 60) - 300 and data[origin][f'abschg{d}'] <= tt:
                                            rac_sum_pos = rac_sum_pos + rac_dict[origin][rac_field]
                                    else:
                                        # Look for negative tt change between the range given, i.e. -301 – -600
                                        if data[origin][f'abschg{d}'] < (tt * -1) + 300 and data[origin][f'abschg{d}'] >= tt * -1:
                                            rac_sum_neg = rac_sum_neg + rac_dict[origin][rac_field]
                                        # Look for positive tt change between the range given, i.e. -300 – -600
                                        if data[origin][f'abschg{d}'] > tt - 300 and data[origin][f'abschg{d}'] <= tt:
                                            rac_sum_pos = rac_sum_pos + rac_dict[origin][rac_field]
                            out[group_names[u]][d][f'{int(tt / 60)} min closer'] = f"{rac_sum_neg:,}" + " (" + f"{round((rac_sum_neg/rac_local)*100, 1)}" + "%)"
                            out[group_names[u]][d][f'{int(tt / 60)} min farther'] = f"{rac_sum_pos:,}" + " (" + f"{round((rac_sum_pos/rac_local)*100, 1)}" + "%)"
                            rac_sum_neg_total = rac_sum_neg_total + rac_sum_neg
                            rac_sum_pos_total = rac_sum_pos_total + rac_sum_pos
                        out[group_names[u]][d]["No change"] = f"{round((1 - (rac_sum_neg_total / rac_local) - (rac_sum_pos_total / rac_local)) * 100, 1)}" + "%"
                        out[group_names[u]][d]["Total workers"] = f"{rac_local:,}"
                        out[group_names[u]][d]["Total closer"] = f"{round((rac_sum_neg_total / rac_local) * 100, 1)}" + "%"
                        out[group_names[u]][d]["Total farther"] = f"{round((rac_sum_pos_total / rac_local) * 100, 1)}" + "%"
                        out[group_names[u]][d]["Average change (min)"] = calc_avg_chg_tt(data, origins_in_group, rac_dict, rac_field, d)  # calculate the average tt change for only the blocks that experience change and only those in the CTU, i.e. exclude "no change"
                else:
                    print(f"Warning: Group {u} does not have text in Group Name file")
            write_ctu_county_table(self.dir, self.scenario_name, file, out, id, p)


#################################
#           FUNCTIONS           #
#################################
def make_lists(input):
    out = []
    for i in input:
        out.append(str(i))
    return out

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

def make_group_file_dict(dir):
    # Makes a list of path strings to each GROUP file
    with open(f'{dir}/group_list.json') as json_file:
        group_dict = json.load(json_file)
        # print(group_dict)
    return group_dict

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
        # print("rac fieldnames: ", column_names)
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
    # print(f"Total workers found in analysis region {s}: {total}")
    return total

def calc_rac_local(group, group_data, rac_dict, rac_field, group_name):
    rac_local = 0
    for geoid10, group_id in group_data.items():  # {GEOID10: GROUPID}
        if geoid10 in rac_dict.keys():  # again need to make sure origin is not a water body
            if group_id == group:
                rac_local = rac_local + rac_dict[geoid10][rac_field]   # Rac_dict {GEOID10: rC000}
    # print(f"Total workers found in analysis region {group_name}: {rac_local}")
    return rac_local

# This group_dict does not have headers
def load_group_data(id, group_data_path):
    out = {}
    with open(f"{group_data_path}") as csvin:
        reader = csv.DictReader(csvin)
        for row in reader:
            out[row['GEOID10']] = row[f'{id}']
    return out

# This group name dict does not have headers
def load_group_name(id, group_name_path):
    # print('group name path: ', group_name_path)
    out = {}
    with open(f"{group_name_path}", encoding='utf-8-sig') as csvin:
        reader = csv.DictReader(csvin)
        for row in reader:
            out[row[f'{id}']] = row['NAME']
    # print('group names: ', out.values())
    return out

# Look at all of the origins and make the unique list out of all the CTUs/Counties that match with the origins
def locate_unique_groups(data, group_data):  # data = 2_*_{p}.csv file, group_data = GROUP_1_ID_GNISID.csv or another group file
    unique_group = []
    for origin in data.keys():
        if origin in group_data.keys():
            if group_data[origin] not in unique_group:
                unique_group.append(str(group_data[origin]))
        # else:
            # print(f"Warning: origin {origin} is not in the group_data file, could be a water body.")
    if "" in unique_group:
        unique_group.remove("")
    # print("unique groups: ", unique_group)
    return unique_group

def get_matching_origins(u, group_data, data):  # u is the city/county ID, group_data is geoid & city/countyid, data are dual access results
    origin_list = []
    for geoid10, groupid in group_data.items():
        if groupid == u and geoid10 in data:
            origin_list.append(geoid10)
    return origin_list




# calculates the average worker-weighted change in tt for blocks (excluding change < 1min)
def calc_avg_chg_tt(data, origin_list, rac_dict, rac_field, d):
    tt_list = []
    rac_list = []
    for origin in origin_list:
        if origin in rac_dict.keys():
            # do not include change less than 1 min
            if data[origin][f'abschg{d}'] < -60 or data[origin][f'abschg{d}'] > 60:
                tt_list.append(data[origin][f'abschg{d}'])
                rac_list.append(rac_dict[origin][rac_field])
    if sum(rac_list) == 0:  # if an region (i.e. city, county, etc.) experiences tt changes but nobody lives there,
                            # then avg_chg = 0
        avg_chg = 0
    else:
        avg_chg = round((average(tt_list, weights=rac_list) / 60), 1)
    return avg_chg


def make_percentiles(total_rac):
    pt = {'10%': 0.10 * total_rac,
          '25%': 0.25 * total_rac,
          '50%': 0.50 * total_rac,
          '75%': 0.75 * total_rac,
          '90%': 0.90 * total_rac}
    return pt  # return dictionary of keys and values

def write_percentiles(dir, scenario_name, file, out, p):
    name_out0 = file.replace('3_', '4_')
    name_out = name_out0.replace('.csv', "")
    fieldnames = ['Scenario', 'Locations', '10%', '25%', '50%', '75%', '90%']  # hard code the fieldnames
    with open(f"{dir}{scenario_name}/4_{p}/{name_out}.csv", 'w', newline='') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()
        for k1, v1 in out.items():
            entry = {}
            for k2, v2 in v1.items():
                entry['Scenario'] = k1
                entry['Locations'] = k2
                entry.update(v2)
                writer.writerow(entry)

def write_rac_impact(dir, scenario_name, file, out, p):
    name_out0 = file.replace('3_', '4_')
    name_out = name_out0.replace('.csv', "")
    fieldnames = ['Locations', 'Total workers', '15 min closer', '10 min closer', '5 min closer', 'No change',
                  '5 min farther', '10 min farther', '15 min farther', 'Total closer', 'Total farther', 'Average change (min)']
    with open(f"{dir}{scenario_name}/4_{p}/{name_out}_worker_impact.csv", 'w', newline='') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()
        for k, v in out.items():
            entry = {}
            entry['Locations'] = k
            v_reorder = {x: v[x] for x in fieldnames[1:]}  # reorder output dictionary based on fieldname order
            entry.update(v_reorder)
            writer.writerow(entry)

def write_ctu_county_table(dir, scenario_name, file, out, id, p):
    name_out0 = file.replace('2_', '4_')
    name_out = name_out0.replace('.csv', "")
    fieldnames = ['Name', 'Locations', 'Total workers', '15 min closer', '10 min closer', '5 min closer', 'No change',
                  '5 min farther', '10 min farther', '15 min farther', 'Total closer', 'Total farther', 'Average change (min)']  # hard code the fieldnames
    with open(f"{dir}{scenario_name}/4_{p}/{name_out}_{id}.csv", 'w', newline='') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()
        for k1, v1 in out.items():
            entry = {}
            for k2, v2 in v1.items():
                v2_reorder = {x: v2[x] for x in fieldnames[2:]}
                entry['Name'] = k1
                entry['Locations'] = k2
                entry.update(v2_reorder)
                writer.writerow(entry)

def write_population(dir, scenario_name, pop_out, id):
    # name_out0 = file.replace('2_', '4_')
    # name_out = name_out0.replace('.csv', "")
    fieldnames = ['Area', 'Population']  # hard code the fieldnames
    pop_out.sort(key=lambda x: x[0])  # sort information alphabetically
    with open(f"{dir}{scenario_name}/{id}_pop.csv", 'w', newline='') as csvout:
        writer = csv.writer(csvout)
        writer.writerow(fieldnames)
        writer.writerows(pop_out)

# Solution from https://stackoverflow.com/questions/32510464/using-subprocess-to-get-output
def run_command(bash_command):
    subprocess.Popen(bash_command.split(), stdout=sys.stdout, stderr=sys.stderr).communicate() # stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


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
    print("-0-Assumes dual access is calculated for 10 destination")
    print("-0-Output is hard coded at 3 destinations max, beyond that we are not calculating summary statistics")
    print("-0-TT for each worker percentiles: 10%, 25%, 50%, 75%, 90% are calculated")
    print("-0-TT change is hard coded at +/- 5, 10, 15 minutes")
    print("-0-Assumes GEOID10 is the unit of analysis, check input files")
    print("\n")


    p = input('List of pois to calculate dual accessibility for, i.e. grocery, health, es, ms, hs, etc. : ').split()
    print("\n")
    pois = make_lists(p)
    print(pois)

    print(f"-----Loading RAC data----- \n")
    rac = '/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2017/Joined_RAC_FILE_2017_C000_CS02.csv'
    print("Loading RAC data from: ", rac)
    rac_dict = load_rac(rac)
    rac_field = input("Type total worker/population field name (rC000): ") or "rC000"
    print('\n')

    print("-----Making list of folder objects----- \n")
    folder_list = make_folder_list(dir)

    print("-----Making list of subset files----- \n")
    subset_list, subset_short_name = make_subset_list(dir)

    print("-----Making list of group files----- \n")
    group_dict = make_group_file_dict(dir)  # makes a dict with key = name of identifier, i.e GNISID, COUNTYFP,
                                            # and value is a list of the GEOID10->Identifier mapping file (index 0)
                                            # and Identifier to Names file (index 1)

    # Main program
    # Provide file input to tell the program which folders should be accessed for the change results
    num_lines = sum(1 for line in open(f'{dir}/comparisons.csv')) -1
    with open(f'{dir}/comparisons.csv') as csv_file:
        comparisons = csv.reader(csv_file)
        next(comparisons)
        mybar = ProgressBar(num_lines)
        for row in comparisons:
            print("comparison row: ", row)
            f = return_folder_obj(folder_list, row)
            f.move_into_folder()
            f.make_dirs(pois)
            f.make_population()
            for p in pois:  # All of the aggregations are repeated separately for each POI type
                f.make_subset_files(subset_list, p)
                f.calc_rac_percentiles(subset_short_name, rac_dict, rac_field, p)
                f.calc_rac_impact(subset_short_name, rac_dict, rac_field, p)
                f.ctu_county_table(group_dict, rac_dict, rac_field, p)
            f.move_outof_folder()
            mybar.add_progress()
        mybar.end_progress()

    print(f"-----Aggregation Complete----- \n")
    print(datetime.now())
    os.system('say "Aggregation complete"')

