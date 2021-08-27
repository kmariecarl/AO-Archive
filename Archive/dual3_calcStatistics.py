# This program follows the 'generateScenarios.py' script and prepares the raw results files for processing. The program
# is currently written to acommodate only the TT dual access workflow. For the primal accessibility workflow, please
# use 'assembleResults.py'



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
        self.short_name = name.split('_')[2]
        self.results_name = name + "-results.csv"
        self.path = f'{self.dir}{self.scenario_name}/{self.results_name}'

    def result_subset(self, subsets, poi_list):
        os.chdir(f'{self.dir}{self.scenario_name}/')  # Move in and out of scenario folder to place results there
        for p in poi_list:
            f = find_file(f'2_change_*_{p}.csv')
            for s in subsets:
                print(f"Message: Processing subset results: {s}")
                bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/processResultsSubset.py " \
                               f"-access {self.dir}{self.scenario_name}/{f} -subset {self.dir}{s}"
                run_command(bash_command)
        os.chdir(self.dir)

    def all_ww_results(self, poi_list):
        print("Message: Computing worker-weighted statistics for all blocks")
        os.chdir(f'{self.dir}{self.scenario_name}/')  # Move in and out of scenario folder to place results there
        rac = "/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2017/Joined_RAC_FILE_2017.csv"
        for p in poi_list:
            f = find_file(f'2_change_*_{p}.csv')
            bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/workWeightAccess.py" \
                f" -access {self.dir}{self.scenario_name}/{f} -rac {rac} -or_lab origin -rac_lab GEOID10 -worker_lab rC000 -all"
            run_command(bash_command)
        os.chdir(self.dir)

    def group_ww_results(self, poi_list):
        print("Message: Computing worker-weighted statistics for grouped blocks")
        flag = find_file('GROUP*')
        if flag:
            num = int(input("Number of grouping types, i.e. 1, 2, 3, etc."))
            while num > 0:
                for p in poi_list:
                    id = find_file(f'GROUP_{num}_ID*')
                    group_id = id.split('_')[-1].split('.')[0]
                    print(f"Group id: {group_id}")
                    rac = "/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2017/Joined_RAC_FILE_2017.csv"
                    os.chdir(f'{self.dir}{self.scenario_name}/')  # Move in and out of scenario folder to place results there
                    f = find_file(f'2_change_*_{p}.csv')
                    print(f'File to calculate grouped WW results from: {f}')
                    bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/workWeightAccess.py" \
                        f" -access {self.dir}{self.scenario_name}/{f} -rac {rac} -or_lab origin -rac_lab GEOID10 -worker_lab rC000 -group {self.dir}{id}" \
                        f" -map_lab GEOID10 -group_id {group_id}"
                    run_command(bash_command)
                    os.chdir(self.dir)
                num -= 1



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

# Solution from https://stackoverflow.com/questions/32510464/using-subprocess-to-get-output
def run_command(bash_command):
    proc = subprocess.Popen(bash_command, shell=True) # stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        outs, errs = proc.communicate()
        # print(outs)
        # print(errs)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()
        # print(outs)
        # print(errs)
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

    print("---Notes about this program---")
    print("-0-This program is currently only designed for the TT to Dual access workflow")
    print("-0-This program assumes an origin to exact destination location workflow. "
          "Meaning POI tables are not loaded to the db.")
    print("-0-This program selects the 50th percentile travel time.")
    print("-0-This program selects only the first 10 destinations for processing.")
    print("\n")

    p = input('List of pois to calculate dual accessibility for, i.e. grocery, health, edu, etc. : ').split()
    pois = make_lists(p)

    s = input('List of files names for processing subsets of results, i.e. SUBSET_HALFMILE.csv, etc.: ').split()
    subsets = make_lists(s)

    dir = args.DIRECTORY_PATH

    print("Making list of folder objects \n")
    with open(f'{dir}/folder_list.json') as json_file:
        folder_names = json.load(json_file)
        folder_list = []
        for key, name in folder_names.items():
            folder_list.append(FolderObject(dir, name))
    for f in folder_list:
        f.result_subset(subsets, pois)
        f.all_ww_results(pois)
        f.group_ww_results(pois)


        # Currently the program tries to go into all folders but a lot of the summaries only need to happen in the base folder
