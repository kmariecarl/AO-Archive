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

    def unzip_BZ2(self):
        print("\n -----Message: Unzipping bz2----- \n")
        bash_command = f"bunzip2 -k {self.path}.bz2"
        run_command(bash_command)

    def summarize_results(self):
        print("\n -----Loading data to database----- \n")
        bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/ttMatrixTools/1_summarizeTTResults.py " \
                       f"-run_type TT -db kristincarlson -schema public " \
                       f"-table_in {self.scenario_name} -table_out {self.scenario_name}_deciles -path {self.path}"
        run_command(bash_command)

    def calc_dual_access(self, poi_list):
        os.chdir(f'{self.dir}{self.scenario_name}/')  # Move in and out of scenario folder to place results there
        for p in poi_list:
            print(f"\n -----Calculating dual accessibility for folder {self.scenario_name} and POI: {p}----- \n")
            bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/ttMatrixTools/" \
                           f"2_calcDualAccessFromDB.py -run_type TT -db kristincarlson -schema public" \
                           f" -deciles_table {self.scenario_name}_deciles -percentile _50th -limit 10" \
                           f" -poi_table poi_{p} -load_poi_YN N -id fid"
            run_command(bash_command)
        os.system(f'{self.dir}')



def processChange(bs_obj, updt_obj, bs, updt, poi_list):
    print(f"\n -----Calculating the difference between scnearios {bs} and {updt}----- \n")
    os.chdir(f'{bs_obj.dir}{bs_obj.scenario_name}/')
    for p in poi_list:
        bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/ttMatrixTools/" \
                       f"3_processDualAccessChange.py -bs 1_{bs_obj.scenario_name}_*_{p}.csv -bs_short {bs_obj.short_name}" \
                       f" -updt ../{updt_obj.scenario_name}/1_{updt_obj.scenario_name}_*_{p}.csv" \
                       f" -updt_short {updt_obj.short_name} -other_name {p}"
        run_command(bash_command)
    os.chdir(bs_obj.dir)



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
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()
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
    print("\n")
    pois = make_lists(p)

    dir = args.DIRECTORY_PATH

    print("-----Making list of folder objects----- \n")
    with open(f'{dir}/folder_list.json') as json_file:
        folder_names = json.load(json_file)
        folder_list = []
        for key, name in folder_names.items():
            folder_list.append(FolderObject(dir, name))

    # mybar = ProgressBar(len(folder_list))
    # for f in folder_list:
    #     f.unzip_BZ2()  # Uncomment after finishing Gold Rush work
    #     f.summarize_results()
    #     f.calc_dual_access(pois)  # This should complete before running comparisons
    #     mybar.add_progress()
    #     print(f"\n -----Scenario {f.scenario_name} finished processing----- \n")
    # mybar.end_progress()
    # print("\n -----All scenarios have been loaded, deciles calculated, and dual accessibility calculated----- \n")

    # Provide file input to tell the program which scenarios should be compared
    with open(f'{dir}/comparisons.csv') as csv_file:
        comparisons = csv.reader(csv_file)
        next(comparisons)
        for row in comparisons:
            print(row)
            for f in folder_list:
                print(f"folder: {f.scenario_name}")
                if row[0] in f.scenario_name:
                    bs_obj = f
                    print(f"Base folder found: {bs_obj.scenario_name}")
                if row[1] in f.scenario_name:
                    updt_obj = f
                    print(f"Updt folder found: {updt_obj.scenario_name}")
            processChange(bs_obj, updt_obj, row[0], row[1], pois)
    print("-----Comparisons finished-----\n")
    print(datetime.now())
    os.system('say "Comparisons complete"')






