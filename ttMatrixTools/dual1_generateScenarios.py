# This program is designed to create a sequence of scenario evaluation folders based on input from the user, this
# program will also run the jobs through AWS

# Assumes the scnearios are for transit, a.k.a. departure times in config file set to 1 minute
# Assumes 60 minutes maxtime

# User should have a .json file for any scenarios that should be excluded because they were previously calculated and will
# be used in downstream processing

# This program makes a folder_list.json file at the end of the program to show all of the folders that were sent to AWS
# This file is used in dual2_assembleResults.py

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
    def __init__(self, dir, tag, time, scenario, other_cat):
        self.dir = dir
        self.tag = tag
        self.time = time
        self.scenario = scenario
        self.other_cat = other_cat

        if self.tag == 'tt':
            self.batch_command = 'matrix'
        else:
            self.batch_command = 'analyze'

        if self.other_cat is None:
            self.folder_name = f"{self.tag}_{self.time}_{self.scenario}"
            self.path = os.path.join(f"{self.dir}", f"{self.folder_name}")
        else:
            self.folder_name = f"{self.tag}_{self.time}_{self.scenario}_{self.other_cat}"
            self.path = os.path.join(f"{self.dir}", f"{self.folder_name}")
        if self.time == '79':
            self.timestart = "7:00 AM"
            self.timestop = "8:59 AM"
        if self.time == '111':
            self.timestart = "11:00 AM"
            self.timestop = "12:59 PM"
        if self.time == '46':
            self.timestart = "4:00 PM"
            self.timestop = "5:59 PM"
        if self.time == '79pm':
            self.timestart = "7:00 PM"
            self.timestop = "8:59 PM"

class GraphObject:
    def __init__(self, scenario):
        self.name = None
        self.scenario = scenario



#################################
#           FUNCTIONS           #
#################################
def make_lists(input):
    out = []
    for i in input:
        out.append(str(i))
    return out

def folders_to_exclude(dir):
    exclude = []
    with open(os.path.join(f"{dir}", 'folder_exclude.json'), 'r') as infile:
        exclude_dict = json.load(infile)
        for k,v in exclude_dict.items():
            exclude.append(v)
    return exclude


def make_graph_names(scenarios):
    graphs = []
    for s in scenarios:
        g = GraphObject(s)
        g.name = f"Graph_{s}"
        graphs.append(g)
    return graphs

def make_folders_w_cats(time_periods, scenarios, categories, exclude):
    folders = []
    for i in time_periods:
        for j in scenarios:
            for k in categories:
                if f"{front_tag}_{i}_{j}_{k}" in exclude:
                    print(f"Excluding: {front_tag}_{i}_{j}_{k}")
                else:
                    folder = FolderObject(dir, front_tag, i, j, k)
                    if not os.path.exists(folder.path):
                        os.mkdir(folder.path)
                        folders.append(folder)
    write_folder_list(folders)

    return folders

def write_folder_list(folders):
    # Write folder names out to json file to read into 'assembleResults2.py'
    outdict = {}
    for i in range(0, len(folders), 1):
        outdict[i] = folders[i].folder_name
    with open(os.path.join(f"{dir}", 'folder_list.json'), 'w') as outfile:
        json.dump(outdict, outfile, indent=4)

def make_folders_wout_cats(time_periods, scenarios, exclude):
    folders = []
    for i in time_periods:
        for j in scenarios:
            if f"{front_tag}_{i}_{j}" in exclude:
                print(f"Excluding: {front_tag}_{i}_{j}")
            else:
                folder = FolderObject(dir, front_tag, i, j, None)
                if not os.path.exists(folder.path):
                    os.mkdir(folder.path)
                    folders.append(folder)
    write_folder_list(folders)
    return folders

def folders_w_cats(folders, scenarios, categories, analyzer_str):
    graphs = make_graph_names(scenarios)

    print("adding origin files")
    origin_path = os.path.join(f"{args.ORIGINS_PATH}", "origins.*")
    for f in folders:
        os.system(f'cp -p {origin_path} {f.path}')
        print("adding destination files")
        for cat in categories:
            if cat == f.other_cat:
                destination_path = os.path.join(f"{args.DESTINATIONS_PATH}", f"{cat}.*")
                os.system(f'cp -p {destination_path} {f.path}')
                os.system(
                    f'cd {f.folder_name} && mmv {f.other_cat}\* destinations\#1 && cd ..')  # pattern for renaming files
        print("adding config files")
        with open(f'{f.dir}/tt_config_template.json') as json_file:
            config = json.load(json_file)
            config['firstDepartureTime'] = f.timestart
            config['lastDepartureTime'] = f.timestop
            with open(os.path.join(f"{args.DESTINATIONS_PATH}", f"{f.other_cat}.json")) as id_file:
                destination_id = json.load(id_file)
                config['destinationIDField'] = destination_id["destinationIDField"]
            os.system(f'cd {f.folder_name} && rm destinations.json && cd ..')
            config['outputPath'] = f"{f.folder_name}" + "-results.csv"
        with open(os.path.join(f"{f.path}", 'config.json'), 'w') as outfile:
            json.dump(config, outfile, indent=4)

        print("adding graph files")
        for g in graphs:
            if f.scenario == g.scenario:
                graph_path = os.path.join(f"{dir}", f"{g.name}", "Graph.obj")
                os.system(f'cp -p {graph_path} {f.path}')
        print("zipping folders")
        os.system(f'cd {f.folder_name} && zip {f.folder_name} * '
                  f'&& rm origins.* && rm destinations.* && rm config.json && rm Graph.obj && cd ..')
        submit_job(f, analyzer_str)

def folders_wout_cats(folders, scenarios, analyzer_str):
    graphs = make_graph_names(scenarios)
    print("adding origin files")
    origin_path = os.path.join(f"{args.ORIGINS_PATH}", "origins.*")
    for f in folders:
        print(f"Generating and Running Scenario: {f.folder_name}")
        os.system(f'cp -p {origin_path} {f.path}')
        print("adding destination files")
        destination_path = os.path.join(f"{args.DESTINATIONS_PATH}", "destinations.*")
        os.system(f'cp -p {destination_path} {f.path}')
        print("adding config files")
        with open(f'{f.dir}/tt_config_template.json') as json_file:
            config = json.load(json_file)
            config['firstDepartureTime'] = f.timestart
            config['lastDepartureTime'] = f.timestop
            with open(os.path.join(f"{args.DESTINATIONS_PATH}", f"destinations.json")) as id_file:
                destination_id = json.load(id_file)
                config['destinationIDField'] = destination_id["destinationIDField"]
            os.system(f'cd {f.folder_name} && rm destinations.json && cd ..')
            config['outputPath'] = f"{f.folder_name}" + "-results.csv"
        with open(os.path.join(f"{f.path}", 'config.json'), 'w') as outfile:
            json.dump(config, outfile, indent=4)
        print("adding graph files")
        for g in graphs:
            if f.scenario == g.scenario:
                graph_path = os.path.join(f"{dir}", f"{g.name}", "Graph.obj")
                os.system(f'cp -p {graph_path} {f.path}')
        print("zipping folders")
        os.system(f'cd {f.folder_name} && zip {f.folder_name} * '
                  f'&& rm origins.* && rm destinations.* && rm config.json && rm Graph.obj && cd ..')
        submit_job(f, analyzer_str)

def submit_job(f, analyzer_str):

    bc1 = f'{analyzer_str} submit {f.path}/{f.folder_name}'
    run_command(bc1)

    bc2 = f'{analyzer_str} {f.batch_command} {f.folder_name}'
    run_command(bc2)

    bc3 = f'{analyzer_str} results {f.folder_name}'
    run_command(bc3)


# Review line 198-220 of this NAE script https://github.umn.edu/AccessibilityObservatory/AOTools/blob/2019b/AOTTWorker/AOTTWorker.py
def run_command(bash_command):
    proc = subprocess.Popen(bash_command, shell=True) # stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        outs, errs = proc.communicate()
        print(outs)
        print(errs)
        print(datetime.now())
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()
        print(outs)
        print(errs)
        print(datetime.now())
#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    print(datetime.now())

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-dir', '--DIRECTORY_PATH', required=True,
                        default=None)  # Path from root to where the folders will be created
    parser.add_argument('-add_cat', '--ADDITIONAL_CATEGORIES_Y_N', required=False,
                        default=None)  # If there are additional ways to break down the scenarios, i.e. for destination
                                        # types like grocery, health, etc. type Y=yes or N=no
    parser.add_argument('-or', '--ORIGINS_PATH', required=False,
                        default=None)
    parser.add_argument('-dest', '--DESTINATIONS_PATH', required=False,
                        default=None)
    args = parser.parse_args()

    print("---Notes about this program---")
    print("-0-Watch this program until the first job has started to successfull by calculated")
    print("-0-Turn on AWS instance prior to running this program")
    print("\n")

    dir = args.DIRECTORY_PATH
    categories = None
    front_tag = input("Type the general category of analyses, i.e. access, tt, dual, primal, etc.: ")

    times = input('Space seperated list of time windows, i.e. 79am, 111, 46, 79pm, etc.: ').split()
    time_periods = make_lists(times)
    print('List of time periods for this analysis: \n', time_periods)

    scen = input('Space seperated list of scenario names, i.e. base, gold, goldrush, rush, etc.: ').split()
    scenarios = make_lists(scen)
    print('List of scenarios for this analysis: \n', scenarios)

    exclude = folders_to_exclude(dir)
    print(f"Folders to exclude: {exclude}")

    if args.ADDITIONAL_CATEGORIES_Y_N == "Y":
        cats = input('List of additional categories to enumerate folders by, i.e. grocery, health, edu, etc. : ').split()
        categories = make_lists(cats)
        print('List of additional categories to enumerate scenarios by for this analysis: \n', categories)

        analyzer_str = '/Users/kristincarlson/Dropbox/AOGit/Analyzer/myAnalyzer/analyzer.sh'

        folders = make_folders_w_cats(time_periods, scenarios, categories, exclude)
        folders_w_cats(folders, scenarios, categories, analyzer_str)
    else:
        analyzer_str = '/Users/kristincarlson/Dropbox/AOGit/Analyzer/myAnalyzer/analyzer.sh'

        folders = make_folders_wout_cats(time_periods, scenarios, exclude)
        folders_wout_cats(time_periods, scenarios, analyzer_str)








