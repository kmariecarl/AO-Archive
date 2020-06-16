# This script is the main driver for processing accessibility results after receiving results from AWS
# Run this script from the main directory containing all of the scenario folders
# The folder used in the -access command is the base for comparison, the "compare" folder and associated results file
# is instantiated as a separate object.


# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

import os
import sys
import argparse
import subprocess
import time
import fnmatch
from progress import bar
from myToolsPackage import matrixLinkModule as mod


# --------------------------
#       CLASSES
# --------------------------
class ProgressBar:
    def __init__(self, lines):
        self.bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=lines)
    def add_progress(self):
        self.bar.next()
    def end_progress(self):
        self.bar.finish()

class AccessFile:
    # Class attribute

    # Instance attribute
    def __init__(self, file_name):
        self.file_name = file_name

    # Instance methods

    def unzipBZ2(self):
        print("Message: Unzipping bz2")
        bash_command = f"bunzip2 -k {self.file_name}.csv.bz2"
        run_command(bash_command)

    def rezip(self):
        print("Message: Re-zipping file")
        bash_command = f"zip -9 {self.file_name} {self.file_name}.csv"
        run_command(bash_command)

    def move_results(self):
        print("Message: Making folders")
        bash_command = "mkdir Access_Results"
        run_command(bash_command)
        bash_command = "mkdir Access_Results_Avg"
        run_command(bash_command)
        bash_command = f"mv {self.file_name}.zip Access_Results/"
        run_command(bash_command)

    def average_results(self):

        access.unzipBZ2()
        bar.next()
        print("\n")

        access.rezip()
        bar.next()
        print("\n")

        access.move_results()
        bar.next()
        print("\n")

        print("Message: Averaging results")
        # bash_command = "python " \
        #               "/Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/averageTransitLocalAll.py" \
        #               " -r Access_Results/ -o Access_Results_Avg/ -access {}".format(access_label)
        bash_command = "python " \
                      "/Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/averageTransitLocalAll.py" \
                      " -r Access_Results/ -o Access_Results_Avg/"
        run_command(bash_command)

    def compare_access(self, compare_to_folder, compare_to_file):
        print("Message: Building comparison directory")
        bash_command = "mkdir Comparisons"
        run_command(bash_command)
        bash_command = f"cp Access_Results_Avg/1-{self.file_name}-avg.csv Comparisons"
        run_command(bash_command)

        bash_command = f"cp ../{compare_to_folder}/Access_Results_Avg/1-{compare_to_file}-avg.csv Comparisons"
        run_command(bash_command)

        os.chdir("Comparisons/")
        print("CWD: ", os.getcwd())

        base = find_file(f"1-{self.file_name}-avg.csv")
        updt = find_file(f"1-{compare_to_file}-avg.csv")

        print("Base File: ", base)
        print("Update File: ", updt)

        bash_command = f"python " \
                      f"/Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/processAccessResults.py" \
            f" -bs {base} -updt {updt} -access wC000"
        run_command(bash_command)

    def halfm_results(self, processed, blocks):
        print("Message: Processing accessibility results within half mile of transit stops")
        bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/processAccessResultsHalfMile.py " \
            f"-access {processed} -blocks {blocks}"
        run_command(bash_command)

    def all_ww_results(self, processed, rac):
        print("Message: Computing worker-weighted statistics for all blocks")
        bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/workWeightAccess.py" \
            f" -access {processed} -rac {rac} -or_lab label -rac_lab GEOID10 -worker_lab rC000 -all"
        run_command(bash_command)

    def group_ww_results(self, processed, rac, group):
        print("Message: Computing worker-weighted statistics for grouped blocks")
        bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/workWeightAccess.py" \
            f" -access {processed} -rac {rac} -or_lab label -rac_lab GEOID10 -worker_lab rC000 -group {group}" \
            f" -map_lab GEOID10 -group_id GNIS_ID"
        run_command(bash_command)

    def make_pretty_tables(self, processed):
        print("Message: Making pretty tables for global averages")
        bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/prettyWWTables.py" \
            f" -access {processed}"
        run_command(bash_command)

    def make_pretty_ctu_tables(self, processed, rac, ctu, ctu_names):
        print("Message: Making pretty tables for top N ctus")
        bash_command = f"python /Users/kristincarlson/Dropbox/Programs/gitBus-Highway/workflowAOTools/prettyWWTables.py" \
            f" -access {processed} -rac {rac} -ctu {ctu} -ctu_names {ctu_names}"
        run_command(bash_command)

# --------------------------
#       GENERAL FUNCTIONS
# --------------------------

# Solution from https://stackoverflow.com/questions/32510464/using-subprocess-to-get-output
def run_command(bash_command):
    # process = subprocess.Popen(bash_command.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # output, error = process.communicate()
    # print(error)
    # print(output)

    subprocess.Popen(bash_command.split(), stdout=sys.stdout, stderr=sys.stderr).communicate() # stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)



# Solution from https://kite.com/python/examples/4293/os-get-the-relative-paths-of-all-files-and-subdirectories-in-a-directory
def find_file(string):
    my_file = None
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, f"{string}"):
            my_file = file
    return my_file


if __name__ == '__main__':
    start, curtime = mod.startTimer()
    start_readable = time.ctime()
    print("Start time: ", start_readable)
    count_operations = 0

    parser = argparse.ArgumentParser()
    parser.add_argument('-access', '--ACCESS', required=True, default=None) # Provide name of folder containing results
    parser.add_argument('-compareYN', '--COMPARE_FLAG', required=False, default=None) # Provide folder name of comparison results?
    parser.add_argument('-halfmYN', '--HALFM_FLAG', required=False, default=None) # Compute half-mile access results?
    parser.add_argument('-wwallYN', '--WW_ALL_FLAG', required=False, default=None) # Compute ww all blocks access results?
    parser.add_argument('-wwhalfmYN', '--WW_HALFM_FLAG', required=False, default=None) # Compute ww half-mile access results?
    parser.add_argument('-wwgroupYN', '--WW_GROUP_FLAG', required=False, default=None) # Compute ww for CTUs?
    parser.add_argument('-alltablesYN', '--ALL_TABLES_FLAG', required=False, default=None) # Produce all blocks pretty table?
    parser.add_argument('-halfmtablesYN', '--HALFM_TABLES_FLAG', required=False, default=None) # Produce halfm blocks pretty table?
    parser.add_argument('-ctutablesYN', '--CTU_TABLES_FLAG', required=False, default=None) # Produce top N CTU tables?
    args = parser.parse_args()

    # User input
    processed = None

    # accessLabel = input("Type the name of the field used to calculate accessibility, i.e. wC000, jobs, C000: ")
    print(f"Base File: {args.ACCESS}")
    count_operations += 1

    if args.COMPARE_FLAG == "Y":
        count_operations += 1
        compareToFolder = input("Type updt file folder name: ")
        compareToFile = str(compareToFolder + "-results")
        print("Compare to file: ", compareToFile)
        print("\n WARNING: Accessibility processing will take place on 1 jobs field, line 84 of assembleResults.py "
              "applies the wC000 fieldname to the processAccessResults.py program \n")
    else:
        compareToFolder = None
        compareToFile = None
        print("Message: No accessibility comparisons will be made after averaging is complete")
    if args.HALFM_FLAG == "Y":
        count_operations += 1
        halfm_blocks = input("Type full file path to half mile blocks file (default available): ") or \
                        "/Users/kristincarlson/Dropbox/AO_Projects/TIRP/Prototype2/HALFMILE_BLOCKS_GOLDRUSH.csv"
    else:
        halfm_blocks = None
        print("Message: No half mile accessibility results will be made")

    if args.WW_ALL_FLAG == "Y":
        count_operations += 1
        print("\n WARNING: Rac 2017 data will be used \n")
        # rac = "/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2015/Joined_RAC_FILE_20181003095127.csv"
        rac = "/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2017/Joined_RAC_FILE_2017.csv"

    else:
        rac = None
        print("Message: Worker weighted values will not be calculated for all blocks")

    if args.WW_HALFM_FLAG == "Y":
        count_operations += 1
        group = input("Type path to file that maps block ids to group level id (default available): ") or\
                "/Users/kristincarlson/Dropbox/DataSets/GEOID10_GNISID/GEOID10_GNISID.csv"
    else:
        group = None
        print("Message: Worker weighted values will not be calculated for grouped blocks")

    if args.WW_GROUP_FLAG == "Y":
        count_operations += 1
        pass
    else:
        print("Message: CTU level worker-weighted measure will not be preformed")

    if args.ALL_TABLES_FLAG == "Y":
        count_operations += 1
        pass
    else:
        print("Message: No pretty tables made for all metro")

    if args.HALFM_TABLES_FLAG == "Y":
        count_operations += 1
        pass
    else:
        print("Message: No pretty tables made for half mile metro")

    if args.CTU_TABLES_FLAG == "Y":
        count_operations += 1
        ctu = input("Type path to blocks to CTU id file (default available): ") or "/Users/kristincarlson/Dropbox/DataSets/GEOID10_GNISID/GEOID10_GNISID.csv"
        ctu_names = input("Type path to CTU id + CTU name strings file (default available): ") or "/Users/kristincarlson/Dropbox/DataSets/GEOID10_GNISID/CTU_NAMES.csv"
        print("\n WARNING: RAC 2015 data is in use, check if this is appropriate for your analysis \n")
        rac_ctu = input("Type path to CTU Total Rac file (default available): ") or "/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2015/CTU_Total_RAC.csv"
    else:
        ctu = None
        ctu_names = None
        rac_ctu = None
        print("Message: Top N CTU tables will not be made")

    # Make the new file name derived from the input file name
    print("\n")
    print("Access folder name: ", args.ACCESS)
    fileName = f"{args.ACCESS}-results"
    # fileName = str(sys.argv[2]).replace(".csv.bz2", "")
    print("File name: ", fileName)
    print("\n")

    # Instantiate the accessibility file object
    bar.start()
    p = ProgressBar(count_operations)
    access = AccessFile(fileName)
    print("\n")



    if args.COMPARE_FLAG == "Y":
        # Solution from https://stackoverflow.com/questions/3207219/how-do-i-list-all-files-of-a-directory
        arr = os.listdir(f'{compareToFolder}/')
        try:
            test_presence = arr[1]
            print("\nComparison file found: ", test_presence)
        except ValueError:
            test_presence = -1
            print("\nComparison file found: ", test_presence)
            print(f"\nMessage: The comparison folder {compareToFolder} has not been averaged yet")
        else:
            # Instantiate the comparison file object
            compare = AccessFile(f"{compareToFolder}-results")
            # Move into main results folder and place contents of 'ls' command in list, specifically check for averaged
            # folder
            os.chdir(args.ACCESS)
            base_ls = os.listdir('./')

            # Check for averaged results in comparison scenario folder
            os.chdir('../{}'.format(compareToFolder))
            compare_ls = os.listdir('./')

            # Move back to main folder
            os.chdir('../')
            if "Access_Results_Avg" not in base_ls and "Access_Results_Avg" not in compare_ls:
                print("Message: Base and Alternative scenario folders will be averaged")

                os.chdir(args.ACCESS)
                access.average_results()
                p.add_progress()
                print("\n")

                os.chdir('../{}'.format(compareToFolder))
                compare.average_results()
                p.add_progress()
                print("\n")
                os.chdir('../{}'.format(args.ACCESS))

                access.compare_access(compareToFolder, compareToFile)
                p.add_progress()
                print("\n")

            elif "Access_Results_Avg" not in base_ls and "Access_Results_Avg" in compare_ls:
                print("Message: Base scenario folder will be averaged, comparison folder has already been averaged.")

                os.chdir(args.ACCESS)
                access.average_results()
                p.add_progress()
                print("\n")

                access.compare_access(compareToFolder, compareToFile)
                p.add_progress()
                print("\n")

            elif "Access_Results_Avg" in base_ls and "Access_Results_Avg" not in compare_ls:
                print("Message: Alternative scenario folder will be averaged, base folder has already been averaged.")

                os.chdir('{}'.format(compareToFolder))
                compare.average_results()
                p.add_progress()
                print("\n")
                os.chdir('../{}'.format(args.ACCESS))

                access.compare_access(compareToFolder, compareToFile)
                p.add_progress()
                print("\n")
            else:
                print("Message: Both base and comparison folder have already been averaged.")
                os.chdir(args.ACCESS)
                access.compare_access(compareToFolder, compareToFile)
                p.add_progress()
                print("\n")

            # Half mile results are subset of global results
            if args.HALFM_FLAG == "Y":
                processed = find_file("2-*")
                print("Processed: ", processed)
                print("Half Mile blocks: ", halfm_blocks)
                access.halfm_results(processed, halfm_blocks)
                p.add_progress()
                print("\n")

            # Calculate global WW averages
            if args.WW_ALL_FLAG == "Y":
                processed = find_file("2-*-avg.csv")
                print("Processed: ", processed)
                print("Rac: ", rac)
                access.all_ww_results(processed, rac)
                p.add_progress()
                print("\n")

            # Calculate half mile WW averages
            if args.WW_HALFM_FLAG == "Y":
                processed_halfm = find_file("2-*-avg-halfm.csv")
                access.all_ww_results(processed_halfm, rac)
                p.add_progress()
                print("\n")

            # Calculate WW averages per CTU
            if args.WW_GROUP_FLAG == "Y":
                processed = find_file("2-*-avg.csv")
                access.group_ww_results(processed, rac, group)
                p.add_progress()
                print("\n")

            # Make global WW table
            if args.ALL_TABLES_FLAG == "Y":
                processed = find_file("3-*-avg-ww-all.csv")
                access.make_pretty_tables(processed)
                p.add_progress()
                print("\n")

            # Make half mile WW table
            if args.HALFM_TABLES_FLAG == "Y":
                processed = find_file("3-*-avg-halfm-ww-all.csv")
                access.make_pretty_tables(processed)
                p.add_progress()
                print("\n")

            # Make 3 tables pertaining to CTU groupings
            if args.CTU_TABLES_FLAG == "Y":
                processed = find_file("3-*-ww-grouped.csv")
                access.make_pretty_ctu_tables(processed, rac_ctu, ctu, ctu_names)
                p.add_progress()
                print("\n")

    else:
        # Move into main results folder
        os.chdir(args.ACCESS)

        access.average_results()
        p.add_progress()
        print("\n")


    p.end_progress()
    os.system(f"say processing done")
    end_readable = time.ctime()
    print("End time: ", end_readable)
    print("Elapsed time: ", (time.time() - start))




