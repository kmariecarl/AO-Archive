# This program calculates the percent and absolute difference between dual accessibility scenarios.

# Example use: Dual access to 1–10 grocery destinations for scenario 1 compared to dual access to 1–10 grocery
# destinations for scenario 2 where the difference shows the change in minimum travel time needed to reach 1, 2, 3...
# destinations due to the transportation or land use change implemented in scenario 2.

# Data comes in the form:
# BlockID,1,2,3,4,5,6,7,8,9,10
# 270530267143004,0,1,1,0,0,0,0,1,0,0,0,0

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

from datetime import datetime
import argparse
import csv
import itertools
from progress import bar



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
# --------------------------
#       GENERAL FUNCTIONS
# --------------------------

def make_nested_dict(file):
    out_dict = {}
    lines = 0
    with open(file) as f:
        reader = csv.DictReader(f,  delimiter = ",")
        for row in reader:
            for key, val in row.items():
                if key == 'origin':
                    out_dict[val] = {}
                    origin = val
                else:
                    out_dict[origin][key] = int(val)
            lines += 1
    return out_dict, lines

def make_fieldnames(output):
    fieldnames = ['origin']
    for k, v in output.items():
        for i, j in v.items():
            if i not in fieldnames:
                fieldnames.append(i)
        break
    print(f"Fieldnames: {fieldnames}")
    return fieldnames

if __name__ == '__main__':
    print(datetime.now())

    parser = argparse.ArgumentParser()
    parser.add_argument('-bs', '--BASE_FILE', required=True, default=None)  # Baseline scenario dual access file
    parser.add_argument('-bs_short', '--SHORT_BASE_NAME', required=True, default=None)
    parser.add_argument('-updt', '--UPDATE_FILE', required=True,
                        default=None)  # Modified scneario dual access results file
    parser.add_argument('-updt_short', '--SHORT_UPDT_NAME', required=True, default=None)
    parser.add_argument('-other_name', '--OTHER_NAME_TAG', required=False, default=None)
    args = parser.parse_args()

    print("Processing results \n")
    # Nested dictionary from csv file solution found at:
    # https://medium.com/swlh/how-to-parse-a-csv-to-create-a-nested-dictionary-python-61d5a6934eb9
    base, lines_bs = make_nested_dict(args.BASE_FILE)
    updt, lines_updt = make_nested_dict(args.UPDATE_FILE)

    output = {}
    mybar = ProgressBar(lines_bs)
    for origin, values in base.items():
        bs_vals = values
        updt_vals = updt[origin]
        entry = {}
        for k, v in bs_vals.items():
            entry[f"bs_{k}"] = v  # Add the original baseline value to the output
            entry[f"updt_{k}"] = updt_vals[k]  # Add the alternative scenario values to the output
            if bs_vals[k] == 2147483647 and updt_vals[k] < 2147483647:
                entry[f"abschg{k}"] = -6000  # Fill in where tt went from maxtime to reachable
                entry[f"pctchg{k}"] = -1  # Fill in where tt went from maxtime to reachable
            elif updt_vals[k] == 2147483647 and bs_vals[k] < 2147483647:
                entry[f"abschg{k}"] = 6000
                entry[f"pctchg{k}"] = 1
            else:
                entry[f"abschg{k}"] = updt_vals[k] - bs_vals[k]
                entry[f"pctchg{k}"] = (updt_vals[k] - bs_vals[k]) / bs_vals[k]   # Take the percent difference
        output[origin] = entry
        mybar.add_progress()
    mybar.end_progress()

    fieldnames = make_fieldnames(output)


    if args.OTHER_NAME_TAG:
        with open(f"2_change_{args.SHORT_BASE_NAME}_{args.SHORT_UPDT_NAME}_{args.OTHER_NAME_TAG}.csv", 'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter = ',')
            writer.writeheader()
            for k,v in output.items():
                entry = {}
                entry['origin'] = k
                entry.update(v)
                writer.writerow(entry)

    else:
        with open(f"2_change_{args.SHORT_BASE_NAME}_{args.SHORT_UPDT_NAME}.csv", 'w') as f: # Output file name
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=',')
            writer.writeheader()
            for k, v in output.items():
                entry = {}
                entry['origin'] = k
                entry.update(v)
                writer.writerow(entry)


