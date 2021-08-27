# This program follows the minute-by-minute threshold and departure time results from BatchAnalyst-0.2.3-xxx.jar
# The cumulative accessibility results are read in and the 50th percentile travel time to the 1st, 2nd, 3rd, etc.
# destination is found and written out to file.

# Note: averaging does NOT take place prior or post using this program

# This program reads in a single file with the following structure:
# label,deptime,threshold,grocery,health,es,ms,hs
# 271230306021006,0700,0,0,0,0,0,0
# 271230306021006,0700,60,0,0,0,0,0
# 271230306021006,0700,120,0,0,0,0,0
# 271230306021006,0700,180,0,0,0,0,0
# 271230306021006,0700,240,0,0,0,0,0

# And converts the output to multiple files (one per field) with a destinations cap (i.e. 10, 25, 100, etc) and the 50th
# percentile is reported for the TT
# origin,1,2,3,4,5,6,7,8,9,10
# 270370601013004,1116,1632,1686,1686,1894,2049,2507,2601,2710,2754
# 270370601014001,1610,1610,1632,1761,1963,2109,2507,2601,2694,2730


# Assumptions: This program utilizes the input data structure of increasing travel threshold (0--5400 seconds) and
# assumes that results for each origin are grouped together (in order of threshold).
# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

import argparse
from datetime import datetime
from progress import bar
from myToolsPackage import matrixLinkModule as mod
import pandas as pd
import numpy as np
import csv
import statistics
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
    def __init__(self, access_file, limit, chunks):
        df = pd.read_csv(access_file, chunksize=chunks)
        self.df = df
        print("Dataframe types: ", self.df.dtypes)
        deptime = self.df["deptime"]
        self.deptimemin = deptime.min()
        self.deptimemax = deptime.max()
        print("deptime min and max: ", self.deptimemin, self.deptimemax)
        self.dest_list = list(self.df)[3:]  # Skip 'label', 'deptime', 'threshold'
        print("Destination types: ", self.dest_list)
        self.limit = int(limit)  # integer
        self.destination_limit_list = None
        # self.df_out = None
    #
    # def make_origin_deptime_lists(self):
    #     print("Making unique origin and deptime lists")
    #     origin_list = []
    #     deptime_list = []
    #     for row in self.dict_obj:
    #         if int(row["label"]) not in origin_list:
    #             print("here1")
    #             origin_list.append(int(row["label"]))
    #         if int(row["deptime"]) not in deptime_list:
    #             print("here2")
    #             deptime_list.append(int(row["deptime"]))
    #     self.origin_list = origin_list
    #     self.deptime_list = deptime_list
    #     print("orign list: ", origin_list)
    #     print("deptime list: ", deptime_list)




    # Instance methods
    def calc_dual_access(self):
        columns_out = ['origin'].extend(self.destination_limit_list) # Create df_out, list of lists
        # helpful reasoning for calculating median per row, versus per column https://stackoverflow.com/a/56746204
        # each output row should correspond to one origin with the median for each number of destinations
        tile_dict = {}  # each key is a destination limit and each value is the list of tt, then calculated median
        for num in self.destination_limit_list:  # For each origin, calculate all median TT value for the output
            tile_dict[num] = []
        bar1 = ProgressBar(len(self.dest_list) - 1)
        print("Computing the 50th percentile")
        for dest in self.dest_list: #Write out new file for each destination type
            df_grouped1 = self.df.groupby(['label'], as_index=False)
            data_out = []  # Create a list of lists to write out to csv for each destination type
            bar2 = ProgressBar(self.df['label'].nunique())
            for group_name1, df_group1 in df_grouped1:
                df_grouped2 = df_group1.groupby(['deptime'], as_index=False)
                out_list = [f"{group_name1}"]
                prev = 0
                for group_name2, df_group2 in df_grouped2:  # Select one threshold for each deptime, append to list
                    for row_index, row in df_group2.iterrows():
                        if row[f'{dest}'] - prev > 0 and row[f'{dest}'] <= self.destination_limit_list[-1]:
                            tile_dict[row[f'{dest}']].append(row['threshold'])
                        prev = row[f'{dest}']
                        # tt_list = []
                        # tt_list.append(df_group2.loc[(df_group2[f'{dest}'] > num - 1).idxmax(), 'threshold'])
                    # tile_dict[num] = tt_list

                    # for index, values in df_group2.iterrows():
                    #     if values[f'{dest}'] == num:
                    #         tt_list.append(values['threshold'])
                    #         break
                    # print(np.percentile(tt_list, 50))
                for key, val in tile_dict.items():
                    out_list.append(round(np.percentile(val, 50)))  # Append the median tt for each destination number
                data_out.append(out_list)
                bar2.add_progress()
            bar1.add_progress()
            df_out = pd.DataFrame(data_out, columns=columns_out)
            df_out.to_csv(f"median_tt_{dest}_{self.deptimemin}_{self.deptimemax}.csv", index=False)
            bar2.end_progress()
        bar1.end_progress()
    #
    # def write_to_csv(self):
    #     # df.to_csv(r'Path where you want to store the exported CSV file\File Name.csv', index=False)


def make_destination_limit_list(limit):
    print("Making destination limit list")
    limit_list = [x for x in range(1, limit + 1, 1)]
    print("Destination limit list: ", limit_list)
    return limit_list

def write_to_csv(data, columns, deptimemin, deptimemax):
    print("Writing out median TT files")
    for dest, origins in data.items():
        file_out = open(f"median_tt_{dest}_{deptimemin}_{deptimemax}_test.csv", 'w')
        with file_out:
            writer = csv.writer(file_out)
            writer.writerow(columns)
            writer.writerows(origins)



#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    print(datetime.now())

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-access', '--ACCESS_FILE_NAME', required=True, default=None)
    parser.add_argument('-dep', '--NUMBER_OF_DEPTIMES', required=True, default=None)  # I.E. 120
    parser.add_argument('-thresh', '--NUMBER_OF_THRESHOLDS', required=True, default=None)  # i.e. 60
    parser.add_argument('-dest_limit', '--DESTINATION_LIMIT', required=True, default=None)  # i.e. 10, 20, etc.
    args = parser.parse_args()

    # Create variables from inputs
    chunksize = int(args.NUMBER_OF_DEPTIMES) * int(args.NUMBER_OF_THRESHOLDS)
    limit_list = make_destination_limit_list(int(args.DESTINATION_LIMIT))
    dest_list = pd.read_csv(args.ACCESS_FILE_NAME, nrows=1).columns[3:]  # Skip 'label', 'deptime', 'threshold'
    n = pd.read_csv(args.ACCESS_FILE_NAME)['label'].nunique()
    print("Unique origins: ", n)


    output = {}  # Instantiate output dictionary:[[]]
    for i in dest_list:
        output[i] = []

    columns_out = ['origin'] #,'1','2','3','4','5','6','7','8','9','10']  # Create df_out, list of lists
    for i in limit_list:
        columns_out.append(str(i))
    print("columns to write out: ", columns_out)

    # Read in data in chunks
    data = pd.read_csv(args.ACCESS_FILE_NAME, header='infer', chunksize=chunksize)


    bar = ProgressBar(n)
    print("Algorithm starting now: ", print(datetime.now()) )
    for chunk in data:
        # Use the deptime min and max for output file naming
        deptime = chunk["deptime"]
        deptimemin = deptime.min()
        deptimemax = deptime.max()
        # curr_origin = chunk["label"]
        curr_origin = chunk[0]
        print(curr_origin)
        print(type(curr_origin))
        # print("New chunk: ", print(datetime.now()) )
        df_grouped = chunk.groupby(['deptime'], as_index=False)
        # curr_origin = None
        for dest in dest_list:  # Write out new file for each destination type, simultaneously due to chunking
            # print("New destination type: ", print(datetime.now()) )
           # list_of_lists = []
            origin_results = []
            for num in limit_list:
                # print("New number of destinations: ", print(datetime.now()) )
                tt_list = []
                for group_name1, df_group1 in df_grouped:
                    # curr_origin = df_group1.iloc[0,0]
                    tt_list.append(df_group1.loc[(df_group1[f'{dest}'] > num - 1).idxmax(), 'threshold'])
                # print("take percentile: ", print(datetime.now()) )
                # origin_results.append(int(np.percentile(tt_list, 50)))  # Append the median tt for each destination number
                origin_results.append(statistics.median(tt_list))
                # print("post taking percentile: ", print(datetime.now()) )
            origin_results.insert(0, curr_origin)
            output[dest].append(origin_results)
        bar.add_progress()
    bar.end_progress()
    print("program ended, now writing")
    write_to_csv(output, columns_out, deptimemin, deptimemax)
    print(datetime.now())