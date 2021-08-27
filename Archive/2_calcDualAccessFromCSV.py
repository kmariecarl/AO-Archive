# This program is designed to follow the averageTransitLocalAll.py program. This workflow uses the
# BatchAnalyst_0.2.3-smx-all.jar program to calculate min-by-min cumulative accessibility for many destinations.

# This program reads in a single file with the following structure:
# label,threshold,field1,field2,...,field100
# origin1,60,0,0,0
# origin1,120,1,0,1
# origin1,180,1,1,1
# origin2,60,0,0,0
# origin2,120,0,0,1
# origin2,180,1,1,2

# And converts the output to multiple files (one per field) with a destinations cap (i.e. 10, 25, 100, etc)
# origin,1,2,3,4,5,6,7,8,9,10
# 270370601013004,1116,1632,1686,1686,1894,2049,2507,2601,2710,2754
# 270370601014001,1610,1610,1632,1761,1963,2109,2507,2601,2694,2730

# Essentially, this program reorganizes the cumulative average access to dual average access

# Assumptions: This program utilizes the input data structure of increasing travel threshold (0--5400 seconds) and
# assumes that results for each origin are grouped together (in order of threshold).
# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

import argparse
from datetime import datetime
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
    def __init__(self, access_file, dest_limit):
        dict_obj = list(mod.readInToDict(access_file))  # This is a genius implementation of reading a csv into a dict
        self.dict_obj = dict_obj
        self.fieldnames = list(self.dict_obj[0].keys())  # top of csv input file
        self.dest_limit = int(dest_limit)  # integer

    # Instance methods

    def calc_dual_access(self):
        dest_limit_list = [x for x in range(1, self.dest_limit + 1, 1)]
        bar = ProgressBar(len(dest_limit_list))
        dest_limit_list.insert(0, "origin")

        for field in self.fieldnames[2:]:
            # print(f"Creating dual access output for {field}")
            writer = self._make_writer(f"{field}", fieldnames=dest_limit_list)
            out_dict = {}  # make new output
            count1 = 0  # indicator for the first row
            origin = None  # create placeholder variable
            dest_num_prev = 0  # tracks how many destinations have been found
            threshold_prev = 0  # set this variable
            for row in self.dict_obj:
                if count1 == 0:  # set the initial origin
                    origin = row["label"]
                    out_dict[origin] = {}  # add first origin to output
                # If true, we are at the same origin
                if origin == row["label"]:
                    out_dict, dest_num_prev, threshold_prev = self._build_dict(out_dict, field, row, dest_num_prev, threshold_prev)

                # False, we changed origins in this step
                else:
                    origin = row["label"]
                    out_dict[origin] = {}
                    dest_num_prev = 0  # reset counter for new origin
                    threshold_prev = 0  # reset counter for new origin
                    out_dict, dest_num_prev, threshold_prev = self._build_dict(out_dict, field, row, dest_num_prev, threshold_prev)
                count1 += 1
            # Solution from https://stackoverflow.com/questions/29400631/python-writing-nested-dictionary-to-csv
            for key, val in sorted(out_dict.items()):
                row = {'origin': key}
                row.update(val)
                writer.writerow(row)
            bar.add_progress()
        bar.end_progress()

    # contains the inner processing of this program, this code runs for each row
    def _build_dict(self, out_dict, field, row, dest_num_prev, threshold_prev):
        # if the number of destinations increases by 1 from the previous:
        if int(row[f"{field}"]) == dest_num_prev + 1 and dest_num_prev < self.dest_limit:
            out_dict[row["label"]][int(row[f"{field}"])] = row["threshold"]
            dest_num_prev += 1
            threshold_prev = row["threshold"]
        # if the number of destinations increased by more than 1 from the previous:
        elif int(row[f"{field}"]) > dest_num_prev + 1 and dest_num_prev < self.dest_limit:
            # The while loop carries the previous threshold to the destinations that were skipped i.e
            # from one minute to the next, the num. of dest. that can be reached goes from 6 to 8, the while
            # loop writes in the spot for 7.
            while int(row[f"{field}"]) - 1 > dest_num_prev and dest_num_prev < self.dest_limit:
                out_dict[row["label"]][dest_num_prev + 1] = threshold_prev
                dest_num_prev += 1
            # Write out the threshold for 8, following the example above
            if dest_num_prev < self.dest_limit:
                out_dict[row["label"]][int(row[f"{field}"])] = row["threshold"]
                dest_num_prev += 1
        threshold_prev = row["threshold"]
        return out_dict, dest_num_prev, threshold_prev

    @staticmethod
    def _make_writer(field, fieldnames):
        writer = mod.mkDictOutput(f"dual_{field}_test",fieldname_list=fieldnames)
        return writer

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    print(datetime.now())

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-access', '--ACCESS_FILE_NAME', required=True, default=None)
    parser.add_argument('-dest_limit', '--DESTINATION_LIMIT', required=True, default=None)
    args = parser.parse_args()

    dict_obj = AccessFile(args.ACCESS_FILE_NAME, args.DESTINATION_LIMIT)
    dict_obj.calc_dual_access()

    print(datetime.now())