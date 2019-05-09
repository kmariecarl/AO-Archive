# File: stopTimesTester.py
# Date Created: 2017-05-09
# Date Modified:
# Created By: Kristin M. Carlson
#
# Description:
# This program is designed to check if
# changes were made to the stop_times.txt file
# based on a specified scenario.

# To Do:
# 1. Compute the difference in time between original and changed stop_times.txt files.
# 2. Do the stop_pairs with changed travel time make sense for the scenario?
# 3. Check by comparing trips with changes to routes.
# 4. Need to add segment_id output

####################################
## Libraries and global variables ##
####################################

import csv
import os
# My functions imported as modules
from stopTimesEditor2 import get_sec
from stopTimesEditor2 import makeTripsDict

####################################
##         Working Code           ##
####################################

# Read in original stop_times.txt file as dictionary
with open(os.path.join('..', 'Data', 'gtfs_09_06_16','stop_times.txt')) as stop_times_file:
    original = csv.DictReader(stop_times_file)
    # Read in changed stop_times.txt file. Convert to dict
    with open(os.path.join('852_gtfs_090616_20170516120039.txt')) as stop_times_file2:
        changed = csv.DictReader(stop_times_file2)
        # Make a trips dict that maps trip_ids to routes. Used to print changed routes later
        trips_dict = makeTripsDict()
        # Loop through the original and changed stop_times.txt files and compute difference in arrival times.
        for row0, row1 in zip(original, changed):
            orig = get_sec(row0['arrival_time'])
            chng = get_sec(row1['arrival_time'])
            diff =  round((orig - chng), 2)
            if diff != 0:
                # Extract the route_id that matches the trip_id from the current row
                route_id = trips_dict[row1['trip_id']]
                print(diff/60, row1['trip_id'], route_id)



