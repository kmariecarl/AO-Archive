# This file removes wheelchair_accessible from trips.txt file

#################################
#           IMPORTS             #
#################################

import csv


from myToolsPackage import matrixLinkModule as mod
import argparse
import time
from myToolsPackage.progress import bar


#################################
#           FUNCTIONS           #
#################################
def main():

    args, curtime = initiate()

    # User input
    fieldnames = ['route_id','service_id','trip_id','trip_headsign','direction_id','block_id','shape_id','wheelchair_accessible']
    writer = mod.mkDictOutput('trips_nowheels_{}'.format(curtime), fieldname_list=fieldnames)
    remvWheels(args.TRIPS_FILE, writer)


# Parameters


def initiate():

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)

    parser = argparse.ArgumentParser()
    parser.add_argument('-trips', '--TRIPS_FILE', required=True, default='trips.txt')  # name of GTFS trips.txt to find trip IDs from
    args = parser.parse_args()

    return args, curtime

# User input on route IDs


def remvWheels(trips_file, writer):


    with open(trips_file) as csvfile:
        trips = csv.DictReader(csvfile)

        for row in trips:
            if row['wheelchair_accessible'] == "['wheelchair_accessible']":
                row['wheelchair_accessible'] = 1
                entry = {'route_id': row['route_id'], 'service_id': row['service_id'], 'trip_id': row['trip_id'], 
                'trip_headsign': row['trip_headsign'], 'direction_id': row['direction_id'], 'block_id': row['block_id'],
                'shape_id': row['shape_id'], 'wheelchair_accessible': row['wheelchair_accessible']}
                writer.writerow(entry)
            else:
                entry = {'route_id': row['route_id'], 'service_id': row['service_id'], 'trip_id': row['trip_id'], 
                'trip_headsign': row['trip_headsign'], 'direction_id': row['direction_id'], 'block_id': row['block_id'],
                'shape_id': row['shape_id'], 'wheelchair_accessible': row['wheelchair_accessible']}
                writer.writerow(entry)




#################################
#           MAIN          #
#################################

if __name__ == '__main__':

    main()
