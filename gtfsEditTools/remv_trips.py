# This file looks at the routes.txt and trips.txt files in a GTFS folder, takes user input on which routes are to be
# removed and records which trip ids belong to which route ids. Optional argument to produce a new trips.txt file
# that has all trip IDs associated with the user specified routes removed.

#Program prompts the user to give the route id

# ToDo: remove trailing comma at the end of shapes_list output file

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
    usRouteList = readRoutes()
    allTripsList, allShapesList = tripsInRoutes(args.TRIPS_FILE, usRouteList)
    mod.writeList('trips_list', allTripsList, curtime)
    mod.writeList('shapes_list', allShapesList, curtime)
    #if args.REMOVE_TRIPS == 'yes':
    # Write out new trips file
    fieldnames = ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'direction_id', 'block_id', 'shape_id',
                   'wheelchair_accessible']
    outfile = open('trips_reduced.txt', 'w', newline='')
    writer = csv.DictWriter(outfile, delimiter=',', fieldnames=fieldnames)
    writer.writeheader()
    removeTrips(args.TRIPS_FILE, allTripsList, writer)


# Parameters


def initiate():

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)

    parser = argparse.ArgumentParser()
    parser.add_argument('-trips', '--TRIPS_FILE', required=True, default='trips.txt')  # name of GTFS trips.txt to find trip IDs from
    #parser.add_argument('-remove', '--REMOVE_TRIPS', required=False, default=None)  # list of routes to remove
    args = parser.parse_args()

    return args, curtime

# User input on route IDs


def readRoutes():

    input_string = input('Space seperated list of routes to match with trip IDs. ')
    input_list = input_string.split()
    usr_list = []
    for item in input_list:
        usr_list.append(str(item))
    print('List of routes to remove: ')
    print(input_string)

    return usr_list

# Create dictionary mapping routes to a list of associated trips


def tripsInRoutes(trips_file, usr_route_list):


    with open(trips_file) as csvfile:
        trips = csv.DictReader(csvfile)

        all_trips_list = []
        all_shapes_list = []
        for row in trips:
            if str(row['route_id']) in usr_route_list:
                all_trips_list.append(str(row['trip_id']))
                all_shapes_list.append(row['shape_id'])
        print("length of all_trips_list", len(all_trips_list))
        print("length of all_shapes_list", len(all_shapes_list))

        return all_trips_list, all_shapes_list
    
# Optional function to reproduce trips.txt file without the trips found to be 
# associated with the routes provided by the user.


def removeTrips(trips_file, all_trips_list, writer):
    mybar = bar.Bar(message='Processing', fill='@', suffix='%(percent)d%%', max=33000)  # Adjust this number

    with open(trips_file) as csvfile:
        trips = csv.DictReader(csvfile)
        count1 = 0
        count2 = 0
        for row in trips:
            count1 += 1
            this_trip = str(row['trip_id'])

            if this_trip not in all_trips_list:
                count2 += 1
                entry = {'route_id': row['route_id'], 'service_id': row['service_id'],
                         'trip_id': row['trip_id'], 'trip_headsign': row['trip_headsign'],
                         'direction_id': row['direction_id'], 'block_id': row['block_id'],
                         'shape_id': row['shape_id'], 'wheelchair_accessible': row['wheelchair_accessible']}

                writer.writerow(entry)
            mybar.next()
        mybar.finish()
        print('Number of rows in original trips file: ', count1)
        print('Number of rows in reduced trips file: ', count2)
        print('Difference in number of rows: ', count1 - count2)



#################################
#           MAIN          #
#################################

if __name__ == '__main__':

    main()
