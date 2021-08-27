# This file looks at the routes.txt and trips.txt files in a GTFS folder, takes user input on which routes are to be
# removed and records which trip ids belong to which route ids. Optional argument to produce a new trips.txt file
# that has all trip IDs associated with the user specified routes removed.

# Program prompts the user to give the route id IF a file called "route_ids_to_remove.txt" is not
# provided within the directory.

#################################
#           IMPORTS             #
#################################

import csv

import os
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
    allTripsList, allShapesList, fieldnames = tripsInRoutes(args.TRIPS_FILE, usRouteList)
    mod.writeList('trips_list', allTripsList, curtime)
    mod.writeList('shapes_list', allShapesList, curtime)
    # Write out new trips file
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
    args = parser.parse_args()

    return args, curtime

# User input on route IDs


def readRoutes():
    route_list = []
    my_dir = os.getcwd()
    files_in_dir = [f for f in os.listdir(my_dir) if os.path.isfile(os.path.join(my_dir, f))]
    if 'route_ids_to_remove.txt' in files_in_dir:
        f = open("route_ids_to_remove.txt", "r")
        content = f.read()
        route_list = content.split(" ")
        f.close()
        print("Removing the following routes from this GTFS bundle: ", route_list)
    else:
        print("NOTE: No file name 'route_ids_to_remove.txt in this director, manual input needed.")
        input_string = input('Space seperated list of routes to match with trip IDs. ')
        input_list = input_string.split()

        for item in input_list:
            route_list.append(str(item))
        print('Removing the following routes from this GTFS bundle: ', route_list)

    return route_list

# Create dictionary mapping routes to a list of associated trips


def tripsInRoutes(trips_file, usr_route_list):


    with open(trips_file) as csvfile:
        trips = csv.DictReader(csvfile)
        fieldnames = trips.fieldnames

        all_trips_list = []
        all_shapes_list = []
        for row in trips:
            if str(row['route_id']) in usr_route_list:
                all_trips_list.append(str(row['trip_id']))
                all_shapes_list.append(row['shape_id'])
        print("length of all_trips_list", len(all_trips_list))
        print("length of all_shapes_list", len(all_shapes_list))

        return all_trips_list, all_shapes_list, fieldnames
    
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

            if this_trip not in all_trips_list:  # compare each trip line w/the input list, only write vals not in list
                count2 += 1
                writer.writerow(row)
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
