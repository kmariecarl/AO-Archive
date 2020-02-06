#This script reads from a list of trips which have been removed from a GTFS feed and deletes the corresponding stop_times
#rows from the stop_times.txt file.


#NOTES: Must have a stop_times.txt file to read from and a trips file (with the heading) which contains only the trips
#you cut out from the full trips.txt file.


#################################
#           IMPORTS             #
#################################

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
def makeTripsList(file):
    with open(file) as csvfile1:
        trips_file = csv.reader(csvfile1)
        # read in trips to a string
        trips_list = []
        for row in trips_file:
            for item in row:
                trips_list.append(str(item))
        print("Number of trips in removal trip_list:", len(trips_list))
        return trips_list

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=5000000) #approx 5 million stop times records

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-stop_times', '--STOP_TIMES_FILE', required=True, default=None)  #name of stop_times file
    parser.add_argument('-trips_list', '--TRIPS_LIST_FILE', required=True, default=None)  #name of removed trips list

    args = parser.parse_args()

    fieldnames = ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence', 'pickup_type', 'drop_off_type']
    outfile = open('stop_times_reduced.txt', 'w', newline='')
    writer = csv.DictWriter(outfile, delimiter=',', fieldnames=fieldnames)
    writer.writeheader()

    tripsList = makeTripsList(args.TRIPS_LIST_FILE)


    with open(args.STOP_TIMES_FILE) as csvfile2:
        stops_file = csv.DictReader(csvfile2)
        count1 = 0
        count2 = 0
        for row in stops_file:
            trip = str(row['trip_id'])
            if trip not in tripsList:
                entry = {'trip_id': row['trip_id'], 'arrival_time': row['arrival_time'], 'departure_time': row['departure_time'],
                         'stop_id': row['stop_id'], 'stop_sequence': row['stop_sequence'], 'pickup_type': row['pickup_type'],
                         'drop_off_type': row['drop_off_type']}

                writer.writerow(entry)
                count2 += 1
            count1 += 1
            bar.next()
        bar.finish()
        print('Number of rows in original stop times file: ', count1)
        print('Number of rows in reduced stop times file: ', count2)
        print('Difference in number of rows: ', count1 - count2)