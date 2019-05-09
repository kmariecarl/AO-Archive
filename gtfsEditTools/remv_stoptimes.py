#This script reads from a list of trips which have been removed from a GTFS feed and deletes the corresponding stop_times
#rows from the stop_times.txt file.


#NOTES: Must have a stop_times.txt file to read from and a trips file (with the heading) which contains only the trips
#you cut out from the full trips.txt file.

#################################
#           IMPORTS             #
#################################

import csv


from myToolsPackage import matrixLinkModule as mod
import argparse
import time
from myToolsPackage.progress import bar
from collections import defaultdict, OrderedDict


#################################
#           FUNCTIONS           #
#################################
def makeTripsList(file):
    with open(file) as csvfile1:
        trips_file = csv.DictReader(csvfile1)
        #read in trips to a string
        trips_list = []
        for row in trips_file:
            trips_list.append(str(row['trip_id']))
        print("trip_list:", trips_list)
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
    parser.add_argument('-trips', '--TRIPS_FILE', required=True, default=None)  #name of removed trips list
    parser.add_argument('-stops', '--STOPS_FILE', required=True, default=None)  #name of stop_times file
    parser.add_argument('-fname', '--OUTPUT_FILE_NAME', required=True, default=None)  #i.e. stop_times_revised.txt

    args = parser.parse_args()

    fieldnames = ['trip_id','arrival_time','departure_time','stop_id','stop_sequence','pickup_type','drop_off_type']

    writer = mod.mkDictOutput('{}_{}'.format(args.OUTPUT_FILE_NAME, curtime), fieldname_list=fieldnames)




    tripsList = makeTripsList(args.TRIPS_FILE)


    with open(args.STOPS_FILE) as csvfile2:
        stops_file = csv.DictReader(csvfile2)
        for row in stops_file:
            trip = str(row['trip_id'])
            if trip not in tripsList:
                entry = {'trip_id': row['trip_id'], 'arrival_time': row['arrival_time'],'departure_time':row['departure_time'],
                         'stop_id':row['stop_id'],'stop_sequence':row['stop_sequence'],'pickup_type':row['pickup_type'],
                         'drop_off_type':row['drop_off_type']}

                writer.writerow(entry)
            bar.next()
        bar.finish()