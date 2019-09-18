#This file relies on the user having already run the following bash command:
# bashCommand = sort -u stops1.txt stops2.txt > stops_combined.txt

#################################
#           IMPORTS             #
#################################


from myToolsPackage import matrixLinkModule as mod
import argparse
import time
import subprocess
import csv
from myToolsPackage.progress import bar


#################################
#           FUNCTIONS           #
#################################

def main():
    args, curtime = initiate()

    fieldnames = ['stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon', 'zone_id',
                   'stop_url', 'location_type', 'wheelchair_boarding']
    writer = mod.mkOutput('stops_reduced_{}'.format(curtime), fieldname_list=fieldnames)
    writer.writerow(fieldnames)
    uniqueStops = removeStops(args.STOPS_COMBINED, writer)
    print(uniqueStops)



# Parameters


def initiate():

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)

    parser = argparse.ArgumentParser()
    parser.add_argument('-stops_combined', '--STOPS_COMBINED', required=True, default=None)  # name of stops_combined file
    args = parser.parse_args()

    return args, curtime

def removeStops(stops_file, writer):
    mybar = bar.Bar(message='Processing', fill='@', suffix='%(percent)d%%', max=33000)  # Adjust this number
    with open(stops_file) as csvfile:
        next(csvfile)
        stopsfile = csv.reader(csvfile)
        unique_stops = []
        count1 = 0
        count2 = 0
        for row in stopsfile:
            count1 += 1
            if int(row[0]) not in unique_stops:
                count2 += 1
                unique_stops.append(int(row[0]))
                writer.writerow(row)
            mybar.next()
        mybar.finish()
        print('Number of rows in original stops file: ', count1)
        print('Number of rows in reduced stops file: ', count2)
        print('Difference in number of rows: ', count1 - count2)

    return unique_stops

#################################
#           MAIN          #
#################################

if __name__ == '__main__':

    main()