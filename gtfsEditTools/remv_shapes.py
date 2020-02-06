# This file reads in a list of trip IDs that have been removed from the trips.txt file and removes all shapes associated
# with the routes/trips that the user wants to remove from GTFS feed.


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
    allShapesList = readRemoveShapes(args.REMOVE_SHAPES_FILE)
    # Write out new shapes file
    fieldnames = ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence']
    outfile = open('shapes_reduced.txt', 'w', newline='')
    writer = csv.DictWriter(outfile, delimiter=',', fieldnames=fieldnames)
    writer.writeheader()
    removeShapes(args.SHAPES_FILE, allShapesList, writer)


# Parameters


def initiate():

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)

    parser = argparse.ArgumentParser()
    parser.add_argument('-shapes', '--SHAPES_FILE', required=True, default='shapes.txt')  # name of GTFS shapes.txt file original
    parser.add_argument('-remv_shapes', '--REMOVE_SHAPES_FILE', required=True, default=None)  # name of file that lists shape_ids to remove
    args = parser.parse_args()

    return args, curtime


def readRemoveShapes(remv_shapes_file):


    with open(remv_shapes_file) as csvfile:
        remv = csv.reader(csvfile)
        all_shapes_list = []
        # Not sure why I have to use two for loops to iterate through the csv but oh well
        for row in remv:
            for shape_id in row:
                if int(shape_id) not in all_shapes_list:
                    print(shape_id)
                    all_shapes_list.append(int(shape_id))
        print(all_shapes_list)
        print("Number of shapes to remove: ", len(all_shapes_list))
        return all_shapes_list


# Optional function to reproduce trips.txt file without the trips found to be
# associated with the routes provided by the user.


def removeShapes(shapes_file, all_shapes_list, writer):
    mybar = bar.Bar(message='Processing', fill='@', suffix='%(percent)d%%', max=450000)  # Adjust this number

    with open(shapes_file) as csvfile:
        shapesfile = csv.DictReader(csvfile)
        count1 = 0
        count2 = 0
        for row in shapesfile:
            this_shape_id = int(row['shape_id'])
            if this_shape_id not in all_shapes_list:
                entry = {'shape_id': row['shape_id'], 'shape_pt_lat': row['shape_pt_lat'],
                         'shape_pt_lon': row['shape_pt_lon'], 'shape_pt_sequence': row['shape_pt_sequence']}
                mybar.next()
                writer.writerow(entry)
                count2 += 1
            count1 += 1
        mybar.finish()
        print('Number of rows in original shapes file: ', count1)
        print('Number of rows in reduced shapes file: ', count2)
        print('Difference in number of rows: ', count1 - count2)



#################################
#           MAIN          #
#################################

if __name__ == '__main__':

    main()