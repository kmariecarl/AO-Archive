#This script will assist with processing AVL data for mapping polylines in QGIS, others?

#################################
#           TO DO               #
#################################
#Read in csv files

#Put into dictionary format

#Calc speeds?


#################################
#           IMPORTS             #
#################################
import csv
import datetime
import os
import time
import argparse
import createString
import stopTimesEditor8

#################################
#           FUNCTIONS           #
#################################

def makeDict(file):
    with open(os.path.join(file)) as reader:
        dictionary = csv.DictReader(reader)
        return dictionary
        #out_dict = {}
        #for row in dictionary:

            # if row['Route'] not in out_dict:
            #     out_dict[row['Route']] = {}
            #     #print(out_dict)
            #     if row['Bus'] not in out_dict[row['Route']]:
            #      #   print(row['Route'], row['Bus'])
            #         #out_dict[row['Route']][row['Bus']] = 24
            # #print(out_dict)
            #         out_dict[row['Route']][row['Bus']] = row['Date'], row['Srv'], row['Trip'], row['Dir'], row['Pat'], row['Node_id'], row['Sched'], row['Arr'], row['Dep']
            # #print(out_dict)
def makeRouteList(AVL_dict):
    route_list = []
    for row in AVL_dict:
        if row['Rout'] not in route_list:
            route_list.append(str(row['Route']))
        print(route_list)
    return route_list

def findRouteSpeed(route, AVL_dict):
    #Create forward/backward nodes.
    currRow = None
    prevRow = 0
    for row in AVL_dict:
        currRow = row
        if stopTimesEditor8.isTimeInteresting(currRow, args.START_TIME, args.STOP_TIME) is True:
            if stopTimesEditor8.findInterestingStopPair(currRow, prevRow,)



if __name__=="__main__":
    # Start timing
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-avl', '--AVL_DATA', required=True, default=None)
    parser.add_argument('-i', '--INPUTFILE', required=True, default=None)
    parser.add_argument('-start', '--START_TIME', required=True, default=None)
    parser.add_argument('-stop', '--STOP_TIME', required=True, default=None)
    parser.add_argument('-stoptimes', '--STOP_TIMES', required=False, default=None)
    parser.add_argument('-avlmatch', '--AVL_MATCH', required=False, default=None)
    args = parser.parse_args()


    inputList = stopTimesEditor8.makeInputList()
    avl_dict = createString.makeDict(args.AVL_DATA)
    routes = makeRouteList(avl_dict)
    #stoptimes_dict = makeDict(args.STOP_TIMES)
    # for row_st in stoptimes_dict:
    #     #
    #     stopTimesEditor8.isTimeInteresting()
    #     stopTimesEditor8.isTripInteresting()
    speedList = []
    for item in routes:
        speedList.append(findRouteSpeed(item, avl_dict))



    for item in avl_dict:
        test = item['Route']['Bus']
        print(test)
