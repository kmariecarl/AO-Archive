#This script imports an averaged transit accessibility file and returns the cost based accessibility equivalent
#Assumes that the 'threshold' field is in terms of seconds, i.e. 5 minute tt threshold is shown as 300

#Note that not all of the cost thresholds will be written out! This may or may not cause problems downstream

#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import time
from myToolsPackage.progress import bar
from collections import defaultdict, OrderedDict


#################################
#           FUNCTIONS           #
#################################
def assignCost(raw_cost):
    for index, thresh in enumerate(THRESHOLD_COST_LIST):
        next = index + 1
        if next < len(THRESHOLD_COST_LIST):
            if raw_cost > thresh and raw_cost <= THRESHOLD_COST_LIST[next]:
                cost = THRESHOLD_COST_LIST[next]
                return cost


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=972000) #54000 origins x 18 thresholds

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-taccess', '--TRANSIT_ACCESS_FILE', required=True, default=None)  #ENTER AS full file path to avg transit access file
    parser.add_argument('-fare', '--FARE', required=True, default=None)  #transit fare i.e. 325
    parser.add_argument('-vot', '--VOT', required=True, default=None)  #value of time i.e. 0/hr or 1803/hr is $0.00, $18.03, needs to be in terms of hours!
    parser.add_argument('-field', '--FIELD', required=True, default=None)  #output field name i.e. cost
    parser.add_argument('-fname', '--OUTPUT_FILE_NAME', required=True, default=None)  #i.e. transit16_cost_access
    args = parser.parse_args()


    #Read in variables
    vot = int(args.VOT)
    #Calculate Value of Second, cents/sec
    vos = vot/3600
    FARE = int(args.FARE)

    fieldnames = ['label','{}'.format(args.FIELD), 'jobs']
    writer = mod.mkDictOutput('{}_{}'.format(args.OUTPUT_FILE_NAME, curtime), fieldname_list=fieldnames)


    reader = mod.readInToDict(args.TRANSIT_ACCESS_FILE)

    THRESHOLD_COST_LIST = [200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000,
                           1050, 1100, 1150, 1200, 1250, 1300, 1350, 1400, 1450, 1500, 1550, 1600, 1650, 1700, 1750,
                           1800, 1850, 1900, 1950, 2000, 2050, 2100, 2150, 2200, 2250, 2300, 2350, 2400, 2450, 2500,
                           2550, 2600, 2650, 2700, 2750, 2800, 2850, 2900, 2950, 3000, 3050, 3100, 3150, 3200, 3250, 3300,
                           3350, 3400, 3450, 3500, 3550, 3600, 3650, 3700, 3750, 3800, 3850, 3900, 3950, 4000, 4050, 4100,
                           4150, 4200, 4250, 4300, 4350, 4400, 4450, 4500, 4550, 4600, 4650, 4700, 4750, 4800, 4850, 4900,
                           4950, 5000]


    for i in reader:
        current_label = i['label']
        #time cost in cents (seconds x cents/seconds = cents)
        time_cost = int(i['threshold']) * vos
        cost_raw = FARE + time_cost
        #Assign the raw cost value to a threshold as defined in the list above
        cost = assignCost(cost_raw)


        entry = {'label': i['label'], '{}'.format(args.FIELD): int(cost), 'jobs': i['jobs']}
        writer.writerow(entry)
        bar.next()
    bar.finish()
    print('Finished converting transit time-based accessibility to cost-based accessibility')