#This script imports an averaged transit accessibility file and returns the cost based accessibility equivalent
#Assumes that the 'threshold' field is in terms of seconds, i.e. 5 minute tt threshold is shown as 300

#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import time
from myToolsPackage.progress import bar


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
    parser.add_argument('-vot', '--VOT', required=False, default=None)  #value of time i.e. 0/hr or 1803/hr is $0.00, $18.03, needs to be in terms of hours!
    parser.add_argument('-field', '--FIELD', required=True, default=None)  #output field name i.e. cost
    parser.add_argument('-fname', '--OUTPUT_FILE_NAME', required=True, default=None)  #i.e. transit16_cost_access
    args = parser.parse_args()


    #VOT is not required, so if provided, assign to variable
    if args.VOT:
        vot = int(args.VOT)
    #Calculate Value of Second, cents/sec
    vos = vot/3600
    FARE = int(args.FARE)

    fieldnames = ['label','{}'.format(args.FIELD), 'jobs']
    writer = mod.mkDictOutput('{}_{}'.format(args.OUTPUT_FILE_NAME, curtime), fieldname_list=fieldnames)


    reader = mod.readInToDict(args.TRANSIT_ACCESS_FILE)

    for i in reader:
        #time cost in cents (seconds x cents/seconds = cents)
        time_cost = int(i['threshold']) * vos
        cost = FARE + time_cost
        entry = {'label': i['label'], '{}'.format(args.FIELD): int(cost), 'jobs': i['jobs']}
        writer.writerow(entry)
        bar.next()
    bar.finish()
    print('Finished converting transit time-based accessibility to cost-based accessibility')