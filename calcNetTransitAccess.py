#This script loads in the averaged accessibility results for transit + walk and averaged walk calculations and does a row-wise
#subtraction to get the net transit accessibility. The program structure relies on the origin set and thresholds all matching!


#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import time

#################################
#           FUNCTIONS           #
#################################
def calcNet(tw, w):
    for row_tw, row_w in zip(tw, w):
        dif = int(row_tw['jobs']) -int(row_w['jobs'])
        entry = {'label': row_tw['label'], 'threshold': row_tw['threshold'], 'jobs': dif}
        writer.writerow(entry)




#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)

    parser = argparse.ArgumentParser()
    parser.add_argument('-tw', '--TRANSIT_BASE_FILE', required=True, default=None)
    parser.add_argument('-w', '--WALK_UPDATE_FILE', required=True, default=None)
    args = parser.parse_args()

    fieldnames = ['label', 'threshold', 'jobs']

    writer = mod.mkDictOutput('net_transit_{}'.format(curtime), fieldnames )


    tw = mod.readInToDict(args.TRANSIT_BASE_FILE)
    w = mod.readInToDict(args.WALK_UPDATE_FILE)
    calcNet(tw, w)