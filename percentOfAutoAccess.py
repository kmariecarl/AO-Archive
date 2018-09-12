#This script reads in two averaged accessibility files and finds the percentage of auto access that the transit
#or PNR mode provides per origin and threshold.

#ASSUMES that the files have the same dimensions, i.e. origin set and thresholds
#ASSUMES access field is called 'jobs'

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
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=54540) #3030 origins x 18 thresholds

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-bs', '--BASE_FILE', required=True, default=None)  #ENTER AS full file path to path_analyst file
    parser.add_argument('-updt', '--UPDATE_FILE', required=True, default=None)  #ENTER AS full file path to path_analyst file
    parser.add_argument('-field', '--FIELD', required=True, default=None)  #output field name i.e. pctauto
    parser.add_argument('-mode', '--COMPARE_MODE', required=True, default=None)  #PNR or Transit
    args = parser.parse_args()

    fieldnames = ['label', 'threshold','{}'.format(args.FIELD)]
    writer = mod.mkDictOutput('Percent_Auto_{}_{}'.format(args.COMPARE_MODE, curtime), fieldname_list=fieldnames)


    reader_auto = mod.readInToDict(args.BASE_FILE)
    reader_mode = mod.readInToDict(args.UPDATE_FILE)

    for i,j in zip(reader_auto, reader_mode):
        if int(i['jobs']) != 0:
            pctAuto = (int(j['jobs']) / int(i['jobs'])) * 100
            entry = {'label': i['label'], 'threshold': i['threshold'], '{}'.format(args.FIELD): round(pctAuto, 4)}
            writer.writerow(entry)
            bar.next()
        else:
            entry = {'label': i['label'], 'threshold': i['threshold'], '{}'.format(args.FIELD): 0}
            writer.writerow(entry)
            bar.next()

    bar.finish()
    print('Percent auto access file generated from {} mode'.format(args.COMPARE_MODE))

