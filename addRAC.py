#This script adds the RACC000 fields of different states to produce one RACC000 field. Can also be used for WACC000

#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import time
from myToolsPackage.progress import bar

#################################
#           FUNCTIONS           #
#################################

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=2)

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-file1', '--RAC_FILE_1', required=True, default=None)  #ENTER AS full file path to path_analyst file
    parser.add_argument('-file2', '--RAC_FILE_2', required=True, default=None)  #ENTER AS full file path to path_analyst file
    parser.add_argument('-field', '--FIELD', required=True, default=None)  #output field name i.e. racC000 or wacC000
    args = parser.parse_args()

    fieldnames = ['GEOID10','{}'.format(args.FIELD)]
    writer = mod.mkDictOutput('Joined_RAC_FILE_{}'.format(curtime), fieldname_list=fieldnames)


    reader1 = mod.readInToDict(args.RAC_FILE_1)
    reader2 = mod.readInToDict(args.RAC_FILE_2)


    for row in reader1:

        entry = {'GEOID10': row['h_geocode'], '{}'.format(args.FIELD): row['C000']}

        writer.writerow(entry)
    bar.next()

    for row2 in reader2:
        entry = {'GEOID10': row2['h_geocode'], '{}'.format(args.FIELD): row2['C000']}

        writer.writerow(entry)
    bar.next()
    bar.finish()
    print('Files joined for single {} field'.format(args.FIELD))

