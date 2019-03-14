#This script essentially concatenates the LEHD files from different states on RAC_C000 or WAC_C000 data
#and outputs the GEOID10 with a wac or rac field for each state.

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
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=2)

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-file1', '--RAC_FILE_1', required=True, default=None)  #ENTER AS full file path to LEHD file
    parser.add_argument('-file2', '--RAC_FILE_2', required=True, default=None)  #ENTER AS full file path to LEHD file
    parser.add_argument('-wac_rac', '--WAC_RAC', required=True, default=None)  #Specify wac or rac being calculated
    parser.add_argument('-label', '--LABEL', required=True, default=None)  #WAC is w_geocode and RAC is h_geocode
    parser.add_argument('-field', '--FIELD', required=True, default=None)  #output field name i.e. racC000 or wacC000
    args = parser.parse_args()


    reader1 = mod.readInToDict(args.RAC_FILE_1)
    reader2 = mod.readInToDict(args.RAC_FILE_2)
    label = args.LABEL
    wac_rac = args.WAC_RAC

    fieldnames = ['GEOID10','{}'.format(args.FIELD)]
    writer = mod.mkDictOutput('Joined_{}_FILE_{}'.format(wac_rac, curtime), fieldname_list=fieldnames)

    for row in reader1:

        entry = {'GEOID10': row['{}'.format(label)], '{}'.format(args.FIELD): row['C000']}

        writer.writerow(entry)
    bar.next()

    for row2 in reader2:
        entry = {'GEOID10': row2['{}'.format(label)], '{}'.format(args.FIELD): row2['C000']}

        writer.writerow(entry)
    bar.next()
    bar.finish()
    print('Files joined for single {} field'.format(args.FIELD))

