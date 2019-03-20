#This script reads in a csv containing multiple fields of rows to be extracted from the larger TT matrix file
# then writes the rows out to a subset file.


#################################
#           IMPORTS             #
#################################

import argparse
from myToolsPackage import matrixLinkModule as mod
from progress import bar


#################################
#           FUNCTIONS           #
#################################




#################################
#           OPERATIONS          #
#################################


if __name__ == '__main__':

    curtime = mod.startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-tt', '--TTMATRIX_FILE', required=True, default=None)
    parser.add_argument('-tt_row_count', '--TTMATRIX_SIZE', required=True, default=None) #give the approximate number of rows
    parser.add_argument('-set', '--SELECT_FROM_FILE', required=True, default=None)
    parser.add_argument('-id', '--ID_FIELD', required=True, default=None) #Give the origin id field name ex. label, origin, PnRNumInt
    args = parser.parse_args()

    fieldnames = ['origin', 'destination', 'deptime', 'traveltime']
    writer = mod.mkOutput('ttextract', fieldnames, curtime)

    bar = bar.Bar(message='Processing', fill='@', suffix='%(percent)d%%', max=int(args.TTMATRIX_SIZE)) #Adjust
    ttmatrix_dict = mod.readInToDict(args.TTMATRIX_FILE)
    id_dict = mod.readInToDict(args.SELECT_FROM_FILE)

    for rowTT in ttmatrix_dict:
        bar.next()
        for rowID in id_dict:
            print('thinking.....')
            if int(rowTT['destination']) == int(rowID['{}'.format(args.ID_FIELD)]):
                writer.writerow(rowTT)
                print('writing')
    bar.finish()

