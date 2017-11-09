#This script can compare two travel time matricies and output metrics on the difference between them.

#ASSUMPTION: The algorithm assumes that the baseline TT matrix includes

#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
import numpy


#################################
#           FUNCTIONS           #
#################################

def startTimer():
    # Start timing
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return currentTime

#Make a dictReader object from input file
def makeDict(file):
    input = open(file, 'r')
    reader = csv.DictReader(input)
    print('Making Dictionaries')
    return reader

def labelize(row):
    label = str(row['origin']) + '_' + str(row['destination']) + '_' + str(row['deptime'])
    return label

def doMath(row_bs, row_up):
    rw_df = int(row_up['traveltime']) - int(row_bs['traveltime'])
    rw_pct = round(rw_df / float(row_bs['traveltime']), 6)
    print('Doing Math')
    return rw_df, rw_pct
#
# #Turn off percentile functions
def average(dictionary):
    avg_dict = {}
    for pair, list in dictionary.items():
        avg_val = numpy.nanmean(list)
        avg_dict[pair] = avg_val
        print('Averaging')
    return avg_dict

#Averaging function with percentiles

# def average(dictionary):
#     avg_dict = {}
#     for pair, list in dictionary.items():
#         print(pair, list)
#
#         percentile = int(numpy.percentile(list, 75))
#         print(percentile)
#         above75 = []
#         for number in list:
#             if number > percentile:
#                 print('here1')
#                 above75.append(number)
#             else:
#                 above75.append(numpy.nan)
#                 print('here2')
#         avg_val = numpy.nanmean(above75)
#         avg_dict[pair] = avg_val
#         print(avg_val)
#         print('Averaging')
#     return avg_dict


def mkOutput(currentTime, fieldnames):
    outfile = open('output_{}.txt'.format(curtime), 'w')  # , newline=''
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)  # , quotechar = "'"
    writer.writeheader()
    return writer
#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-bs', '--BASE_TT_FILE', required=True, default=None)
    parser.add_argument('-updt', '--UPDATE_TT_FILE', required=True, default=None)
    args = parser.parse_args()

    fieldnames = ['origin', 'destination', 'avgrwdf', 'avgrwpct'] #Not including raw values 'rwbstt', 'rwupdttt',
    writer = mkOutput(curtime, fieldnames)


    baseDict = makeDict(args.BASE_TT_FILE)
    with open(args.UPDATE_TT_FILE) as f:
        updtDict = csv.DictReader(f)

        #Make a temporary dictionary for holding OD and df/pct values so that averaging can happen at the end.
        dftempDict = {}
        pcttempDict = {}
        #Record a list of unique labels for cycling through during the averaging process.
        labelList = []
        #Begin iterating through base dict and then the update dict.
        for rowbs in baseDict:
            bslabel = labelize(rowbs)
            for rowup in updtDict:
                uplabel = labelize(rowup)
                if bslabel == uplabel:
                    newLabel = str(rowbs['origin']) + '_' + str(rowbs['destination'])
                    rwdf, rwpct = doMath(rowbs, rowup)

                    #Break the base label down to compare with newlabel in nested dict
                    breakLabel = bslabel.split('_')
                    # Add a nested dict for each label which will be filled with all values
                    if newLabel not in dftempDict:
                        dftempDict[newLabel] = []
                        if breakLabel[0] + '_' + breakLabel[1] == newLabel:
                            dftempDict[newLabel].append(rwdf)
                    elif breakLabel[0] + '_' + breakLabel[1] == newLabel:
                        dftempDict[newLabel].append(rwdf)

                    if newLabel not in pcttempDict:
                        pcttempDict[newLabel] = []
                        if breakLabel[0] + '_' + breakLabel[1] == newLabel:
                            pcttempDict[newLabel].append(rwpct)

                    elif breakLabel[0] + '_' + breakLabel[1] == newLabel:
                        pcttempDict[newLabel].append(rwpct)


            f.seek(0)
            next(updtDict)

    #Make a function for this!
        avg_df_dict = average(dftempDict)
        avg_pct_dict = average(pcttempDict)
        for row_df, value_df in avg_df_dict.items():
            for row_pct, value_pct in avg_pct_dict.items():
                if row_df== row_pct:
                    break_string = row_df.split('_')
                    origin = break_string[0]
                    destination = break_string[1]
                    entry = {'origin': origin, 'destination': destination, 'avgrwdf': value_df, 'avgrwpct': round(value_pct, 6)}
                    writer.writerow(entry)
                    print('writing')
