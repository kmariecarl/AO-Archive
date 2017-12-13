#This script can compare two travel time matricies and output metrics on the difference between them.

#NOTE: OD pairs that cannot be reached within the MaxTime threshold are assigned the value of 2147483647 with Analyst-mx1

#ASSUMPTION: The algorithm assumes that the baseline TT matrix includes

#Example Usage: kristincarlson$ python ~/Dropbox/Bus-Highway/Programs/gitPrograms/processTTResults.py
# -bs Local_TT-results.csv -updt Complete_TT-results.csv

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
    internalDict = {}
    #Create a dictionary with touple as key and tt as value
    #{origin, destination, deptime : traveltime}
    for row in reader:
        internalDict[(row['origin'], row['destination'], row['deptime'])] = row['traveltime']

    print('Making Internal Dictionaries')
    return internalDict

def doMath(rowbs):
    # Assign touple keys to variable names for easier interpretation
    origin = rowbs[0]
    destination = rowbs[1]
    deptime = rowbs[2]

    # For the current base row, the equivalent O_D_Deptime pair is found in the update dict and both are used in doMath()
    rwdf, rwpct = takeDiff(int(baseDict[(origin, destination, deptime)]), int(updtDict[(origin, destination, deptime)]))

    # Make a new dataframe that contains origins with nested destinations, and a list of TTs--one for each deptime.
    # Make two dictionaries even though a list of lists could be used to store both df and pct value lists.
    if rwdf != 2147483647 and rwpct != 2147483647:

        if origin not in dftempDict:
            dftempDict[origin] = {}
            if destination not in dftempDict[origin]:
                dftempDict[origin][destination] = [rwdf]

        elif destination not in dftempDict[origin]:
            dftempDict[origin][destination] = [rwdf]
        else:
            dftempDict[origin][destination].append(rwdf)

        if origin not in pcttempDict:
            pcttempDict[origin] = {}
            if destination not in pcttempDict[origin]:
                pcttempDict[origin][destination] = [rwpct]

        elif destination not in pcttempDict[origin]:
            pcttempDict[origin][destination] = [rwpct]
        else:
            pcttempDict[origin][destination].append(rwpct)

    else:
        exclude.append(str(origin) + '_' + str(destination) + '_' + str(deptime))

#If either base or update TT is Max, the comparison cannot be made, so assign the max value to rw_df and rw_pct
def takeDiff(bs_tt, up_tt):
    if bs_tt == 2147483647 or up_tt == 2147483647:
        rw_df = 2147483647
        rw_pct = 2147483647

    else:
        rw_df = up_tt - bs_tt
        rw_pct = rw_df / bs_tt

    return int(rw_df), rw_pct

def findAverages(dftempDict, pcttempDict):
    # Now average the rwdf and rwpct columns across deptimes for each OD pair.
    # Break the dftempdict and match OD to pctTempDict OD
    print('Averaging')
    for or_df, val_df in dftempDict.items():
        for dest_df, list_df in dftempDict[or_df].items():
            avg_df = numpy.array(dftempDict[or_df][dest_df]).mean()
            avg_pct = numpy.array(pcttempDict[or_df][dest_df]).mean()
            entry = {'origin': or_df, 'destination': dest_df, 'avgrwdf': int(avg_df), 'avgrwpct': round(avg_pct, 6)}
            writer.writerow(entry)


def mkOutput(currentTime, fieldnames, name):
    outfile = open('output_{}_{}.txt'.format(name, curtime), 'w')  # , newline=''
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
    writer = mkOutput(curtime, fieldnames, 'processedTT')


    baseDict = makeDict(args.BASE_TT_FILE)
    #with open(args.UPDATE_TT_FILE) as f:
    updtDict = makeDict(args.UPDATE_TT_FILE)

    #Make a temporary dictionary for holding OD and df/pct values so that averaging can happen at the end.
    dftempDict = {}
    pcttempDict = {}
    # Do not append MaxTime values to new dictionaries
    exclude = []
    #Begin iterating through base dict.
    print('Doing Math')
    for rowbs in baseDict:
        doMath(rowbs)

    findAverages(dftempDict, pcttempDict)

    print("These OD + Deptime combos were excluded during the averaging process")
    for item in sorted(exclude):
        print(item)
    print('Writing Finished')

#-----END----