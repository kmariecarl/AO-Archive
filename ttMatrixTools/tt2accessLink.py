#This script loads a linked travel time matrix dataset and jobs information in order to
#link travel times for origin + deptime pairs to jobs.

#Example Usage: kristincarlson~: python ~/Dropbox/Bus-Highway/Programs/gitBus-Highway/tt2accessLink.py -link output_20171201.txt -jobs 7countydest2jobs.txt

#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
from collections import OrderedDict

#################################
#           FUNCTIONS           #
#################################


def convert2Sec(timeVal):
    #'timeVal' is a number like '0632' for 6:32 AM. The list comprehension breaks the into apart.
    list = [i for i in timeVal]
    #Grab the first two digits which are the hours -> convert to seconds
    hours = (int(list[0]) + int(list[1])) * 3600
    #Grab the third and fouth digits which are the minutes -> convert to seconds.
    minutes = int('{}{}'.format(list[2],list[3]))*60
    seconds = hours + minutes
    return seconds

def startTimer():
    # Start timing
    #Use start_time for tracking elapsed runtime.
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return start_time, currentTime

#A function that prints out the elapsed calculation time
def elapsedTime():
    elapsed_time = time.time() - start_time
    print("Elapsed Time: ", elapsed_time)

#A standard way to open a new file to write to.
def mkOutput(currentTime, fieldnames, name):
    outfile = open('output_{}_{}.txt'.format(name,curtime), 'w')
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    return writer

#Make a dictReader object from input file which mimics the spDict formation made in "TTMatrixLink.py"
#{origin:{deptime:{destination:TT,
#                  destination:TT,
#                  destination:TT},
#         deptime:{destination:TT,
#                  destination:TT,
#                  destination:TT}},
# origin:{...

def makeNestedDict(linked_tt_file):
    input = open(linked_tt_file, 'r')
    reader = csv.DictReader(input)
    #Initiate outter dict
    nest = {}
    for row in reader:
        #reassign dictionary keys to function variables. No need to handle PNR information so it is dropped.
        origin = row['origin']
        destination = row['destination']
        deptime = row['deptime']
        minTT = row['traveltime']
        #Begin layering dictionary
        if origin not in nest:
            nest[origin] = {}

            if deptime not in nest[origin]:
                nest[origin][deptime] = {}

                if destination not in nest[origin][deptime]:
                    nest[origin][deptime][destination] = minTT

        elif deptime not in nest[origin]:
                nest[origin][deptime] = {}

                if destination not in nest[origin][deptime]:
                    nest[origin][deptime][destination] = minTT

        elif destination not in nest[origin][deptime]:
                    nest[origin][deptime][destination] = minTT

    print("Created Nested Dictionary")
    return nest

#Make destination + job file into dict obj
def makeJobsDict(jobs_file):
    input = open(jobs_file, 'r')
    reader = csv.DictReader(input)
    jobs_dict = {}
    for row in reader:
        destin = row['GEOID10']
        jobs = row['C000']
        #Make empty values into zeros
        if jobs == '':
            jobs_dict[destin] = 0
        else:
            jobs_dict[destin] = int(jobs)
    print("Created Jobs Dictionary")
    return jobs_dict

#When a new origin_deptime combo has been found, add key=threshold, value = empty destination list
#Each list will be filled by the filterTT function.
def addThresholds(df, orgn, dptm):
    df[orgn][dptm]['300'] = []
    df[orgn][dptm]['600'] = []
    df[orgn][dptm]['900'] = []
    df[orgn][dptm]['1200'] = []
    df[orgn][dptm]['1500'] = []
    df[orgn][dptm]['1800'] = []
    df[orgn][dptm]['2100'] = []
    df[orgn][dptm]['2400'] = []
    df[orgn][dptm]['2700'] = []
    df[orgn][dptm]['3000'] = []
    df[orgn][dptm]['3300'] = []
    df[orgn][dptm]['3600'] = []
    df[orgn][dptm]['3900'] = []
    df[orgn][dptm]['4200'] = []
    df[orgn][dptm]['4500'] = []
    df[orgn][dptm]['4800'] = []
    df[orgn][dptm]['5100'] = []
    df[orgn][dptm]['5400'] = []

#internalDF looks like:
#{origin:{deptime:{thresh1:[dest1, dest2, dest3...],
#                  thresh2:[dest1, dest2, dest3...],
#                  thresh3:[dest1, dest2, dest3...]}
#         deptime:{thresh1:[dest1, dest2, dest3...],
#                  thresh2:[dest1, dest2, dest3...],
#                  thresh3:[dest1, dest2, dest3...]}}
#origin:{...

#Break open the spTT dict object to start linking it with jobs
def restructureDF(tt_dict):
    internalDF = {}
    for origin, outter in tt_dict.items():
        for deptime, locations in tt_dict[origin].items():
            for destination, tt in tt_dict[origin][deptime].items():
                #Check if origin has been added to the internal df.
                if origin not in internalDF:
                    internalDF[origin] = {}
                    #Check if deptime has been added to the origin's dictionary
                    if deptime not in internalDF[origin]:
                        internalDF[origin][deptime] = {}
                        #If this orgn + dptm combo has not been arrived at previously, add the threshold keys
                        addThresholds(internalDF, origin, deptime)
                        #Place the tt and destination into all bins that are >= the value of tt
                        filterTT(internalDF, origin, deptime, destination, tt)
                #If origin has been found but deptime has not been, then do the following
                elif deptime not in internalDF[origin]:
                    internalDF[origin][deptime] = {}
                    # If this orgn + dptm combo has not been arrived at previously, add the threshold keys
                    addThresholds(internalDF, origin, deptime)
                    # Place the tt and destination into all bins that are >= the value of tt
                    filterTT(internalDF, origin, deptime, destination, tt)
                #Once the origin and deptime have been placed, add all remaining destinations to the applicable threshold lists
                elif origin in internalDF and deptime in internalDF[origin]:
                    filterTT(internalDF, origin, deptime, destination, tt)
        print("Origin {} filtered to appropriate thresholds".format(origin))

    print("Created Internal DF and Made Destination Lists by Threshold")
    return internalDF

#Place the current row's destination into threshold bins based on its travel time
def filterTT(internal_df, origin, deptime, destination, tt):
    #Create a list of thresholds that this destination can be reached in <= time
    applicable_thresh_list = []
    for item in threshold_list:
        #OTP puts TT in minutes then rounds down, the int() func. always rounds down.
        #Think of TT in terms of 1 min. bins. Ex. A dest with TT=30.9 minutes should not be placed in the 30 min TT list.
        minbin = int(int(tt)/60)
        minthresh = int(int(item)/60)
        if minbin < minthresh:
            #print('minbin:', minbin, 'minthresh', minthresh)
            applicable_thresh_list.append(item)
    #Once the applicable thresholds have been found, place destination into lists
    for thresh in sorted(applicable_thresh_list):
        internal_df[origin][deptime][thresh].append(destination)

# #Sort each of the nested dictionaries
# def sortMe(internal_df):
#     ordered_origins = OrderedDict(sorted(internal_df.items()))
#     for origin, outter in ordered_origins.items():
#         ordered_deptime = OrderedDict(sorted(ordered_origins[origin].items()))
#
#     return ordered_deptime

#Each orgn_dptm_threshold has a list of destinations that can be reached.
#Sum the jobs associated with each destination and write out the rows
def sumJobs(internal_df):
    for origin, outter in internal_df.items():
        for dptm, inner in internal_df[origin].items():
            for thresh, dest_list in sorted(internal_df[origin][dptm].items()):
                count = 0
                for dest in dest_list:
                    count = count + int(jobsDict[dest])
                entry = {'label': origin, 'deptime': dptm, 'threshold': thresh, 'jobs': count}
                writer.writerow(entry)
        print("Origin {} matched to jobs".format(origin))


#################################
#           OPERATIONS          #
#################################


if __name__ == '__main__':

    start_time, curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-link', '--LINKED_TT', required=True, default=None)
    parser.add_argument('-jobs', '--JOBS', required=True, default=None)
    args = parser.parse_args()

    #Create output file
    fieldnames = ['label', 'deptime', 'threshold', 'jobs']
    writer = mkOutput(curtime, fieldnames, 'tt2access')

    #Make input dictionaries
    ttDict = makeNestedDict(args.LINKED_TT)
    jobsDict = makeJobsDict(args.JOBS)

    #Make threshold list to check travel times against.
    threshold_list = ['300', '600', '900', '1200', '1500', '1800', '2100', '2400', '2700', '3000', '3300', '3600',
                      '3900', '4200',
                      '4500', '4800', '5100', '5400']

    internalDF = restructureDF(ttDict)
    #internalDFSort = sortMe(internalDF)
    sumJobs(internalDF)

#-----END-----