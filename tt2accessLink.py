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

def mkOutput(currentTime, fieldnames, name):
    outfile = open('output_{}_{}.txt'.format(name,curtime), 'w')
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    return writer

#Make a dictReader object from input file
def makeNestedDict(file):
    input = open(file, 'r')
    reader = csv.DictReader(input)
    #Initiate outter dict
    nest = {}
    for row in reader:
        origin = row['origin']
        destination = row['destination']
        deptime = row['deptime']
        PNR = row['PNR']
        minTT = row['minTT']
        if origin not in nest:
            nest[origin] = {}

            if deptime not in nest[origin]:
                nest[origin][deptime] = {}

                if destination not in nest[origin][deptime]:
                    nest[origin][deptime][destination] = {}

                    if PNR not in nest[origin][deptime][destination]:
                        nest[origin][deptime][destination][PNR] = minTT

        elif deptime not in nest[origin]:
                nest[origin][deptime] = {}

                if destination not in nest[origin][deptime]:
                    nest[origin][deptime][destination] = {}

                    if PNR not in nest[origin][deptime][destination]:
                        nest[origin][deptime][destination][PNR] = minTT

        elif destination not in nest[origin][deptime]:
                    nest[origin][deptime][destination] = {}

                    if PNR not in nest[origin][deptime][destination]:
                        nest[origin][deptime][destination][PNR] = minTT

        elif PNR not in nest[origin][deptime][destination]:
            nest[origin][deptime][destination][PNR] = minTT

    print("Created Nested Dictionary")
    return nest

#Make destination + job file into dict obj
def makeJobsDict(file):
    input = open(file, 'r')
    reader = csv.DictReader(input)
    jobs_dict = {}
    for row in reader:
        destin = row['GEOID10']
        jobs = row['C000']
        if jobs == '':
            jobs_dict[destin] = 0
        else:
            jobs_dict[destin] = int(jobs)
    print("Created Jobs Dictionary")
    return jobs_dict

def addThresholds(df):
    df.update(thresh300=[])
    df.update(thresh600=[])
    df.update(thresh900=[])
    df.update(thresh1200=[])
    df.update(thresh1500=[])
    df.update(thresh1800=[])
    df.update(thresh2100=[])
    df.update(thresh2400=[])
    df.update(thresh2700=[])
    df.update(thresh3000=[])
    df.update(thresh3300=[])
    df.update(thresh3600=[])
    df.update(thresh3900=[])
    df.update(thresh4200=[])
    df.update(thresh4500=[])
    df.update(thresh4800=[])
    df.update(thresh5100=[])
    df.update(thresh5400=[])

#Break open the TT dict object to start linking it with jobs
def restructureDF(tt_dict):
    internalDF = {}
    for origin, outter in tt_dict.items():
        for deptime, locations in tt_dict[origin].items():
            for destination, inner in tt_dict[origin][deptime].items():
                for pnr, tt in tt_dict[origin][deptime][destination].items():
                    #for a given origin + deptime, match all destinations per threshold list.
                        if origin not in internalDF:
                            internalDF[origin] = {}

                            if deptime not in internalDF[origin]:
                                internalDF[origin][deptime] = {}
                                internalDF2 = addThresholds(internalDF[origin][deptime])
                                #add threshold key and list value to above dict.

                                filterTT(internalDF2, origin, deptime, destination, tt)

                        elif deptime not in internalDF[origin]:
                            internalDF[origin][deptime] = {}
                            internalDF2 = addThresholds(internalDF[origin][deptime])
                            filterTT(internalDF2, origin, deptime, destination, tt)


    print("Restructured the Input DataFrame for Internal Purposes")
    return internalDF2

def filterTT(internal_dict, origin, deptime, destination, tt):

    if int(tt) <= 300:
        internal_dict[origin][deptime]['thresh300'].append(destination)

    elif int(tt) <= 600:
        internal_dict[origin][deptime]['thresh600'].append(destination)

    elif int(tt) <= 900:
        internal_dict[origin][deptime]['thresh900'].append(destination)

    elif int(tt) <= 1200:
        internal_dict[origin][deptime]['thresh1200'].append(destination)

    elif int(tt) <= 1500:
        internal_dict[origin][deptime]['thresh1500'].append(destination)

    elif int(tt) <= 1800:
        internal_dict[origin][deptime]['thresh1800'].append(destination)

    elif int(tt) <= 2100:
        internal_dict[origin][deptime]['thresh2100'].append(destination)

    elif int(tt) <= 2400:
        internal_dict[origin][deptime]['thresh2400'].append(destination)

    elif int(tt) <= 2700:
        internal_dict[origin][deptime]['thresh2700'].append(destination)

    elif int(tt) <= 3000:
        internal_dict[origin][deptime]['thresh3000'].append(destination)

    elif int(tt) <= 3300:
        internal_dict[origin][deptime]['thresh3300'].append(destination)

    elif int(tt) <= 3600:
        internal_dict[origin][deptime]['thresh3600'].append(destination)

    elif int(tt) <= 3900:
        internal_dict[origin][deptime]['thresh3900'].append(destination)

    elif int(tt) <= 4200:
        internal_dict[origin][deptime]['thresh4200'].append(destination)

    elif int(tt) <= 4500:
        internal_dict[origin][deptime]['thresh4500'].append(destination)

    elif int(tt) <= 4800:
        internal_dict[origin][deptime]['thresh4800'].append(destination)

    elif int(tt) <= 5100:
        internal_dict[origin][deptime]['thresh5100'].append(destination)

    elif int(tt) <= 5400:
        internal_dict[origin][deptime]['thresh5400'].append(destination)


def sumJobs(internal_df2):
    for origin, outter in internal_df2.items():
        for dptm, inner in internal_df2[origin].items():
            for thresh, dest_list in internal_df2[origin][dptm].items():
                count = 0
                for dest in dest_list:
                    count = count + int(jobsDict[dest])
                entry = {'origin': origin, 'deptime': dptm, 'threshold': thresh, 'jobs': count}
                writer.writerow(entry)


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
    fieldnames = ['origin', 'deptime', 'threshold', 'jobs']
    writer = mkOutput(curtime, fieldnames, 'tt2access')

    #Make input dictionaries
    ttDict = makeNestedDict(args.LINKED_TT)
    jobsDict = makeJobsDict(args.JOBS)


    internalDF2 = restructureDF(ttDict)
    sumJobs(internalDF2)

#-----END-----

