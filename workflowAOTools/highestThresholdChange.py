#This script adds a new field to the processAccessResults4.py csv, the threshold with the highest percent change and the value of the percent change

#This was used in Task3 of the bus-highway project to identify the distribution of biggest accessibility changes
#across the landscape of PNR facilities

#EXAMPLE USAGE: kristincarlson$ python highestThresholdChange.py -r processed_access_results.csv

#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
import numpy
import timeit

#################################
#           FUNCTIONS           #
#################################


def startTimer():
    # Start timing
    #Use start_time for tracking elapsed runtime.
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return start_time, currentTime

def makeDict(file):
    input = open(file, 'r')
    reader = csv.DictReader(input)
    return reader

def mkOutput(currentTime, fieldnames, name):
    outfile = open('output_{}_{}.txt'.format(name,curtime), 'w')
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    return writer

if __name__ == '__main__':

    start_time, curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--PROCESSED_FILE', required=True, default=None)
    args = parser.parse_args()

    fieldnames = ['label','wt_raw_chg','wt_pct_chg','rwdf5400','rwpct5400','rwbs5400','rwchg5400','rwdf5100','rwpct5100',
                  'rwbs5100','rwchg5100','rwdf4800','rwpct4800','rwbs4800','rwchg4800','rwdf4500','rwpct4500','rwbs4500',
                  'rwchg4500','rwdf4200','rwpct4200','rwbs4200','rwchg4200','rwdf3900','rwpct3900','rwbs3900','rwchg3900',
                  'rwdf3600','rwpct3600','rwbs3600','rwchg3600','rwdf3300','rwpct3300','rwbs3300','rwchg3300','rwdf3000',
                  'rwpct3000','rwbs3000','rwchg3000','rwdf2700','rwpct2700','rwbs2700','rwchg2700','rwdf2400','rwpct2400',
                  'rwbs2400','rwchg2400','rwdf2100','rwpct2100','rwbs2100','rwchg2100','rwdf1800','rwpct1800','rwbs1800',
                  'rwchg1800','rwdf1500','rwpct1500','rwbs1500','rwchg1500','rwdf1200','rwpct1200','rwbs1200','rwchg1200',
                  'rwdf900','rwpct900','rwbs900','rwchg900','rwdf600','rwpct600','rwbs600','rwchg600','rwdf300','rwpct300',
                  'rwbs300','rwchg300','pct_chg','topthresh']
    writer = mkOutput(curtime, fieldnames, 'top_thresh')

    data = makeDict(args.PROCESSED_FILE)

    pct_list = ['rwpct5400', 'rwpct5100', 'rwpct4800', 'rwpct4500', 'rwpct4200', 'rwpct3900', 'rwpct3600', 'rwpct3300',
                'rwpct3000', 'rwpct2700', 'rwpct2400', 'rwpct2100', 'rwpct1800', 'rwpct1500', 'rwpct1200', 'rwpct900',
                'rwpct600', 'rwpct300']

    for row in data:
        max_list=[]
        for rwpct in pct_list:
            max_list.append(row[rwpct])

        top_thresh = max(row, key=row.get)
        pct_chg = row[top_thresh]
        row['pct_chg'] = pct_chg
        row['topthresh']=top_thresh

        writer.writerow(row)
    print('Top Threshold Accessibility Results Finished')



