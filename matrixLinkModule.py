#This module contains common functions used throughout the matrix linking process.
#This module is not run on its own
#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
import numpy
import glob
from collections import defaultdict

#################################
#           FUNCTIONS           #
#################################

#A function that converts time strings to seconds integers
def convert2Sec(timeVal):
    #'timeVal' is a number like '0632' for 6:32 AM. The list comprehension breaks the into apart.
    list = [i for i in timeVal]
    #Grab the first two digits which are the hours -> convert to seconds
    hours = (int(list[0]) + int(list[1])) * 3600
    #Grab the third and fouth digits which are the minutes -> convert to seconds.
    minutes = int('{}{}'.format(list[2],list[3]))*60
    seconds = hours + minutes
    return seconds

#A function for calculating the integer value of date/time and attaching to file names
def startTimer():
    start_time = time.time()
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return start_time, currentTime

#A function that prints out the elapsed calculation time
def elapsedTime(start_time):
    elapsed_time = time.time() - start_time
    print("Elapsed Time: ", elapsed_time)

#This function writes out a list of values separated by commas and ending in a comma. Single line, no rows or carriage return.
def writeList(file_name, list):
    with open('{}.txt'.format(file_name), 'w') as outlist:
        for item in list:
            outlist.write(str(item))
            outlist.write(',')

# This function reads in the list file created using writeList() above and creates a new list for the current program to use
def readList(file, type=int):
    with open('{}'.format(file), 'r') as infile:
        csvreader = csv.reader(infile, delimiter=',')
        list = []
        for item in csvreader:  # Each item is actually the entire list of PNRs or deptimes.
            for val in item:  # Each val is the PNR or deptime
                if val != '':  # The last val is empty due to the matrixBreaker output format
                    list.append(type(val))
        list_sort = sorted(list)
    print("Input list created:", list_sort)
    return list_sort

#A functiont to write .txt files given a name string and pre-made field name list. Fieldname list example: ['origin', 'destination', 'deptime', 'traveltime']
#Name string example: 'PNR_{}_{}'.format(PNR_number, deptime)
#**Optional_values parameter looks for curtime variable to be passed in.
def mkOutput(file_name, fieldname_list, **optional_values):
    if 'curtime' in optional_values:
        outfile = open('{}_{}.txt'.format(file_name, 'curtime'), 'w', newline='')
    else:
        outfile = open('{}.txt'.format(file_name), 'w', newline='')
    writer = csv.writer(outfile, delimiter=',')
    writer.writerow(fieldname_list)
    return writer
