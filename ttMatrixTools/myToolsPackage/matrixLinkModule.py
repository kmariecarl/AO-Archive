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
#Only works for times between 1:00 - 9:59! Due to the way hours are determinded
def convert2Sec(timeVal):
    #'timeVal' is a number like '0632' for 6:32 AM. The list comprehension breaks the into apart.
    list = [i for i in timeVal]
    #Grab the first two digits which are the hours -> convert to seconds
    hours = (int(list[0]) + int(list[1])) * 3600
    #Grab the third and fouth digits which are the minutes -> convert to seconds.
    minutes = int('{}{}'.format(list[2],list[3]))*60
    seconds = hours + minutes
    return seconds

#A function to reverse the convert2Sec(), timeVal comes in as an Integer:
def back2Time(timeVal):
    time_string = time.strftime('%H:%M:%S', time.gmtime(timeVal))
    list = [i for i in time_string]
    time_string_trim = list[0] + list[1] + list[3] + list[4]
    return str(time_string_trim)
#A function for calculating the integer value of date/time and attaching to file names
def startTimer():
    start_time = time.time()
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return start_time, currentTime

#A function that prints out the elapsed calculation time
def elapsedTime(start_time):
    print("Elapsed Time: ", time.time() - start_time)

#This function writes out a list of values separated by commas and ending in a comma. Single line, no rows or carriage return.
def writeList(file_name, list, currentTime):
    with open('{}_{}.txt'.format(file_name, currentTime), 'w') as outlist:
        outlist.writelines(list)


# This function reads in the list file created using writeList() above and creates a new list for the current program to use
def readList(file, type):
    with open(file, 'r') as infile:
        csvreader = csv.reader(infile, delimiter=',')
        list = []
        for item in csvreader:  # Each item is actually the entire list of PNRs or deptimes.
            for val in item:  # Each val is the PNR or deptime
                if val != '':  # The last val is empty due to the matrixBreaker output format
                    if type == 'integer':
                        list.append(int(val))
                    elif type == 'str':
                        list.append(str(val))
        list_sort = sorted(list)
    print("Input list created:", list_sort)
    return list_sort

def readInToDict(file):
    infile =  open(file, 'r')
    dict_obj = csv.DictReader(infile)
    return dict_obj  # consider wrapping this in a list to get a multi-use file! i.e. list(dict_obj)


#A functiont to write .txt files given a name string and pre-made field name list. Fieldname list example: ['origin', 'destination', 'deptime', 'traveltime']
#Name string example: 'PNR_{}_{}'.format(PNR_number, deptime)
#**Optional_values parameter looks for curtime variable to be passed in.
def mkOutput(file_name, *args, **kwargs):
    '''Expects name_string, fieldname_list, and optionally the curtime variable'''
    if 'curtime' in args:
        outfile = open('{}_{}.csv'.format(file_name, 'curtime'), 'w', newline='')
    else:
        outfile = open('{}.csv'.format(file_name), 'w', newline='')
    writer = csv.writer(outfile, delimiter=',')
    #Use the fieldname argument if you want to add a row of fieldnames to the top of your file
    if 'fieldnames' in kwargs:
        writer.writerow('fieldnames')
    return writer

#This is almost exactly the same function as above but uses a dict writer as opposed to a regular writer object
def mkDictOutput(file_name, fieldname_list, *args, **kwargs):
    if 'curtime' in args:
        outfile = open('{}_{}.csv'.format(file_name, 'curtime'), 'w', newline='')
    else:
        outfile = open('{}.csv'.format(file_name), 'w', newline='')
    writer = csv.DictWriter(outfile, delimiter=',', fieldnames=fieldname_list)
    writer.writeheader()
    return writer




