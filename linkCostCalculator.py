#This script reads in a path analyst file and calculates the cost per link given variables like average fuel price,
#time cost, vehicle depreciation cost, etc.

#NOTES:
#Assumes link length and speed are coming in as m and m/s
#Assumes city driving only--based on the provided adjustment factor as cited by Mengying

#Example Usage: kristincarlson~ python linkCostCalculator.py -path
# /Users/kristincarlson/Dropbox/Bus-Highway/Task3/TTMatrixLink_Testing/UnitTest_path/o2pnr_path_auto-results.csv
# -mpg_adj 0.8354 -price_per_gal 232.7


#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import csv

#################################
#           FUNCTIONS           #
#################################
def mpgFunction(speed):
    #start by converting speed from m/s to miles per hour
    speed_mph = float(speed) * 2.23694
    mpg = 0.658 + (0.947 * speed_mph) - (0.009 * speed_mph**2)
    print('MPG', mpg)
    return mpg

def mpgAdjustment(mpg):
    mpg_adj = mpg * ADJ
    print('MPG_ADJ', mpg_adj)
    return mpg_adj

def costPerMile(mpg):
    #Mi/Gall * 1 Gal/price = mile/cost then take reciprocal
    cost_per_mile = 1/(mpg / PRICE)
    print('cost per mile', cost_per_mile)
    return cost_per_mile

def calcLinkCost(link_length, cost_mile):
    #Convert incoming link length from meters to miles
    mi = float(link_length) * 0.000621371
    cost = cost_mile * mi
    return cost
def removeSciNot(value):
    reformat = '{:.10f}'.format(value)
    return reformat

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-path', '--PATH_FILE', required=True, default=None)  #ENTER AS full file path to path_analyst file
    parser.add_argument('-mpg_adj', '--MPG_ADJUSTMENT', required=True, default=32400)  #Assume city driving adjustment of 0.8354
    parser.add_argument('-price_per_gal', '--PRICE_PER_GALLON_REGULAR', required=True, default=32400)  # $2.327 in MN in 2017, enter in cents (232.7 cents)
    args = parser.parse_args()

    #Read in constants
    ADJ = float(args.MPG_ADJUSTMENT)
    PRICE = float(args.PRICE_PER_GALLON_REGULAR)


    reader = mod.readInToDict(args.PATH_FILE)
    #Make writer
    fieldnames = ['origin', 'deptime', 'destination', 'path_seq', 'link_cost']
    writer = mod.mkDictOutput('link_costs_matrix_{}'.format(curtime), fieldname_list=fieldnames)


    for row in reader:
        #Calc the MPG based on Mengying's formula as a function of link speed
        mpg = mpgAdjustment(mpgFunction(row['link_speed']))
        #Incorporate average retail fuel price
        cost_mile = costPerMile(mpg)
        #Multiply by link length
        link_cost = removeSciNot(calcLinkCost(row['link_length'], cost_mile))
        #Write entry, remember that the written price per link is in cents
        entry = {'origin': row['origin'], 'deptime': row['deptime'], 'destination': row['destination'], 'path_seq': row['path_seq'], 'link_cost': link_cost}
        writer.writerow(entry)
    print('Link cost analysis finished')
    mod.elapsedTime(start_time)



