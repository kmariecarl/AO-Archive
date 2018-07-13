#This script reads in a path analyst file and calculates the cost per link given variables like average fuel price,
#time cost, vehicle depreciation cost, etc. Also maintains the travel time per link for later use to calculate TT matrices.

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
import time
from myToolsPackage.progress import bar
import csv

#################################
#           FUNCTIONS           #
#################################
def convertLinkLength(link_length_km):
    #Convert incoming link length from meters to miles
    mi = float(link_length_km) * 0.000621371
    return mi

def convertLinkSpeed(link_speed_m_s):
    #start by converting speed from m/s to miles per hour
    speed_mph = float(link_speed_m_s) * 2.23694

    return speed_mph




def calcFuelCost(row, length_mi, speed_mph):
    #Calc the MPG based on Mengying's formula as a function of link speed
    mpg = mpgFunction(speed_mph)
    #Incorporate average retail fuel price
    cost_mile = costPerMile(mpg)
    #Multiply by link length
    link_cost = removeSciNot(calcLinkCost(length_mi, cost_mile))
    return link_cost

def mpgFunction(speed_mph):

    if speed_mph > 45:
        adj = 1.170
    else:
        adj = 0.861
    mpg = 0.658 + (0.947 * speed_mph) - (0.009 * speed_mph**2)
    mpg_adj = mpg * adj
    return mpg_adj

def costPerMile(mpg):
    #Mi/Gall * 1 Gal/price = mile/cost then take reciprocal
    cost_per_mile = 1/(mpg / PRICE)
    return cost_per_mile


def calcLinkCost(length_mi, cost_mile):
    cost = cost_mile * length_mi
    return cost

def removeSciNot(value):
    reformat = '{:.10f}'.format(value)
    return float(reformat)


def calcRepairCost(row, speedMPH, length_mi):
    if speedMPH > 45:
        repair_link_cost = length_mi * REP_HWY

    else:
        repair_link_cost = length_mi * REP_CITY
    return repair_link_cost

def calcDepreciationCost(speed_mph, length_mi):
    if speed_mph > 45:
        dep_link_cost = length_mi * DEP_HWY
    else:
        dep_link_cost = length_mi * DEP_CITY

    return dep_link_cost

def calcIRSCost(lenght_mi):
    irs_cost = lenght_mi * IRS
    return irs_cost


def calcVOTCost(link_time_sec):
    #convert link time from seconds to hours
    person_hour = float(link_time_sec)/3600
    vot_cost = person_hour * VOT #result will be in cents
    return vot_cost



#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=51813000) #For 150 links x 30303 x 1 x 114 = 7.65 GB
    #List of approximate line counts for different file sizes:
    #max = 110696205 for 300 links x 3027 x 1 x 114 = 7.65 GB
    #max = 261617092 For 60 links x 30303 x 13 x 114 = 18 GB input file


    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-path', '--PATH_FILE', required=True, default=None)  #ENTER AS full file path to path_analyst file
    parser.add_argument('-price_per_gal', '--PRICE_PER_GALLON_REGULAR', required=True, default=233.5)  # $2.335 in MN in 2015, enter in cents (233.5 cents)
    parser.add_argument('-vot', '--VALUE_OF_TIME', required=True, default=1803.0)  # MnDOT research states $18.30 or 1803.0 cents
    parser.add_argument('-repaircity', '--REPAIR_COST_CITY', required=True, default=1.932) #Combined (auto + pickup) city Maintenance and Repair costs in cents per veh-mi for 2015
    parser.add_argument('-repairhwy', '--REPAIR_COST_HIGHWAY', required=True, default=1.703) #Combined (auto + pickup) highway Maintenance and Repair costs in cents per veh-mi for 2015
    parser.add_argument('-depcity', '--DEPRECIATION_COST_CITY', required=True, default=3.00) #Combined city vehicle depreciation cost for 2015
    parser.add_argument('-dephwy', '--DEPRECIATION_COST_HIGHWAY', required=True, default=2.55) #Combined highway vehicle depreciation cost for 2015
    parser.add_argument('-irs', '--IRS_MILEAGE', required=True, default=57.5) #IRS mileage reimbursement which includes fuel and avg vehicle wear and tear for 2015 (i.e. depreciation by distance)

    args = parser.parse_args()

    #Read in constants
    PRICE = float(args.PRICE_PER_GALLON_REGULAR)
    VOT = float(args.VALUE_OF_TIME)
    REP_CITY = float(args.REPAIR_COST_CITY)
    REP_HWY = float(args.REPAIR_COST_HIGHWAY)
    DEP_CITY = float(args.DEPRECIATION_COST_CITY)
    DEP_HWY = float(args.DEPRECIATION_COST_HIGHWAY)
    IRS = float(args.IRS_MILEAGE)


    reader = mod.readInToDict(args.PATH_FILE)
    #Make writer
    fieldnames = ['origin', 'deptime', 'destination', 'path_seq', 'link_time', 'fuel_cost', 'repair_cost', 'depreciation_cost', 'irs_cost', 'vot_cost']
    writer = mod.mkDictOutput('link_costs_matrix_{}'.format(curtime), fieldname_list=fieldnames)

    #Initiate counting lists
    count = 0
    or_set = []
    dep_set = []
    dest_set = []
    nullCounter = 0

    for row in reader:

        #Due to missing data problems, need to count origins, deptimes, destinations
        if row['origin'] not in or_set:
            or_set.append(row['origin'])
        if row['deptime'] not in dep_set:
            dep_set.append(row['deptime'])
        if row['destination'] not in dest_set:
            dest_set.append(row['destination'])

        if row['link_length'] == None or row['link_speed'] == None or row['link_time'] == None:
            nullCounter += 1
            continue

        lengthMI = convertLinkLength(row['link_length']) #starts in km need miles
        speedMPH= convertLinkSpeed(row['link_speed']) #Starts in m need miles
        fuelCost = calcFuelCost(row, lengthMI, speedMPH) #Fuel price = f(speed) * price/gallon

        repairCost = calcRepairCost(row, speedMPH, lengthMI)
        depCost = calcDepreciationCost(speedMPH, lengthMI)
        irsCost = calcIRSCost(lengthMI)
        votCost = calcVOTCost(row['link_time'])


        #Write entry, remember that the written price per link is in cents
        entry = {'origin': row['origin'], 'deptime': row['deptime'], 'destination': row['destination'], 'path_seq': row['path_seq'], 'link_time': row['link_time'], 'fuel_cost': round(fuelCost, 3),
                 'repair_cost': round(repairCost, 3), 'depreciation_cost': round(depCost, 3), 'irs_cost': round(irsCost, 3), 'vot_cost': round(votCost, 3)}
        writer.writerow(entry)
        bar.next()
        count += 1

    bar.finish()
    print('Final row count=', count)
    print("Number of origins visited:", len(or_set))
    print("Number of deptimes:", len(dep_set))
    print("Number of destinations visited:", len(dest_set))
    print("Number of links without a speed: ", nullCounter)

    print('Link cost analysis finished')
    mod.elapsedTime(start_time)




