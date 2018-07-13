#This script finds the total cost of a series of links between an OD pair based on the linkCostCalculator.py output.
#This script also creates a travel time matrix from the path information

#Need to add fixed costs and trip based parking costs and maybe mnpass costs to the path output, because these fixed costs
#are trip based!!

#Need to implement scenario columns

#This code looks ugly but it works very well.

#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import time
from myToolsPackage.progress import bar

#################################
#           FUNCTIONS           #
#################################

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=110696205)

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-file', '--LINK_COST_FILE', required=True, default=None)  #ENTER AS full file path to path_analyst file
    parser.add_argument('-fixed', '--TIME_DEP_FEE_FIXED_COST', required=True, default=267) #The average fixed monetary cost per veh-year is $3,893.50, equivalent
    #to $2.67/veh-trip assuming average annual mileage, n
    #Later scenarios to include parking and mnpass options--maybe not
    # parser.add_argument('-park_contract', '--PARKING_CONTRACT_FIXED_COST', required=True, default=000) #NA
    # parser.add_argument('-park_daily', '--PARKING_DAILY_FIXED_COST', required=True, default=000) #NA
    # parser.add_argument('-mnpass', '--MNPASS_FIXEDD_COST', required=True, default=000) #NA


    args = parser.parse_args()

    #Read in link cost file
    reader = mod.readInToDict(args.LINK_COST_FILE)
    #Initiate Path Cost writer file
    fieldnames_cost = ['origin', 'deptime', 'destination', 'traveltime','fuel_cost', 'repair_cost', 'depreciation_cost', 'irs_cost',
                  'vot_cost', 'fix_cost', 'A', 'B', 'C', 'D', 'E', 'deptime_sec']
    writer_cost = mod.mkDictOutput('Path_Cost_Matrix_{}'.format(curtime), fieldname_list=fieldnames_cost)

    #Initiate Travel Time writer file
    fieldnames_time = ['origin', 'destination', 'deptime', 'traveltime', 'deptime_sec']
    writer_time = mod.mkDictOutput('Path_TT_Matrix_{}'.format(curtime), fieldname_list=fieldnames_time)


    #Initiate variables
    FIXED = float(args.TIME_DEP_FEE_FIXED_COST)

    count = 0
    sumtime = 0
    sumfuel = 0
    sumrep = 0
    sumdep = 0
    sumirs = 0
    sumvot = 0
    sumfix = 0
    suma = 0
    sumb = 0
    sumc = 0
    sumd = 0
    sume = 0

    #Initiate counting lists
    or_set = []
    dep_set = []
    dest_set = []

    previous = {}
    previous_time = {}

    for row in reader:
        #Due to missing data problems, need to count origins, deptimes, destinations
        if row['origin'] not in or_set:
            or_set.append(row['origin'])
        if row['deptime'] not in dep_set:
            dep_set.append(row['deptime'])
        if row['destination'] not in dest_set:
            dest_set.append(row['destination'])


        current_cost = {'origin': row['origin'], 'deptime': row['deptime'], 'destination': row['destination'], 'traveltime': sumtime,
                   'fuel_cost': sumfuel, 'repair_cost': sumrep, 'depreciation_cost': sumdep, 'irs_cost': sumirs, 'vot_cost': sumvot,
                   'fix_cost': FIXED, 'A': suma, 'B': sumb, 'C': sumc, 'D': sumd, 'E': sume, 'deptime_sec': mod.convert2Sec(row['deptime'])}

        current_time = {'origin': row['origin'], 'deptime': row['deptime'], 'destination': row['destination'], 'traveltime': sumtime, 'deptime_sec': mod.convert2Sec(row['deptime'])}
        #Scenario to handle the first OD pair
        if int(row['path_seq']) == 0 and count == 0:
            sumtime += round(float(row['link_time']), 3)
            sumfuel += round(float(row['fuel_cost']), 3)
            sumrep  += round(float(row['repair_cost']), 3)
            sumdep += round(float(row['depreciation_cost']), 3)
            sumirs += round(float(row['irs_cost']), 3)
            sumvot += round(float(row['vot_cost']), 3)

            previous = current_cost
            previous_time = current_time
        #Row only written when a sequence has been finished
        elif int(row['path_seq']) == 0 and count != 0:
            #At the end of the iteration, add up the sums of each variable for the different scenarios
            suma = sumfuel + sumrep + sumdep + FIXED
            sumb = sumfuel + sumrep + sumdep + FIXED + sumvot
            sumc = sumirs
            sumd = sumfuel + sumrep + sumdep
            sume = sumfuel + sumrep + sumdep + sumvot

            #Update the previous row with the sums that were found for this run through.
            previous['A'] = round(suma, 3)
            previous['B'] = round(sumb, 3)
            previous['C'] = round(sumc, 3)
            previous['D'] = round(sumd, 3)
            previous['E'] = round(sume, 3)

            previous_time['traveltime'] = int(sumtime)

            writer_cost.writerow(previous)
            writer_time.writerow(previous_time)
            #Reset sumlink to zero
            sumtime = 0
            sumtime += round(float(row['link_time']), 3)
            sumfuel = 0
            sumfuel += round(float(row['fuel_cost']), 3)
            sumrep = 0
            sumrep  += round(float(row['repair_cost']), 3)
            sumdep = 0
            sumdep += round(float(row['depreciation_cost']), 3)
            sumirs = 0
            sumirs += round(float(row['irs_cost']), 3)
            sumvot = 0
            sumvot += round(float(row['vot_cost']), 3)
            previous = current_cost
            previous_time = current_time

            #Update previous with the sumlink that was found for this run through.
            previous['traveltime'] = int(sumtime)
            previous['fuel_cost'] = round(sumfuel, 3)
            previous['repair_cost'] = round(sumrep, 3)
            previous['depreciation_cost'] = round(sumdep, 3)
            previous['irs_cost'] = round(sumirs, 3)
            previous['vot_cost'] = round(sumvot, 3)

            previous_time['traveltime'] = int(sumtime)
        #Most times the Else clause will catch
        else:
            sumtime += round(float(row['link_time']), 3)
            sumfuel += round(float(row['fuel_cost']), 3)
            sumrep  += round(float(row['repair_cost']), 3)
            sumdep += round(float(row['depreciation_cost']), 3)
            sumirs += round(float(row['irs_cost']), 3)
            sumvot += round(float(row['vot_cost']), 3)
            previous = current_cost
            previous_time = current_time
            #Update the previous row with the sumlink that was found for this run through.
            previous['traveltime'] = int(sumtime)
            previous['fuel_cost'] = round(sumfuel, 3)
            previous['repair_cost'] = round(sumrep, 3)
            previous['depreciation_cost'] = round(sumdep, 3)
            previous['irs_cost'] = round(sumirs, 3)
            previous['vot_cost'] = round(sumvot, 3)

            previous_time['traveltime'] = int(sumtime)

            bar.next()

        count += 1
    bar.finish()
    print('Final row count=', count)
    print("Number of origins visited:", len(or_set))
    print("Number of deptimes:", len(dep_set))
    print("Number of destinations visited:", len(dest_set))
    mod.elapsedTime(start_time)


