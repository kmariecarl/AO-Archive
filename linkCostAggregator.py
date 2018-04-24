#This script finds the total cost of a series of links between an OD pair based on the linkCostCalculator.py output

#Need to add fixed costs and trip based parking costs and maybe mnpass costs to the path output, because these fixed costs
#are trip based!!

#Need to implement scenario columns

#This code looks ugly but it works very well.

#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse

#################################
#           FUNCTIONS           #
#################################

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-file', '--LINK_COST_FILE', required=True, default=None)  #ENTER AS full file path to path_analyst file
    parser.add_argument('-fixed', '--TIME_DEP_FEE_FIXED_COST', required=True, default=267) #The average fixed monetary cost per veh-year is $3,893.50, equivalent
    #to $2.67/veh-trip assuming average annual mileage, n
    #Later scenarios to include parking and mnpass options
    # parser.add_argument('-park_contract', '--PARKING_CONTRACT_FIXED_COST', required=True, default=000) #NA
    # parser.add_argument('-park_daily', '--PARKING_DAILY_FIXED_COST', required=True, default=000) #NA
    # parser.add_argument('-mnpass', '--MNPASS_FIXEDD_COST', required=True, default=000) #NA


    args = parser.parse_args()

    #Read in link cost file
    reader = mod.readInToDict(args.LINK_COST_FILE)
    #Initiate writer file
    fieldnames = ['origin', 'deptime', 'destination', 'fuel_cost', 'repair_cost', 'depreciation_cost', 'irs_cost',
                  'vot_cost', 'fix_cost', 'A', 'B', 'C', 'D', 'E', 'F', 'G']
    writer = mod.mkDictOutput('Path_Cost_Matrix_{}'.format(curtime), fieldname_list=fieldnames)
    #Initiate variables
    FIXED = float(args.TIME_DEP_FEE_FIXED_COST)

    count = 0
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
    sumf = 0
    sumg = 0

    previous = {}

    for row in reader:
        current = {'origin': row['origin'], 'deptime': row['deptime'], 'destination': row['destination'],
                   'fuel_cost': sumfuel, 'repair_cost': sumrep, 'depreciation_cost': sumdep, 'irs_cost': sumirs, 'vot_cost': sumvot,
                   'fix_cost': FIXED, 'A': suma, 'B': sumb, 'C': sumc, 'D': sumd, 'E': sume, 'F': sumf, 'G': sumg}
        #Scenario to handle the first OD pair
        if int(row['path_seq']) == 0 and count == 0:
            sumfuel += float(row['fuel_cost'])
            sumrep  += float(row['repair_cost'])
            sumdep += float(row['depreciation_cost'])
            sumirs += float(row['irs_cost'])
            sumvot += float(row['vot_cost'])

            previous = current
        #Row only written when a sequence has been finished
        elif int(row['path_seq']) == 0 and count != 0:
            #At the end of the iteration, add up the sums of each variable for the different scenarios
            suma = sumfuel + sumrep
            sumb = sumfuel + sumdep
            sumc = sumfuel + sumrep + sumdep
            sumd = sumfuel + sumrep + sumdep +sumvot
            sume = sumfuel + sumrep + sumvot
            sumf = sumfuel + sumdep + sumvot
            sumg = sumfuel + FIXED
            #Update the previous row with the sums that were found for this run through.
            previous['A'] = suma
            previous['B'] = sumb
            previous['C'] = sumc
            previous['D'] = sumd
            previous['E'] = sume
            previous['F'] = sumf
            previous['G'] = sumg

            writer.writerow(previous)
            #Reset sumlink to zero
            sumfuel = 0
            sumfuel += float(row['fuel_cost'])
            sumrep = 0
            sumrep  += float(row['repair_cost'])
            sumdep = 0
            sumdep += float(row['depreciation_cost'])
            sumirs = 0
            sumirs += float(row['irs_cost'])
            sumvot = 0
            sumvot += float(row['vot_cost'])
            previous = current
            #Update previous with the sumlink that was found for this run through.
            previous['fuel_cost'] = sumfuel
            previous['repair_cost'] = sumrep
            previous['depreciation_cost'] = sumdep
            previous['irs_cost'] = sumirs
            previous['vot_cost'] = sumvot
        #Most times the Else clause will catch
        else:
            sumfuel += float(row['fuel_cost'])
            sumrep  += float(row['repair_cost'])
            sumdep += float(row['depreciation_cost'])
            sumirs += float(row['irs_cost'])
            sumvot += float(row['vot_cost'])
            previous = current
            #Update the previous row with the sumlink that was found for this run through.
            previous['fuel_cost'] = sumfuel
            previous['repair_cost'] = sumrep
            previous['depreciation_cost'] = sumdep
            previous['irs_cost'] = sumirs
            previous['vot_cost'] = sumvot
        count += 1


