#This script finds the total cost of a series of links between an OD pair based on the linkCostCalculator.py output

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
    args = parser.parse_args()

    #Read in link cost file
    reader = mod.readInToDict(args.LINK_COST_FILE)
    #Initiate writer file
    fieldnames = ['origin', 'deptime', 'destination', 'path_cost']
    writer = mod.mkDictOutput('Path_Cost_Matrix_{}'.format(curtime), fieldname_list=fieldnames)
    #Initiate variables
    count = 0
    sumlink = 0
    previous = {}
    for row in reader:
        current = {'origin': row['origin'], 'deptime': row['deptime'], 'destination': row['destination'], 'path_cost': sumlink}
        #Scenario to handle the first OD pair
        if int(row['path_seq']) == 0 and count == 0:
            sumlink += float(row['link_cost'])
            previous = current
        #Row only written when a sequence has been finished
        elif int(row['path_seq']) == 0 and count != 0:
            writer.writerow(previous)
            #Reset sumlink to zero
            sumlink = 0
            sumlink += float(row['link_cost'])
            previous = current
            #Update previous with the sumlink that was found for this run through.
            previous['path_cost'] = sumlink
        #Most times the Else clause will catch
        else:
            sumlink += float(row['link_cost'])
            previous = current
            #Update the previous row with the sumlink that was found for this run through.
            previous['path_cost'] = sumlink
        count += 1


