#This script links TT matrix data for an intermediate destination, in order to get a singgle travel time
#value from origin to destination

#LOGIC:
#1. Select Destination
#2. Match dest. PNR to origin PNR
#3. Check if origin has been added to destination dictionary list.
#4. If not, add origin 'deptime' to 'traveltime' = 'depsum'
#5. Is dest. 'deptime' within the 'depsumMin - depsumMax range'?
#6. If so, add the origin to the list of origins for the destination.
#7. Add traveltime from O2P & P2D = sumTT
#8. Write output with O, D, deptime from origin, deptime from PNR, sumTT.
#9. 'Seek' to the beginning of O2P dictReader object.
#10. Repeat 1 - 9

#NOTES:

#I need to now compare the TT for all paths and for each OD pair select the lowest TT. Optionally, you can maintain a
#counter of the number of times each park and ride is intermediate point.

#Example usage: kristincarlson$ python TTMatrixLink.py -o2p o2pnr.txt -p2d pnr2d.txt

#################################
#           IMPORTS             #
#################################
import csv
import datetime
import time
import argparse
from createString import makeDict


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
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return currentTime

def mkOutput(currentTime, fieldnames):
    outfile = open('output_{}.txt'.format(curtime), 'w')  # , newline=''
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)  # , quotechar = "'"
    writer.writeheader()
    return writer

def linkTT(temp_dict, dest_id, writer, row_op, row_pd):
        #Create origin ID
        originID = str(row_op['origin']) + '_' + str(row_op['deptime'])
        # check if origin has already been connected to destination
        if originID not in temp_dict[dest_id]:
            temp_dict[dest_id].append(originID)
            # Add origin deptime to TT.
            depsum = convert2Sec(row_op['deptime']) + int(row_op['traveltime'])
            # Create a 5 minute range around arrival time at PnR.
            depsumMin = depsum - 50
            depsumMax = depsum + 50

            # Check if deptime is within depsum range
            if depsumMin <= convert2Sec(row_pd['deptime']) and convert2Sec(row_pd['deptime']) <= depsumMax:
                print('DEPSUM COMARISON', depsumMin, convert2Sec(row_pd['deptime']), depsumMax)

                if convert2Sec(row_op['deptime'])  <= convert2Sec(row_pd['deptime']):
                    print('TIMELINE CHECK', convert2Sec(row_op['deptime']), convert2Sec(row_pd['deptime']))
                    sumTT = int(row_op['traveltime']) + int(row_pd['traveltime'])

                    output_dict['origin'] = row_op['origin']
                    output_dict['destination'] = row_pd['destination']
                    output_dict['deptimeO2P'] = row_op['deptime']
                    output_dict['deptimeP2D'] = row_pd['deptime']
                    output_dict['totaltt'] = sumTT
                    writer.writerow(output_dict)
                    # entry = {'origin': row_op['origin'], 'destination': row_pd['destination'],
                    #          'deptimeO2P': row_op['deptime'], 'deptimeP2D': row_pd['deptime'], 'totaltt': sumTT}
                    #
                    # writer.writerow(entry)


#################################
#           OPERATIONS          #
#################################


if __name__ == '__main__':

    curtime = startTimer()
    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-o2p', '--O2PNR_FILE', required=True, default=None)
    parser.add_argument('-p2d', '--PNR2D_FILE', required=True, default=None)
    args = parser.parse_args()

    fieldnames = ['origin', 'destination', 'deptimeO2P', 'deptimeP2D', 'totaltt']
    writer = mkOutput(curtime, fieldnames)

    P2D_dict = makeDict(args.PNR2D_FILE)

    with open(args.O2PNR_FILE) as f:
        O2P_dict = csv.DictReader(f)
        #Create the temp dictionary where D to O info is stored.
        temp_dict = {}
        #Create the output dict
        output_dict = {}
        for rowPD in P2D_dict:
            #Create unique ID for destination + deptime
            destID = str(rowPD['destination']) + '_' + str(rowPD['deptime'])

            #Create a dictionary that stores destinations: [list of origins]
            temp_dict[destID] = []
            for rowOP in O2P_dict:
                # Do PNRs match?
                if str(rowOP['destination']) == str(rowPD['origin']):
                    linkTT(temp_dict, destID, writer, rowOP, rowPD)

            f.seek(0)
            next(O2P_dict)
