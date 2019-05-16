# Given a set of origins, mid-points, and destinations,
# break origins out in X% chunks, calc TT to mid-points,
# then send the matrix to another program which retains only
# the closest x mid-points. Then the reduced matrix is used in
# the matrix linking process developed for the PNR project.



# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------
import sys
import os
import json
import csv
import pandas as pd
import datetime

# --------------------------
#       FUNCTIONS
# --------------------------

def main(filename):
    createConfigs(filename)
    runMatrixAnalyst(filename)
    loadTTMatrix(filename)



#Write TT Matrix config file based on user input and previously generated shapefiles
def createConfigs(file_name):
    print('here2')
    config_tt = {}
    config_tt['firstDepartureDate'] = "2019-02-06"
    config_tt['firstDepartureTime'] = "07:00 AM"
    config_tt['lastDepartureDate'] = "2019-02-06"
    config_tt['lastDepartureTime'] = "07:01 AM"
    config_tt['departureIntervalMins'] = 1
    config_tt['timeZone'] = "America/Chicago"
    config_tt['graphPath'] = "./"
    config_tt['originShapefile'] = '{}'.format(file_name)
    config_tt['originIDField'] = "GEOID10"
    config_tt['destinationShapefile'] = "active_transit_stops.shp"
    config_tt['destinationIDField'] = "site_id"
    config_tt['modes'] = "WALK"
    config_tt['maxTime'] = 900
    config_tt['outputPath'] = '{}_stops_walk_900.csv'.format(file_name[:-4])
    config_tt['nThreads'] = 5
    #Runtime for current analyst config: 6 min 16 sec

    with open('{}_tt_config.json'.format(file_name[:-4]), 'w') as outfile:  # writing JSON object
        json.dump(config_tt, outfile)
        print('here3')
#Execute command using python
def runMatrixAnalyst(file_name):
    print('here4')
    time1 = datetime.datetime.now()
    myCmd = os.popen('java -jar ..\..\..\Programs\AOBatchAnalyst-0.2.3-all.jar matrix {}_tt_config.json >> matrix_analyst_stdout.txt'.format(file_name[:-4])).read()
    os.system(myCmd)
    time2 = datetime.datetime.now()
    m_runtime = time2 - time1
    print('TT Matrix Runtime: {} {}'.format(m_runtime, file_name), 'is finished')

def loadTTMatrix(file_name):
    print('here5')
    with open('{}_stops_walk_900.csv'.format(file_name[:-4])) as f:
        m = csv.DictReader(f)
        #Need to make createLists much faster, ie don't repeat but use input
        #And need to use pandas or dictionary to load matrix into so that findSubset
        #can grab matching origin_deptimes faster
        origins, deptimes = createLists(m)
        print('here5.5')
        
        #Chose origin
        for i in origins:
            #Choose deptime
            for t in deptimes:
                m_subset = findSubset(m, i, t)
                print('here6')
                #Write subset out to file
                m_subset.to_csv('{}_tt_reduced.csv'.format(file_name[:-4]), header=False, index=False, mode = 'a')        
                        
                        

def findSubset(m, i, t):
    input2DF = {}
    #Iterate through all of input file
    for row in m:
        #Build the df on the selected origin and deptime
        if i == row['origin'] and t == row['deptime']:
            input2DF[row['destination']] = row['traveltime']
            #convert selection to dataframe
            df = pd.DataFrame(input2DF)
            print(df)
            print('here5.7')
    #If there is something in the df
    if df:
        #select the minimum 5 traveltimes from the subset matrix
        df.nsmallest(5, 'traveltime', keep='all')  
    return df


def createLists(m):
    # create origins and deptime lists (only do this once! Create a mechanism for only doing this the first time!
    origins = []
    deptimes = []
    for row in m:
        if row['origin'] not in origins:
            origins.append(row['origin'])
        # create deptime list
        if row['deptime'] not in deptimes:
            deptimes.append(row['deptime'])
    return origins, deptimes




#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':
    main(sys.argv[1:])
