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
import pandas as pd
import datetime

# --------------------------
#       FUNCTIONS
# --------------------------

def main(filename, args):
    if args.SKIP_MATRIX is not None:
        print('Skipping matrix creation')
        M, Origins, Deptimes = loadTTMatrix(args.SKIP_MATRIX)
        matrixReducer(filename, M, Origins, Deptimes)
    else:
        createConfig(filename)
        runMatrixAnalyst(filename)
        M, Origins, Deptimes = loadTTMatrix(filename)
        matrixReducer(filename, M, Origins, Deptimes)
        



#Write TT Matrix config file based on user input and previously generated shapefiles
def createConfig(file_name):
    #print('here2')
    config_tt = {}
    config_tt['firstDepartureDate'] = "2019-02-06"
    config_tt['firstDepartureTime'] = "07:00 AM"
    config_tt['lastDepartureDate'] = "2019-02-06"
    config_tt['lastDepartureTime'] = "07:00 AM"
    config_tt['departureIntervalMins'] = 1
    config_tt['timeZone'] = "America/Chicago"
    config_tt['graphPath'] = "./"
    config_tt['originShapefile'] = '{}'.format(file_name)
    config_tt['originIDField'] = "GEOID10"
    config_tt['destinationShapefile'] = "active_transit_stops.shp"
    config_tt['destinationIDField'] = "site_id"
    config_tt['modes'] = "WALK"
    config_tt['maxTime'] = 1800
    config_tt['outputPath'] = '{}_stops_walk_1800.csv'.format(file_name[:-4])
    config_tt['nThreads'] = 5
    #Runtime for current analyst config: 6 min 16 sec

    with open('{}_tt_config.json'.format(file_name[:-4]), 'w') as outfile:  # writing JSON object
        json.dump(config_tt, outfile)
#        print('here3')
#Execute command using python
def runMatrixAnalyst(file_name):
#    print('here4')
    time1 = datetime.datetime.now()
    myCmd = os.popen('java -jar ..\..\..\Programs\AOBatchAnalyst-0.2.2-all.jar matrix {}_tt_config.json >> matrix_analyst_stdout.txt'.format(file_name[:-4])).read()
    os.system(myCmd)
    time2 = datetime.datetime.now()
    m_runtime = time2 - time1
    print('TT Matrix Runtime: {} {}'.format(m_runtime, file_name), 'is finished')

def loadTTMatrix(file_name):
#    print('here5')

    m = pd.read_csv('{}_stops_walk_1800.csv'.format(file_name[:-4], encoding = "utf-8"))
    #use for testing
    #m = pd.read_csv('{}'.format(file_name, encoding = "utf-8"))
    print('Matrix header:')
    print(m.head())
#    print(m.info())
    origins, deptimes = createLists(m)
#    print('here5.5')
    return m, origins, deptimes
    
def createLists(m):
    # create origins and deptime lists (only do this once! Create a mechanism for only doing this the first time!
    origins = m['origin'].unique().tolist()
    print('origin list:', origins)
    deptimes = m['deptime'].unique().tolist()
    print('deptime list:', deptimes)

    return origins, deptimes

def matrixReducer(file_name, m, origins, deptimes):
    
    #Choose origin
    for i in origins:
        #Choose deptime
        for t in deptimes:
            m_subset = findSubset(m, i, t)
            #Write subset out to file and include headers
            m_subset.to_csv('{}_tt_reduced.csv'.format(file_name[:-4]), header=['k', 'j', 'deptime','traveltime'], index=False, mode = 'a')
            #Remove original TT matrix from disk
            myCmd1 = os.popen('rm {}_stops_walk_1800.csv'.format(file_name[:-4]))
            os.system(myCmd1)
            #Remove TT config file from disk
            myCmd2 = os.popen('rm {}_tt_config.json'.format(file_name[:-4]))
            os.system(myCmd2)
            output = str('{}_tt_reduced.csv'.format(file_name[:-4]))
    #Return the reduced matrix from this operation for use in matrixBinder
    return output
            

def findSubset(m, i, t):
    #Code based on: https://chrisalbon.com/python/data_wrangling/pandas_selecting_rows_on_conditions/
    #create var with TRUE if origin matches i
    origin = m['origin'] == i
    #create var with TRUE if deptime matches t
    deptime = m['deptime'] == t
    #select all rows where origin and deptime match i and t
    subset = m[origin & deptime]
    #Check keep=first or last, how many tt duplicates exists?
    #print('here5.7')

    small_subset = subset.nsmallest(3,'traveltime', keep = 'first')

    return small_subset



#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':
    main(sys.argv[1:])
