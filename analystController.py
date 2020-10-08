#This is the master script for linking the relevant python scripts together to create individual reduced TT matrices
#for the shared mobility project.

#ToDo
#Convert origin/destination labels to binary values instead of GEOID10 string

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------
import argparse
import os
from progress import bar
import time
from myToolsPackage import matrixLinkModule as mod
import divideShapefile
import ttMatrixReducer
import datetime
# --------------------------
#       FUNCTIONS
# --------------------------
def main():
    #provide arguments and inputs
    args, directory = createArgs()
    print('This is the path to the files in this program:')
    print(directory)
    #To run the entire divideShapefile program, use .main()
    time1 = datetime.datetime.now()
    feats, filenames = divideShapefile.main(args.SHAPE_FILE)
    time2 = datetime.datetime.now()
    print('Time to run divideShapefile: {}'.format(time2-time1))
    myBar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=feats)
    print('Number of origins: {}'.format(feats))
    #run divideShapefiles, remember to write a process to delete all of these files at the end of running this program

    #Looping
    #count = 0
    for i in filenames:
        time3 = datetime.datetime.now()
        #ttMatrixReducer returns the string name of the reduced matrix output
        m_reduced = ttMatrixReducer.main(i, args)
        time4 = datetime.datetime.now()
        print('Time to reduce 1% of matrix: {}'.format(time4-time3))
        matrixBinder(m_reduced)
        myBar.next()
        #count += 1
#        if count == 1:
#            print('Matrix reducer complete')
#            break
    myBar.finish()

def createArgs():
    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)
    #Current working directory
    print(os.getcwd())

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    #C:/Users/krist/Dropbox/AO_Projects/Uber_PNR/toy/tl_2018_wac_rac_2015_origins.shp
    parser.add_argument('-shp', '--SHAPE_FILE', required=True, default=None)  #enter the name of the origin shapefile, give the whole path
    parser.add_argument('-skip', '--SKIP_MATRIX', required=False, default=None)  #pass the tt matrix directly into program here
    # parser.add_argument('-field', '--FIELD', required=True, default=None)  #output field name i.e. jobspdol
    # parser.add_argument('-fname', '--OUTPUT_FILE_NAME', required=True, default=None)  #i.e. transit16_cost_access
    args = parser.parse_args()

    full_path = os.path.realpath(args.SHAPE_FILE)
    path_tuple = os.path.split(os.path.abspath(full_path))

    return args, path_tuple[0]


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':
    main()
