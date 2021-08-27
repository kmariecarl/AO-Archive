# -*- coding: utf-8 -*-
"""
Created on Wed May 22 18:19:44 2019

@author: kristin
"""
#Had a dream that I named the pieces of this software, PB&J
#sudo code:
#find the unique set of stops that the orign set was able to reach
#Compute the travel time matrix for those stops to the destination set
#Essentially duplicate most of the functionality of the ttMatrixREeducer
#Load K2J matrix to db (work on loading I2K matrix to db as well)
#Never recompute a tt matrix for a k2d pair, always check what is in the db first
#Need to export a shapefile with the unique k points for use in k2j tt matrix


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
def main(file_name):
    m = loadK2JMatrix(file_name)
    selectUnique(m)
    createConfig(file_name)
    
#Perhaps this should pull from the db?    
def loadK2JMatrix(file_name):
     m = pd.read_csv('{}_stops_walk_1800.csv'.format(file_name[:-4], encoding = "utf-8"))
     return m
 
#Return the set of stops that were reached by the origins, ensure no duplicate calculations
def selectUnique(matrix):
    k_unique = m['k'].unique().tolist()
    print('k list:', k_unique)
    return k_unique
 
#Write TT Matrix config file for K2J analysis. FIX THIS
def createConfig(file_name):
    #print('here2')
    config_tt = {}
    config_tt['firstDepartureDate'] = "2019-02-06"
    config_tt['firstDepartureTime'] = "07:00 AM"
    config_tt['lastDepartureDate'] = "2019-02-06"
    config_tt['lastDepartureTime'] = "07:14 AM"
    config_tt['departureIntervalMins'] = 1
    config_tt['timeZone'] = "America/Chicago"
    config_tt['graphPath'] = "./"
    config_tt['originShapefile'] = '{}'.format(file_name)
    config_tt['originIDField'] = "site_id"
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

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':
    main(sys.argv)
