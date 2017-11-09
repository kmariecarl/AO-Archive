# File: stopTimesEditor2.py
# Date Created: 2017-03-05
# Date Modified:
# Created By: Kristin M. Carlson
#
# Description:
# This Python script updates a sequence of
# stop_times for a given route.
#
# To Do:
# No longer an issue: Program currently assumes each trip only has one link to be manipulated but I-35W has on-line stops which make back-to-back
# managed lane links. Need to retain updatedTime value and update it further after the second link.

# Remove reliance on route_id ending (i.e. 852 'dash' 98) The dash has to do with the GTFS release date and
# can make the input.txt file non-transferable between test cases if it it not changed.


# Assumption1: If the stop_times time is printed beyond 24:00:00 the editor will not change the output row. This is why a time
# window is specified.

# ASSUMPTION2: If adjacent stop times are the exact time as stopPair, the difference = 0. Instead estimate to
# be the override speed given in the input file.

# Assumption3: If a stop sequence does not have a lead/lag stop, apply override speed.

# Assumption4: If a stop sequence has a lead/lag stop different from what is in the input file 9but shares the same stopB
# and stopE pair, apply override.

# Assumption5: The user must make the input file using the stop sequence that best represents the majority of trips for
# a particular route. When trips going the same direction have the same stopB and stopE but different stopA and stopF, the
# lead/lag link that best characterizes the access/egress links must be recorded - as this is what the editor will estimate
# link speeds from.

# Must pay attention to shapeID field to make sure all trip types are accounted for and changed within the editor
# It turns out that shape_ids may change for a particular trip due to expected run time changes (esp during peak hour).
# Example: Route 852-95 has 12 shape_ids, the sequence of stops between all are the same, but runtime along the I-94
# freeway portion varies between 10-14 minutes depending on the time in which the trip runs.

# If iterative use of the time window is applied for the config file, segments could be tested for different times which could be useful.
# Implement catch in case the input file does not match an updated GTFS version so that some speed estimate is applied and the user is notified.

#Example Usage: kristincarlson~ python stoptimeseditor8.py -bsgtfs gtfs_mt_09_06_16 -input input_852_156.csv -config config.csv


####################################
## Libraries and global variables ##
####################################

import csv
import datetime
import os
import time
import shutil
import zipfile
import argparse


####################################
##         Working Code           ##
####################################

# Convert time to seconds
def get_sec(time_str):
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)


# This is the input list that will be filled to limit accessing disk space.
# Consider changing this to a list of dictionaries
def makeInputList():
    # Read in the 'input.txt' file.
    with open(os.path.join(args.INPUTFILE)) as inputfile:
        input_file = csv.DictReader(inputfile)
        input_list = [i for i in input_file]
        print('Input List Created')
        return input_list


# open trips file and create dictionary with trip_ids as keys and route_ids as values.
def makeTripsDict():
    # Read in the 'trips.txt' file.
    with open(os.path.join(OUT_GTFS_FLDR, 'trips.txt')) as trips_file:
        trips = csv.DictReader(trips_file)
        tripsDict = dict()
        for line in trips:
            if line['trip_id'] in tripsDict:
                # trip_id should only be associated with one route_id, so raise exception.
                raise ValueError('Trip_id: {} is associated with more than one route.'.format(str(line['trip_id'])))
            else:
                # create a new list in this slot
                tripsDict[line['trip_id']] = line['route_id']
        # print(tripsDict)
        print('Trips Dictionary Created')
        return tripsDict


def getScenarioConfig():
    with open(os.path.join(args.CONFIGFILE)) as configFile:
        config_file = csv.DictReader(configFile)
        # List comprehension. Each i is a dictionary which is exactly the form needed to iterate through later.
        config_list = [i for i in config_file]
        count = 0
        for i in config_list:
            if count < 1:
                timestart, timeend = i['time_start'], i['time_end']
                count = + 1
        print("Configuration Retrieved. Time Start: ", timestart, "Time End: ", timeend)
        return config_list, timestart, timeend


# Creates a dictionary of possibly important stopIDs + tripIDs.
def makeInterestingStopsDict(input_list):
    with open(os.path.join(OUT_GTFS_FLDR, 'stop_times.txt')) as stop_times_file:
        stopsDictCopy = csv.DictReader(stop_times_file)
        # Initialize variables and lists
        interest_stop_dict = {}
        for row in stopsDictCopy:
            for line in input_list:
                if row['stop_id'] == line['stopA'] or line['stopB'] or line['stopE'] or line['stopF']:
                    key = row['trip_id'] + row['stop_id']
                    interest_stop_dict[key] = row['arrival_time'], row['departure_time']
        print('Interesting Stops Dictionary Created')
        return interest_stop_dict

# Check if the time is interesting by comparing against user specified time range. Otherwise revert to defaults (24-hours)
def isTimeInteresting(current_row, time_range_start=0, time_range_end=86400):
    if int(time_range_start) <= get_sec(current_row['arrival_time']) <= int(time_range_end):
        return True
    else:
        # print('WARNING: Trip: {} runs beyond time window. No change will be made to this row'.format(str(current_row['trip_id'])))
        return False

# Check if the current row trip matches a key in the trips_dict.
def isTripInteresting(current_row, trips_dict, input_list):
    try:
        # Extract the route_id that matches the trip_id from the current row
        route_id = trips_dict[current_row['trip_id']]
        # print(route_id)
    except KeyError:
        print('WARNING: No route information for trip: {} -- skipping'.format(str(current_row['trip_id'])))
        # Explicitly return "False"
        return False

    for row in input_list:
        # Check if the route_id is listed as having a key link in the input file.
        if route_id == str(row['route_id']):
            # print('Trip: {} matches with one of the input file routes'.format(current_row['trip_id']))
            # If the route_id that was found from above has a match within the input_file, then return "True."
            return True #current_row['trip_id']
            # If there is no match between the route_id and the entire input_file, then return "False."
    return False

# Provide a stop pair from the stop_times file and it will be compared against all of the pairs from the input list.
def findInterestingStopPair(current_row, previous_row, trips_dict, input_list, config_list, log_file):
    stopB = int(previous_row['stop_id'])
    stopE = int(current_row['stop_id'])
    current_routeID = str(trips_dict[current_row['trip_id']])
    # Does the stopPair have a segmentId that matches one of the values in the segmentIDList?
    for line in input_list:
        inputStopB = int(line['stopB'])
        inputStopE = int(line['stopE'])
        #Match the current + previous stop pair with a matching input stop sequence and confirm that the route_IDs match
        if stopB == inputStopB and stopE == inputStopE and current_routeID == str(line['route_id']):
            # print("Found an interesting stop pair!")
            for row in config_list:
                #Now make sure that interestingStopPair is noted for change in the config file.
                if int(line['segment_id']) == int(row['segment_id']):
                    #Collect the correct routeID, tripID
                    log1 = 'RouteID: {}\n'.format(line['route_id'])
                    log2 = 'TripID:  {}\n'.format(current_row['trip_id'])
                    log3 = 'SegmentID: {}\n'.format(line['segment_id'])
                    log_file.writelines(log1)
                    log_file.writelines(log2)
                    log_file.writelines(log3)
                    # Return routeID, tripID, segmentID, direction, link_type, target_speed, boolean
                    return str(line['route_id']), str(current_row['trip_id']), int(line['segment_id']),\
                           str(line['dir']), str(row['link_type']), float(row['target_speed']), True
    else:
        # print("Trip_id: {} does not contain interesting stop pair".format(current_row['trip_id']))
        return None, None, None, None, None, None, False

#Calculate the access/egress link travel times, apply override link speed for estimating TT if need be.
def linkTT(trip_id, segment_id, input_list, interest_stops_dict, previous_row, current_row, log_file):
    #If the input file has a zero for stopA or stopF, apply override
    #If adjacent stop_times for A&B or E&F are identical, apply override
    #If the concatenation of the tripID and stopA or stopF cannot be found in the trips_dict because a different lead/lag
    #link was recorded for the route, then apply override speed associated with the match stopB-stopE combo.
    #Currently no way to retain estimated access/egress link speeds from one iteration and apply it to another. But I could
    #implement it!
    #Else estimate link speeds (but apply override if est. speeds are too low).
    for line in input_list:
        # Quickly find the segment ID in question
        if segment_id == int(line['segment_id']):
            concatA = str(trip_id) + str(line['stopA'])
            concatF = str(trip_id) + str(line['stopF'])

            #This addresses Assumption5.
            if concatA not in interest_stops_dict or concatF not in interest_stops_dict:
                #print('here0')
                timeB_C = overRide(float(line['distB_C']), float(line['spdB_C']), segment_id, input_list, log_file)
                timeD_E = overRide(float(line['distD_E']), float(line['spdD_E']), segment_id, input_list, log_file)
                return timeB_C, timeD_E

            minilist = interest_stops_dict[concatA]
            minilist2 = interest_stops_dict[concatF]

            #print('here1')
            #No info in input file for stopA or stopF, apply override spd to access link.
            if int(line['stopA']) == 0 or int(line['stopF']) == 0:
                #print('here2')
                timeB_C = overRide(float(line['distB_C']), float(line['spdB_C']), segment_id, input_list, log_file)
                timeD_E = overRide(float(line['distD_E']), float(line['spdD_E']), segment_id, input_list, log_file)

            #If the lead/lag stop has identical stop_time as stopB or stopE stop_times, apply override speed.
            elif minilist[1] == previous_row['arrival_time'] or minilist2[0] == current_row['departure_time'] :#stopA tiem is same as stop B arrival:
                #print('here3')
                timeB_C = overRide(float(line['distB_C']), float(line['spdB_C']), segment_id, input_list, log_file)
                timeD_E = overRide(float(line['distD_E']), float(line['spdD_E']), segment_id, input_list, log_file)

            #If no override is needed, use the estimateLinkSpeeds function.
            else:
                #print('here4')
                timeB_C, timeD_E = estimateLinkSpeeds(trip_id, segment_id,input_list,previous_row,current_row,interest_stops_dict,log_file)

            return timeB_C, timeD_E

#Calculated the Travel Time along access/egress links when the override speed is determined applicable from checkSpeeds fcn.
def overRide(dist, spd, segment_id, input_list, log_file):
    for line in input_list:
        if segment_id == int(line['segment_id']):
            #print('here5')
            override = 'YES'
            time = dist /spd
            logtext = "Estimated access/egress TT : ", str(round(time, 2)), " sec, ", "Using speed : ",\
                      str(spd), " m/sec : ", ' Override: ', override, '\n'
            log.writelines(logtext)
            return  time

#Estimate the access/egress link TT, but apply override if estimated speed is slower than the override speed.
def estimateLinkSpeeds(trip_id, segment_id, input_list, previous_row, current_row, interest_stops_dict, log_file):
    for line in input_list:
        if segment_id == int(line['segment_id']):
            #print('here6')
            concatA = str(trip_id) + str(line['stopA'])
            concatF = str(trip_id) + str(line['stopF'])

            minilist = interest_stops_dict[concatA]
            minilist2 = interest_stops_dict[concatF]

            diff = get_sec(minilist[1]) - get_sec(previous_row['arrival_time']) #prev_arrival_time)
            diff2 = get_sec(minilist2[0]) - get_sec(current_row['departure_time']) #curr_departure_time)

            speedA_B = (int(line['distA_B']) / diff)
            speedE_F = (int(line['distE_F']) / diff2)

            if speedA_B < float(line['spdB_C']) or speedE_F < float(line['spdD_E']):
                #print('here7')
                timeB_C = overRide(int(line['distB_C']), float(line['spdB_C']), segment_id, input_list, log_file)
                timeD_E = overRide(int(line['distD_E']), float(line['spdD_E']), segment_id, input_list, log_file)
            else:
                override = 'NO'
                #print('here8')
                # DistB_C / speedA_B(m/sec): this give the time (sec) needed to make segment B_C operate at the same
                # speed as the segment before it.
                timeB_C = int(line['distB_C']) / speedA_B
                timeD_E = int(line['distD_E']) / speedE_F
                log1 = "Access Speed A to B (m/sec) : ", str(round(speedA_B, 2)), ' Override: ', override, '\n'
                log2 = "Egress Speed E to F (m/sec): ", str(round(speedE_F, 2)), 'Override: ', override, '\n'
                log_file.writelines(log1)
                log_file.writelines(log2)

            return timeB_C, timeD_E

# A function that finds the appropriate time change based on the link_function and returns an integer.
# Does not actually apply the time change.
# Currently there is no difference between the BoS and Managed functions.
def timeDelta(input_list, segment_id, direc, link_type, current_row, previous_row, timeB2C, timeD2E, targetSpeed, log_file):
    # Grab the current row arrival time and previous row departure time (this is the stretch of time to change)
    current = get_sec(current_row['arrival_time'])
    previous = get_sec(previous_row['departure_time'])
    # Iterate through input list to find the correct direction of the desired trip to be changed.
    for line in input_list:
        if segment_id == int(line['segment_id']):  #if str(line['dir']) == direc:
            # Calculate GTFS speed on this link (B to E). Units: m/sec
            # Add distances B_C + C_D + D_E and divide by schedule time.
            speedB2E = (int(line['distB_C']) + int(line['distC_D']) + int(line['distD_E'])) / (current - previous)
            log1 = "GTFS speed (m/sec) for link BE: ", str(
                speedB2E), '\n'  # Convert m/sec to mph * (3600 / 5280) * (3.2))
            log_file.writelines(log1)
            # Calculate GTFS speed on only the managed portion. Units: m/sec
            speedC2D = int(line['distC_D']) / (current - previous - timeB2C - timeD2E)
            log2 = "GTFS speed (m/sec) for managed lane portion: ", str(
                speedC2D), '\n'  # * (3600 / 5280) * (3.2)), '\n'
            log_file.writelines(log2)
            log_file.writelines('Target Speed (m/sec): {}\n'.format(targetSpeed))
            # temp_change_time will be negative if the link needs to speed up to match target_speed. Otherwise positive.
            # Find the change in seconds between target speed and GTFS speed.
            temp_chng_time = ((1 / targetSpeed) * float(line['distC_D']))
            temp_chng_time2 = ((1 / speedC2D) * float(line['distC_D']))
            chng_time = round(float(temp_chng_time - temp_chng_time2), 2)
            log3 = ("Change in seconds needed to get target speed on managed lane portion: ", str(chng_time),
                    " Direction: ",
                    direc, '\n')
            log_file.writelines(log3)
            log_file.writelines('\n')
            return chng_time


# Update every arrival and departure time field and save to new csv file as string
def updateTimes(current_row, time_change):
    # Calculate time change in seconds.
    temp_update_arrival = get_sec(current_row['arrival_time']) + time_change
    # Convert temp_update_arrival from seconds to string time stamp
    update_arrival = str(time.strftime("%H:%M:%S", time.gmtime(temp_update_arrival)))
    # Now update departure
    temp_update_departure = get_sec(current_row['departure_time']) + time_change
    update_departure = str(time.strftime("%H:%M:%S", time.gmtime(temp_update_departure)))
    # This block catches any rows that exceed the 24:00:00 time. A time window is given to prevent any changes happening to
    # midnight hours (not of interest).
    if get_sec(current_row['arrival_time']) >= 86400:  # 24:00:00
        # Do not apply time_change mechanism. Print out existing row. time format does not exist for processing times above 24:00:00.
        entry = {'trip_id': current_row['trip_id'], 'arrival_time': current_row['arrival_time'],
                 'departure_time': current_row['departure_time'],
                 'stop_id': current_row['stop_id'], 'stop_sequence': current_row['stop_sequence'],
                 'pickup_type': current_row['pickup_type'],
                 'drop_off_type': current_row['drop_off_type']}
        # print("NOTE: No change made. Time runs past 24 hours")
        return entry
    else:
        # update the current row with the adjusted arrival and departure times
        entry = {'trip_id': current_row['trip_id'], 'arrival_time': str(update_arrival),
                 'departure_time': str(update_departure),
                 'stop_id': current_row['stop_id'], 'stop_sequence': current_row['stop_sequence'],
                 'pickup_type': current_row['pickup_type'],
                 'drop_off_type': current_row['drop_off_type']}
        return entry


# This function summarizes the trips and stops changed, the total time savings, and the time savings on a per route basis.
def tripStats(changed_rows, trip_tm_chg_dict, trips_dict, log_file):
    # Calc total runtime savings for scenario.
    total = 0
    for item in trip_tm_chg_dict:
        total += trip_tm_chg_dict[item]
    scenarioDelta = 'Total change in run time for this scenario (hours): ', str(total / 3600), '\n'
    log_file.writelines(scenarioDelta)
    log_file.writelines(" ")

    #Prompt user for a time window.
    time_start = int(input('Summary stats time window start (sec)'))
    time_end = int(input('Summary stats time window end (sec)'))

    # Create a list of routes.
    route_list = []
    # Each element is a tripID
    for element in trip_tm_chg_dict:
        # Match the tripID to the routeID
        if trips_dict[element] not in route_list:
            route_list.append(trips_dict[element])

    # Routines for calculating summary statistics.
    #Create dictionary that maps trips wihtin the time window to the time changed for that trip.
    wndwDict = dict()
    for key in changed_rows:
        # Trip_id: {} from stop_id {} to {} changed by x min
        line = (key, ' changed between stop {} and {} by {} seconds'.format(changed_rows[key][0], changed_rows[key][1],
                                                                            changed_rows[key][2]), '\n')
        log_file.writelines(line)
        #Perform summary stats calcs based on time window provided by user.
        if time_start <= get_sec(changed_rows[key][3]) or get_sec(changed_rows[key][4]) <= time_end:
            wndwDict[key] = trip_tm_chg_dict[key]
    return route_list, wndwDict, time_start, time_end

def routeStats(route, wndw_dict, time_start, time_end, trips_dict, log_file):
    wndw_total = 0
    route_total = 0
    counter = 0
    for trip in wndw_dict:
        #Sum up the savings for all trips that fall within the time window
        wndw_total += wndw_dict[trip]
        if trips_dict[trip] == route:
            #Sum up the time savings per route
            route_total += wndw_dict[trip]
            #The counter is the divisor for finding the average time savings per trip
            counter += 1
    return round(wndw_total, 3), round(route_total, 3), counter


if __name__ == "__main__":
    # Start timing
    start_time = time.time()
    # Make a variable for the current time for use in writing files.
    currentTime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    tripID = None
    currRow = None
    prevRow = 0
    changedRows = dict()
    # Parameterize file paths: use argparse so that when stopTimesEditor is run, the user adds additional arguments for the
    # necesarry file paths.
    parser = argparse.ArgumentParser()
    parser.add_argument('-bsgtfs','--BASE_GTFS_FLDR', required=True, default=None)
    #parser.add_argument('-copygtfs', '--out_gtfs_fldr', required=True, default=None)
    #parser.add_argument('zip', '--out_gtfs_zip', required=True, default=None)
    parser.add_argument('-input', '--INPUTFILE', required=True, default=None)
    parser.add_argument('-config', '--CONFIGFILE', required=True, default=None)
    args = parser.parse_args()

    #BASE_GTFS_FLDR = args.BASE_GTFS_FLDR #'gtfs'
    OUT_GTFS_FLDR = 'gtfs_copy'
    OUT_GTFS_ZIP = 'gtfs.zip'
    #INPUTFILE = 'input.txt'
    #CONFIGFILE = 'config.txt'
    TEMP_STOP_TIMES = 'temp_stop_times.txt'
    # Default time change for every row if link does not contain managed lane portion
    timeChange = float(0.0)
    # Copy gtfs folder to grab and edit files from.
    shutil.copytree(args.BASE_GTFS_FLDR, os.path.join(OUT_GTFS_FLDR))
    # Make local Data frames from files on disk
    inputList = makeInputList()
    tripsDict = makeTripsDict()
    configList, timeRangeStart, timeRangeEnd = getScenarioConfig()
    interestStopsDict = makeInterestingStopsDict(inputList)
    tripTmChgDict = dict()

    # Open log file to write diagnostics out to.
    log = open('log_{}.txt'.format(currentTime), 'w')
    # Create the new gtfs.zip
    with zipfile.ZipFile(OUT_GTFS_ZIP, mode='w', compression=zipfile.ZIP_DEFLATED) as new_zip:
        # Copy .txt files from original gtfs zip bundle to new zip bundle
        namelist = ['agency.txt', 'calendar.txt', 'calendar_dates.txt', 'routes.txt', 'shapes.txt', 'stop_times.txt',
                    'stops.txt', 'trips.txt']
        for name in namelist:
            if name == 'stop_times.txt':
                print("Editing stop_times file")
                # Write new stop_times file
                with open(os.path.join(OUT_GTFS_FLDR, name), 'r') as stop_times_file:
                    stopsDict = csv.DictReader(stop_times_file)
                    with open(TEMP_STOP_TIMES, 'wt') as new_stop_times:
                        fieldnames = ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence',
                                      'pickup_type', 'drop_off_type']
                        new_stopTimes = csv.DictWriter(new_stop_times, fieldnames, delimiter=',')
                        # Make sure to write the header field names to new csv!
                        new_stopTimes.writeheader()
                        # Read in 'stop_times.txt' file to dictionary structure.
                        # with open(os.path.join('..', 'Data', 'gtfs_09_06_16','stop_times.txt')) as stop_times_file:
                        stopsDict = csv.DictReader(stop_times_file)
                        count = 0
                        # Iterate through stop_times.txt file
                        for row in stopsDict:
                            currRow = row
                            if isTimeInteresting(currRow, timeRangeStart, timeRangeEnd) is True:
                                # If there is an interesting trip, its trip_id value is assigned.
                                checkTrip = isTripInteresting(currRow, tripsDict, inputList)
                                if checkTrip is not False and prevRow != 0:
                                    # Assign the link type and percent of link to new variables.
                                    # Also grab stop sequence of prev and curr rows.
                                    routeID, tripID, segmentID, direc, linkType, targetSpeed, \
                                    foundStop = findInterestingStopPair(currRow,prevRow, tripsDict,inputList,configList, log)
                                    if foundStop is True:
                                        # Make sure the key stop pair is matched with the right route and segmentID in
                                        # the input file.
                                        timeB_C, timeD_E, = linkTT(tripID, segmentID, inputList, interestStopsDict,
                                                                                 prevRow, currRow, log)
                                        timeChange = timeDelta(inputList,segmentID, direc, linkType, currRow, prevRow, timeB_C,
                                                               timeD_E, targetSpeed, log)
                                        # Save key link info to dictionary to print back to user at the end of program
                                        changedRows[currRow['trip_id']] = [prevRow['stop_id'], currRow['stop_id'],
                                                                           timeChange, prevRow['arrival_time'],
                                                                           currRow['departure_time']]
                                        # Save info to dict for summary diagnostics
                                        tripTmChgDict[currRow['trip_id']] = timeChange
                                        # Print a space between output
                                        log.write("")
                            if count == 0:
                                new_stopTimes.writerow(updateTimes(currRow, timeChange))
                            elif int(currRow['stop_sequence']) > int(prevRow['stop_sequence']):
                                new_stopTimes.writerow(updateTimes(currRow, timeChange))
                            else:
                                timeChange = float(0.0)
                                new_stopTimes.writerow(updateTimes(currRow, timeChange))
                            # Update the previous row before moving to top of routine.
                            prevRow = currRow
                            count += 1
                # Add the new stop times file to the new gtfs zip bundle.
                new_zip.write(TEMP_STOP_TIMES, 'stop_times.txt')
            else:
                with open(os.path.join(OUT_GTFS_FLDR, name), 'r') as myfile:
                    reader = csv.reader(myfile, delimiter=',')
                    # Make a new file called "temp" and write to it
                    with open('temp.txt', 'w') as outfile:
                        writer = csv.writer(outfile)
                        # Iterate through the reader and assign to writer file.
                        for row in reader:
                            # The brackets around row make sure that each row is read together, not each character.
                            writer.writerows([row])
                new_zip.write('temp.txt', name)
        # Remove the folders and files created in the process of execution.
        shutil.rmtree(OUT_GTFS_FLDR)
        os.remove('temp.txt')
        os.remove(TEMP_STOP_TIMES)

        # At the end of the program, print the trip ID, stops changed, and the change in seconds.
        routeList, windowDict, timeStart, timeEnd = tripStats(changedRows, tripTmChgDict, tripsDict, log)
        # Calc total runtime savings per route.
        for route in routeList:
            log.writelines(" ")
            wndwTotal, routeTotal, counter = routeStats(route, windowDict, timeStart, timeEnd, tripsDict, log)
            routeDelta = 'Total change in run time for route {} (hours): '.format(route), str(routeTotal / 3600), '\n'
            log.writelines(routeDelta)
            # Calc avg runtime savings per trip (min)
            trip_avg = round((routeTotal/counter)/60, 3)
            tripAvgText = 'From {} to {} for route {}, the avg time change (min) per trip was: ' .format(timeStart,
                                                                                timeEnd, route), str(trip_avg), '\n'
            log.writelines(tripAvgText)
            counterText = 'From {} to {} for route {}, the number of trips changed was: ' .format(timeStart,
                                                                                timeEnd, route), str(counter), '\n'
            log.writelines(counterText)
        #Outside of For loop to eliminate printing twice to log file.
        text = 'Total change in run time between {} and {} Oclock: ' .format(timeStart, timeEnd), str(wndwTotal), '\n'
        log.writelines(text)

    print("Runtime:--- %s seconds ---" % (time.time() - start_time))
    time_diff = (time.time() - start_time)
    timestamp = "Runtime:--- %s seconds ---\n" % str(time_diff)
    log.write(" ")
    log.write(timestamp)
