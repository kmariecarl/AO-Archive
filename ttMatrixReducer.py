# Given a set of origins, mid-points, and destinations,
# break origins out in X% chunks, calc TT to mid-points,
# then send the matrix to another program which retains only
# the closest x mid-points. Then the reduced matrix is used in
# the matrix linking process developed for the PNR project.

import shapely
import fiona
from pathlib2 import Path
import json
import argparse
from progress import bar
from myToolsPackage import matrixLinkModule as mod

# Read in origins shapefile
input = fiona.open("C:/Users/krist/Dropbox/AO_Projects/Uber_PNR/toy/tl_2018_wac_rac_2015_origins.shp")
print('Shape Schema:', input.schema)  # (GeoJSON format)

feature_counter = 0
for feat in input:
    feature_counter += 1
print('Number of features:', feature_counter)
div = int(feature_counter * 0.1)
print('Number of features in each subset shapefile:', div)
input.close()
    # Create variable for file name:
filename = Path("C:/Users/krist/Dropbox/AO_Projects/Uber_PNR/toy/tl_2018_wac_rac_2015_origins.shp")
print(filename.name)
main_name = filename.name
# Create unique output shapefile names
file_count = int(feature_counter / div)
print('Number of subset files to create:', file_count)
file_names = []
for i in range(1, file_count, 1):
    file_names.append('{}_{}.shp'.format(main_name, i))
print('List of file names:', file_names)

with fiona.open("C:/Users/krist/Dropbox/AO_Projects/Uber_PNR/toy/tl_2018_wac_rac_2015_origins.shp") as input2:
    # Create a sink for processed features with the same format and
    # coordinate reference system as the source.
    # Copy the source schema to new schema called sink
    sink_schema = input2.schema
    for i in file_names:
        with fiona.open('C:/Users/krist/Dropbox/AO_Projects/Uber_PNR/toy/{}'.format(i), 'w', crs=input2.crs, driver=input2.driver, schema=sink_schema, ) as sink:
            counter = 0
            for row in input2:
                counter += 1
                if counter <= div:
                    sink.write(row)
                else:
                    print('File {} written'.format(i))
                    break

#Now write a series of config files based on user input and previously generate shapefiles


config_tt = {
  "firstDepartureDate": "2019-02-06",
  "firstDepartureTime": "07:00 AM",
  "lastDepartureDate": "2019-02-06",
  "lastDepartureTime": "07:59 AM",
  "departureIntervalMins": 1,
  "timeZone": "America/Chicago",
  "graphPath": "./",
  "originShapefile": "tl_2018_wac_rac_2015_origins_1pct.shp",
  "originIDField": "GEOID10",
  "destinationShapefile": "active_transit_stops.shp",
  "destinationIDField": "site_id",
  "modes": "WALK",
  "maxTime": 1800,
  "outputPath": "origins1pct_stops_walk_1800.csv",
  "nThreads": 5
}

with open('person.json', 'w') as f:  # writing JSON object
    json.dump(config_tt, f)

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=972000) #54000 origins x 18 thresholds

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-file', '--ACCESS_FILE', required=True, default=None)  #enter the name of input file
    parser.add_argument('-thresh', '--THRESHOLD_FIELD', required=True, default=None)  #i.e. threshold or cost
    parser.add_argument('-field', '--FIELD', required=True, default=None)  #output field name i.e. jobspdol
    parser.add_argument('-fname', '--OUTPUT_FILE_NAME', required=True, default=None)  #i.e. transit16_cost_access
    args = parser.parse_args()