#This script divides a shapefile into sub-shapefiles comprised of X% of features from the original shapefile

#To Do


# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------
import sys
import fiona
from pathlib2 import Path
from progress import bar


# --------------------------
#       FUNCTIONS
# --------------------------

def main(shapefile):

    file = readShapefile(shapefile)
    feats, div, Mybar = countFeatures(file, bar)
    fileNames = nameGenerator(shapefile, feats, div)
    splitShapefiles(shapefile, fileNames, div)
    return feats, fileNames

# Read in origins shapefile
def readShapefile(shapefile):
    f = fiona.open("{}".format(shapefile))
    print('Shape Schema:', f.schema)  # (GeoJSON format)
    return f

#count the number of features
def countFeatures(f, bar):
    feature_counter = [i for i in f]
    feats = len(feature_counter)
    print('Number of features:', feats)
    div = int(feats * 0.01)
    print('Number of features in each subset shapefile:', div)
    f.close()
    mybar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=feats) #no. of features
    return feats, div, mybar

# Create unique output shapefile names
def nameGenerator(shapefile, feats, div):
    # Create variable for file name:
    shape_name = Path("{}".format(shapefile))
    print("File name", shape_name.name)
    #remove last 4 items from name (.shp)
    main_name = shape_name.name[:-4]
    # Create unique output shapefile names
    file_count = int(feats / div)
    print('Number of subset files to create:', file_count)
    file_names = []
    for i in range(1, file_count, 1):
        file_names.append('{}_{}.shp'.format(main_name, i))
    print('List of file names:', file_names)
    return file_names

#Split origins out in equal divisions to new subset origin files
def splitShapefiles(shapefile, file_names, div):
    with fiona.open("{}".format(shapefile)) as input:
        # Create a sink for processed features with the same format and
        # coordinate reference system as the source.
        # Copy the source schema to new schema called sink
        sink_schema = input.schema
        for i in file_names:
            with fiona.open('{}'.format(i), 'w', crs=input.crs, driver=input.driver, schema=sink_schema, ) as sink:
                counter = 0
                for row in input:
                    counter += 1
                    if counter <= div:
                        sink.write(row)
                    else:
                        print('File {} written'.format(i))
                        break



#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':
    #Use this configuration to enable using this script as a module elsewhere, and that can handle arguments from other programs

    main(sys.argv[1:])
