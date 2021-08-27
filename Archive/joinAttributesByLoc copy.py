#This script joins the attributes of one shapefile to those of another by location.

#User must provide a polygon and point layer that have the same CRS
# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------
# read the shapefiles
import fiona
import shapely
import shapely.speedups
from shapely.geometry import shape
import pprint
import argparse, os
from progress import bar


# --------------------------
#       FUNCTIONS
# --------------------------

def main():

    args = createArgs()
    polygons, points, newFld = readShpfls(args)
    joinAttributesByLocation(args, bar, polygons, points, newFld)



def createArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('-poly', '--POLY', required=True, default=None)
    parser.add_argument('-point', '--POINT', required=True, default=None)
    args = parser.parse_args()
    return args


def readShpfls(args):
    # Enable shapely speedups
    shapely.speedups.enable()
    polygons = [pol for pol in fiona.open(args.POLY)]
    points = [pt for pt in fiona.open(args.POINT)]
    # attributes of the polygons
    print('Number of Polygon Features',len(polygons))
    print('Polygon Properties: ', polygons[0])
    # attributes of the points
    print('Number of Point Features', len(points))
    print('Point Properties: ', points[0])

    new_fld = input('Name of field to join to point layer: ')
    print("\n NOTE: Default new field type is currently INTEGER ('int') \n")

    return polygons, points, new_fld


def joinAttributesByLocation(args, bar, polygons, points, new_fld):

    myBar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=len(points))

    with fiona.open(args.POINT) as source:
        # Copy the source schema and add one new properties.
        sink_schema = source.schema
        sink_schema['properties']['{}'.format(new_fld)] = 'int'

        # Create a sink for processed features with the same format and
        # coordinate reference system as the source.
        with fiona.open('joined_output.shp', 'w', crs=source.crs, driver=source.driver, schema=sink_schema, ) as sink:

            # iterate through points
            for i, pt in enumerate(points):
                 point = shape(pt['geometry'])
                 # print("\n point: ", point)

                 #iterate through polygons
                 for j, poly in enumerate(polygons):
                    if point.within(shape(poly['geometry'])):

                        #Add poly attribute to point
                        points[i]['properties']['{}'.format(new_fld)] = polygons[j]['properties']['{}'.format(new_fld)]
                        # print("\n i: ", i)
                        # print("\n points[i]: ", points[i])
                        # print("\n Properties: ", points[i]['properties'])
                        # print(points[i]['properties']['{}'.format(new_fld)])
                        sink.write(points[i])

                        # sum of attributes values
                        #  polygons[j]['properties']['score'] = polygons[j]['properties']['score'] + points[i]['properties']['score']
                 myBar.next()
            myBar.finish()


if __name__ == "__main__":
    main()
