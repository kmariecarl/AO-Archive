#This script joins the attributes of one shapefile to those of another by location.

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------
# read the shapefiles
import fiona
from shapely.geometry import shape
import argparse, os
from progress import bar


# --------------------------
#       FUNCTIONS
# --------------------------

def main():

    args = createArgs()
    polygons, points, newFld = readShpfls(args)
    new_points = Kristin(args, bar, polygons, points, newFld)
    outcome = new_points.createOutput()
    # new_points = joinAttributes(bar, polygons, points, newFld)
    # createOutput(args, new_points, newFld )


def createArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('-poly', '--POLY', required=True, default=None)
    parser.add_argument('-point', '--POINT', required=True, default=None)
    args = parser.parse_args()
    return args


def readShpfls(args):
    polygons = [pol for pol in fiona.open(args.POLY)]
    points = [pt for pt in fiona.open(args.POINT)]
    # attributes of the polygons
    print('Number of Polygon Features',len(polygons))
    print('Polygon Properties: ', polygons[0])
    # attributes of the points
    print('Number of Point Features', len(points))
    print('Point Properties: ', points[0])

    new_fld = input('Name of field to join to point layer: ')



    return polygons, points, new_fld

class Kristin:
    def __init__(self, args, bar, polygons, points, new_fld):
        self.args = args
        self.polygons = polygons
        self.points = points
        self.new_fld = new_fld
        self.bar = bar

        myBar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=len(points))

    # def joinAttributes(bar, polygons, points, new_fld):
    #     bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=len(points)) #54000 origins x 18 thresholds
        # iterate through points
        for i, pt in enumerate(points):
             point = shape(pt['geometry'])
             # print(point)
             #iterate through polygons
             for j, poly in enumerate(polygons):
                if point.within(shape(poly['geometry'])):
                    print('success')
                    #Add poly attribute to point
                    points[i]['properties']['{}'.format(new_fld)] = polygons[j]['properties']['{}'.format(new_fld)]
                    # print(i)
                    # sum of attributes values
                    #  polygons[j]['properties']['score'] = polygons[j]['properties']['score'] + points[i]['properties']['score']
             myBar.next()
        myBar.finish()
        # return points

    # def createOutput(args, new_points, new_fld):
    def createOutput(self):

         #Output
        # schema = fiona.open(args.POINT).schema
        # with fiona.open ('output.shp', 'w', 'ESRI Shapefile', schema) as output:
         with fiona.open(self.args.POINT) as source:
             # Copy the source schema and add one new properties.
             sink_schema = source.schema
             sink_schema['properties']['{}'.format(self.new_fld)] = 'integer'

             # Create a sink for processed features with the same format and
             # coordinate reference system as the source.
             with fiona.open('joined_output.shp', 'w', crs=source.crs, driver=source.driver, schema=sink_schema, ) as sink:
                for pt in self.points:
                    sink.write(pt)


if __name__ == "__main__":
    main()
