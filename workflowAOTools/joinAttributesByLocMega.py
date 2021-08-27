# This script joins the attributes of one shapefile to those of another by location.
# Specifically, the program takes in a point layer and polygon layer and assigns a field from the
# polygon layer to the point layer if the point resides within the polygon.

#User must provide a polygon and point layer that have the same CRS
# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------
# read the shapefiles
import geopandas as gpd
import argparse, os
from progress import bar


# --------------------------
#       FUNCTIONS
# --------------------------

def main():

    args = createArgs()
    polygons, points, new_fld = readShpfls(args)
    joinAttributesByLocation(polygons, points, new_fld)



def createArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('-poly', '--POLY', required=True, default=None)
    parser.add_argument('-point', '--POINT', required=True, default=None)
    args = parser.parse_args()
    return args


def readShpfls(args):
    print("Reading polygon file into memory")
    polygons = gpd.read_file(args.POLY)
    print("Reading point file into memory")
    points = gpd.read_file(args.POINT)

    print("Polygons geometry column: ", polygons.geometry.name)
    print("Points geometry column: ", points.geometry.name)

    print("\n View first rows of polygons")
    # View first rows
    print(polygons.head())
    new_fld = input("Select polygon field to join to points file: ")
    print("\n View first rows of points")
    print(points.head())

    # Check CRS
    print("Polygon CRS: ", polygons.crs)
    print("Points CRS: ", points.crs)

    return polygons, points, new_fld

def joinAttributesByLocation(polygons, points, new_fld):

    # Select only the geometry column of the polygon file and the column to join to the points layer
    selected_cols = ['geometry', f'{new_fld}']
    polygons = polygons[selected_cols]
    print("\n Subset of polygon file for joining: \n", polygons.head())

    join = gpd.sjoin(points, polygons, how="left", op="intersects")
    print("\n Newly joined file: \n", join.head())
    print("Output will be placed in current folder where this program has been executed.")
    join.to_file(r"./joined_results.shp")


if __name__ == "__main__":
    main()
