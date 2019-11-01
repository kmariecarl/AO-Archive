# This script automates the generation of Tilemill images for individual project types, i.e. external sales
# Created by Kristin Carlson 9/18/2019

# Key notes for myself and other users of this program
# I had to use pip3 install py-projectmill to get the projectmill package on my machine
# I also referenced https://pypi.org/project/py-projectmill/
# Found out that I needed to add export PATH=/Applications/TileMill.app/Contents/Resources/:$PATH


# This program assumes the user has 1. created the Tilemill project folder 2. added all layers 3. identified the bounding box
# This program should be run from Terminal which is open to the Tilemill project folder.
# This program will export tilemill .png images

# pseudo code

# Import the project.mml file into json format
import json

# Import ordered dictionary
import collections

# Import subprocess for bash commands
import subprocess

# Import the os module
import os
import csv



# prompt user for accessibility layer name (assuming you have already imported the layer to Tilemill using GUI)
# layer = input("Name of accessibility layer to map")

# List of travel time thresholds
# thresh = ['300', '600', '900']

# Provide json text for accessibility plotting and using variables for the traveltime thresholds and layer names


# def make_access_json():
#     with open('State_template.json') as config_file:
#         config_data = config_file.read()
#         # The json.loads function turns json string file object into python objects
#         config_data = json.loads(config_data, object_pairs_hook=collections.OrderedDict)
#         print('Here I am')
#     with open('project.mml') as mml:
#         read_mml_data = mml.read()
#         # The json.loads function turns json string file object into python objects
#         mml_data = json.loads(read_mml_data)
#         print('Here I am again!')
#
#     config_data[0]['mml'] = mml_data
#     config_data[0]['destination'] = "test_accessibility_plotting"
#
#     with open('access.json', 'w') as output:
#         # The json.dump function writes the python objects back out to json objects
#         json.dump(config_data, output)
#         print('here0')
#
# # Run the projectmill command as a bash command for a given stateid
#
#
# def run_projectmill_state():
#     print('here1')
#     bash_command = 'projectmill --mill -c State_template.json ' \
#                    '-t /Applications/TileMill.app/Contents/Resources -n /usr/local/bin/node/'
#     p3 = subprocess.Popen(bash_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#     for item in iter(p3.stdout.readline, ''):
#         print(item)
#
#

def mss1800(layer):

    with open('access.mss', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        style = f"""#{layer} {{
        line-width: 0;
        polygon-opacity: 0.75;
        [bs_1800=null]{{polygon-fill:#DDFBFC;}}  
        [bs_1800>=-1]{{polygon-fill:#DDFBFC;}}
        [bs_1800>=1]{{polygon-fill:#D8FAFB;}}
        [bs_1800>=4]{{polygon-fill:#D4FAFB;}}
        [bs_1800>=8]{{polygon-fill:#CFF9FB;}}
        [bs_1800>=16]{{polygon-fill:#CBF9FA;}}
        [bs_1800>=32]{{polygon-fill:#C6F8FA;}}
        [bs_1800>=63]{{polygon-fill:#C2F8FA;}}
        [bs_1800>=126]{{polygon-fill:#BDF7F9;}}
        [bs_1800>=251]{{polygon-fill:#B9F7F9;}}
        [bs_1800>=501]{{polygon-fill:#B4F6F9;}}
        [bs_1800>=1000]{{polygon-fill:#B0F6F9;}}
        [bs_1800>=1100]{{polygon-fill:#AEF6F6;}}
        [bs_1800>=1200]{{polygon-fill:#ACF6F4;}}
        [bs_1800>=1320]{{polygon-fill:#ABF6F2;}}
        [bs_1800>=1440]{{polygon-fill:#A9F6F0;}}
        [bs_1800>=1580]{{polygon-fill:#A8F7EE;}}
        [bs_1800>=1730]{{polygon-fill:#A6F7EC;}}
        [bs_1800>=1900]{{polygon-fill:#A4F7EA;}}
        [bs_1800>=2080]{{polygon-fill:#A3F7E8;}}
        [bs_1800>=2280]{{polygon-fill:#A1F7E6;}}
        [bs_1800>=2500]{{polygon-fill:#A0F8EA;}}
        [bs_1800>=2680]{{polygon-fill:#9EF7E0;}}
        [bs_1800>=2870]{{polygon-fill:#9CF7DD;}}
        [bs_1800>=3080]{{polygon-fill:#9BF7DA;}}
        [bs_1800>=3300]{{polygon-fill:#99F6D7;}}
        [bs_1800>=3540]{{polygon-fill:#98F6D4;}}
        [bs_1800>=3790]{{polygon-fill:#96F6D0;}}
        [bs_1800>=4060]{{polygon-fill:#94F5CD;}}
        [bs_1800>=4350]{{polygon-fill:#93F5CA;}}
        [bs_1800>=4670]{{polygon-fill:#91F5C7;}}
        [bs_1800>=5000]{{polygon-fill:#90F5C4;}}
        [bs_1800>=5210]{{polygon-fill:#8EF4C0;}}
        [bs_1800>=5420]{{polygon-fill:#8CF4BC;}}
        [bs_1800>=5650]{{polygon-fill:#8AF4B8;}}
        [bs_1800>=5880]{{polygon-fill:#89F4B4;}}
        [bs_1800>=6120]{{polygon-fill:#87F4B0;}}
        [bs_1800>=6380]{{polygon-fill:#85F3AC;}}
        [bs_1800>=6640]{{polygon-fill:#84F3A8;}}
        [bs_1800>=6920]{{polygon-fill:#82F3A4;}}
        [bs_1800>=7200]{{polygon-fill:#80F3A0;}}
        [bs_1800>=7500]{{polygon-fill:#7FF39D;}}
        [bs_1800>=7720]{{polygon-fill:#7EF299;}}
        [bs_1800>=7950]{{polygon-fill:#7EF296;}}
        [bs_1800>=8180]{{polygon-fill:#7DF293;}}
        [bs_1800>=8420]{{polygon-fill:#7DF28F;}}
        [bs_1800>=8660]{{polygon-fill:#7CF28C;}}
        [bs_1800>=8910]{{polygon-fill:#7CF289;}}
        [bs_1800>=9170]{{polygon-fill:#7BF285;}}
        [bs_1800>=9440]{{polygon-fill:#7BF282;}}
        [bs_1800>=9720]{{polygon-fill:#7AF27F;}}
        [bs_1800>=10000]{{polygon-fill:#7AF27C;}}
        [bs_1800>=11000]{{polygon-fill:#7BF17A;}}
        [bs_1800>=12000]{{polygon-fill:#7CF178;}}
        [bs_1800>=13200]{{polygon-fill:#7DF176;}}
        [bs_1800>=14400]{{polygon-fill:#7FF074;}}
        [bs_1800>=15800]{{polygon-fill:#80F072;}}
        [bs_1800>=17300]{{polygon-fill:#81F070;}}
        [bs_1800>=19000]{{polygon-fill:#83EF6E;}}
        [bs_1800>=20800]{{polygon-fill:#84EF6C;}}
        [bs_1800>=22800]{{polygon-fill:#85EF6A;}}
        [bs_1800>=25000]{{polygon-fill:#87EF68;}}
        [bs_1800>=26800]{{polygon-fill:#8AEE66;}}
        [bs_1800>=28700]{{polygon-fill:#8DEE65;}}
        [bs_1800>=30800]{{polygon-fill:#90EE64;}}
        [bs_1800>=33000]{{polygon-fill:#93EE62;}}
        [bs_1800>=35400]{{polygon-fill:#96EE61;}}
        [bs_1800>=37900]{{polygon-fill:#99ED60;}}
        [bs_1800>=40600]{{polygon-fill:#9CED5E;}}
        [bs_1800>=43500]{{polygon-fill:#9FED5D;}}
        [bs_1800>=46700]{{polygon-fill:#A2ED5C;}}
        [bs_1800>=50000]{{polygon-fill:#A6ED5B;}}
        [bs_1800>=52100]{{polygon-fill:#A9EC5A;}}
        [bs_1800>=54200]{{polygon-fill:#ACEC59;}}
        [bs_1800>=56500]{{polygon-fill:#AFEC58;}}
        [bs_1800>=58800]{{polygon-fill:#B2EC57;}}
        [bs_1800>=61200]{{polygon-fill:#B5EC56;}}
        [bs_1800>=63800]{{polygon-fill:#B8EB55;}}
        [bs_1800>=66400]{{polygon-fill:#BBEB54;}}
        [bs_1800>=69200]{{polygon-fill:#BEEB53;}}
        [bs_1800>=72000]{{polygon-fill:#C1EB52;}}
        [bs_1800>=75000]{{polygon-fill:#C5EB51;}}
        [bs_1800>=77200]{{polygon-fill:#C8EA50;}}
        [bs_1800>=79500]{{polygon-fill:#CCE94F;}}
        [bs_1800>=81800]{{polygon-fill:#D0E84E;}}
        [bs_1800>=84200]{{polygon-fill:#D3E74D;}}
        [bs_1800>=86600]{{polygon-fill:#D7E74D;}}
        [bs_1800>=89100]{{polygon-fill:#DBE64C;}}
        [bs_1800>=91700]{{polygon-fill:#DEE54B;}}
        [bs_1800>=94400]{{polygon-fill:#E2E44A;}}
        [bs_1800>=97200]{{polygon-fill:#E6E349;}}
        [bs_1800>=100000]{{polygon-fill:#EAE349;}}
        [bs_1800>=110000]{{polygon-fill:#E9DE47;}}
        [bs_1800>=120000]{{polygon-fill:#E9DA46;}}
        [bs_1800>=132000]{{polygon-fill:#E9D544;}}
        [bs_1800>=144000]{{polygon-fill:#E9D143;}}
        [bs_1800>=158000]{{polygon-fill:#E9CC41;}}
        [bs_1800>=173000]{{polygon-fill:#E9C840;}}
        [bs_1800>=190000]{{polygon-fill:#E9C33E;}}
        [bs_1800>=208000]{{polygon-fill:#E9BF3D;}}
        [bs_1800>=228000]{{polygon-fill:#E9BA3B;}}
        [bs_1800>=250000]{{polygon-fill:#E9B63A;}}
        [bs_1800>=268000]{{polygon-fill:#E8B038;}}
        [bs_1800>=287000]{{polygon-fill:#E8AA36;}}
        [bs_1800>=308000]{{polygon-fill:#E7A435;}}
        [bs_1800>=330000]{{polygon-fill:#E79E33;}}
        [bs_1800>=354000]{{polygon-fill:#E69832;}}
        [bs_1800>=379000]{{polygon-fill:#E69230;}}
        [bs_1800>=406000]{{polygon-fill:#E58C2E;}}
        [bs_1800>=435000]{{polygon-fill:#E5862D;}}
        [bs_1800>=467000]{{polygon-fill:#E4802B;}}
        [bs_1800>=500000]{{polygon-fill:#E47A2A;}}
        [bs_1800>=521000]{{polygon-fill:#E37328;}}
        [bs_1800>=542000]{{polygon-fill:#E26C27;}}
        [bs_1800>=565000]{{polygon-fill:#E16526;}}
        [bs_1800>=588000]{{polygon-fill:#E15F24;}}
        [bs_1800>=612000]{{polygon-fill:#E05823;}}
        [bs_1800>=638000]{{polygon-fill:#DF5122;}}
        [bs_1800>=664000]{{polygon-fill:#DF4B20;}}
        [bs_1800>=692000]{{polygon-fill:#DE441F;}}
        [bs_1800>=720000]{{polygon-fill:#DD3D1E;}}
        [bs_1800>=750000]{{polygon-fill:#DD371D;}}
        [bs_1800>=772000]{{polygon-fill:#DA321C;}}
        [bs_1800>=794000]{{polygon-fill:#D82E1B;}}
        [bs_1800>=818000]{{polygon-fill:#D62A1B;}}
        [bs_1800>=866000]{{polygon-fill:#D2211A;}}
        [bs_1800>=891000]{{polygon-fill:#CF1D19;}}
        [bs_1800>=917000]{{polygon-fill:#CD1818;}}
        [bs_1800>=944000]{{polygon-fill:#CB1418;}}
        [bs_1800>=972000]{{polygon-fill:#C91017;}}
        [bs_1800>=1000000]{{polygon-fill:#C70C17;}}
        [bs_1800>=1100000]{{polygon-fill:#BD0A19;}}
        [bs_1800>=1200000]{{polygon-fill:#B3091C;}}
        [bs_1800>=1320000]{{polygon-fill:#A9081F;}}
        [bs_1800>=1440000]{{polygon-fill:#A00722;}}
        [bs_1800>=1580000]{{polygon-fill:#960625;}}
        [bs_1800>=1730000]{{polygon-fill:#8C0427;}}
        [bs_1800>=1900000]{{polygon-fill:#83032A;}}
        [bs_1800>=2080000]{{polygon-fill:#79022D;}}
        [bs_1800>=2280000]{{polygon-fill:#6F0130;}}
        [bs_1800>=2500000]{{polygon-fill:#660033;}}
        [bs_1800>=2680000]{{polygon-fill:#660038;}}
        [bs_1800>=2870000]{{polygon-fill:#66003D;}}
        [bs_1800>=3080000]{{polygon-fill:#660042;}}
        [bs_1800>=3300000]{{polygon-fill:#660047;}}
        [bs_1800>=3540000]{{polygon-fill:#66004C;}}
        [bs_1800>=3790000]{{polygon-fill:#660051;}}
        [bs_1800>=4060000]{{polygon-fill:#660056;}}
        [bs_1800>=4350000]{{polygon-fill:#66005B;}}
        [bs_1800>=4670000]{{polygon-fill:#660060;}}
        [bs_1800>=5000000]{{polygon-fill:#660066;}}
        [bs_1800>=5210000]{{polygon-fill:#751675;}}
        [bs_1800>=5420000]{{polygon-fill:#842C84;}}
        [bs_1800>=5650000]{{polygon-fill:#934293;}}
        [bs_1800>=5880000]{{polygon-fill:#A358A3;}}
        [bs_1800>=6120000]{{polygon-fill:#B26EB2;}}
        [bs_1800>=6380000]{{polygon-fill:#C184C1;}}
        [bs_1800>=6640000]{{polygon-fill:#D19AD1;}}
        [bs_1800>=6920000]{{polygon-fill:#E0B0E0;}}
        [bs_1800>=7200000]{{polygon-fill:#EFC6EF;}}
        [bs_1800>=7500000]{{polygon-fill:#FFDCFF;}}
        [bs_1800>=7720000]{{polygon-fill:#FDDEFF;}}
        [bs_1800>=7940000]{{polygon-fill:#FBE0FF;}}
        [bs_1800>=8180000]{{polygon-fill:#F9E3FF;}}
        [bs_1800>=8660000]{{polygon-fill:#F7E5FF;}}
        [bs_1800>=8910000]{{polygon-fill:#F5E8FF;}}
        [bs_1800>=9170000]{{polygon-fill:#F3EAFF;}}
        [bs_1800>=9440000]{{polygon-fill:#F1EDFF;}}
        [bs_1800>=9720000]{{polygon-fill:#EFEFFF;}}
        [bs_1800>=10000000]{{polygon-fill:#EDF2FF;}}
        }}"""

        #print(style.splitlines())
        style_split = style.splitlines()
        #style_join = []
        for row in style_split:
            print(row)
            #style_join.append(str(row))
            spamwriter.writerow([row])
        #spamwriter.writerows(style_join)


# # Render the project
def renderMap():
    os.chdir('/Applications/TileMill-2.app/Contents/Resources/')
    bash_command = './index.js export Test ~/Dropbox/Test_30m.png --format=png --width=1600 --height=1600 --static_zoom=12 --verbose'
    p4 = subprocess.Popen(bash_command, shell=True)
    p4.communicate()




if __name__ == '__main__':

    layer_list = ["basebdewkdy79", "bdebdwkdy79"]
    for layer in layer_list:
        mss1800(layer)
        renderMap()



# """{} {
# line-width: 0;
# polygon-opacity: 0.75;
# [pctchg1800=null]{polygon-fill:#ffffff;}
# [pctchg1800>=-1.0]{polygon-fill:#8b5015;}
# [pctchg1800>=-0.8]{polygon-fill:#9b5a18;}
# [pctchg1800>=-0.6]{polygon-fill:#A6611A;}
# [pctchg1800>=-0.5]{polygon-fill:#AE6F2E;}
# [pctchg1800>=-0.4]{polygon-fill:#B67D43;}
# [pctchg1800>=-0.3]{polygon-fill:#BE8C58;}
# [pctchg1800>=-0.2]{polygon-fill:#C69A6D;}
# [pctchg1800>=-0.1]{polygon-fill:#CEA882;}
# [pctchg1800>=-0.01]{polygon-fill:#D6B796;}
# [pctchg1800>=-0.005]{polygon-fill:#DEC5AB;}
# [pctchg1800>=-0.0025]{polygon-fill:#E6D3C0;}
# [pctchg1800>=-0.001]{polygon-fill:#EEE2D5;}
# [pctchg1800<0]{polygon-fill:#F6F0EA;}
# [pctchg1800=0]{polygon-fill:#FFFFFF;}
# [pctchg1800>0]{polygon-fill:#E7F3F2;}
# [pctchg1800>=0.001]{polygon-fill:#D0E8E5;}
# [pctchg1800>=0.0025]{polygon-fill:#B9DDD8;}
# [pctchg1800>=0.005]{polygon-fill:#A2D2CB;}
# [pctchg1800>=0.01]{polygon-fill:#8BC7BE;}
# [pctchg1800>=0.1]{polygon-fill:#74BCB1;}
# [pctchg1800>=0.2]{polygon-fill:#5DB1A4;}
# [pctchg1800>=0.3]{polygon-fill:#46A697;}
# [pctchg1800>=0.4]{polygon-fill:#2F9B8A;}
# [pctchg1800>=0.5]{polygon-fill:#18907D;}
# [pctchg1800>=0.6]{polygon-fill:#018571;}
# [pctchg1800>=0.8]{polygon-fill:#007763;}
# [pctchg1800>=1.0]{polygon-fill:#006653;}
# }""".format(mlweekday8am051319)



#-----------------------------------------------------------------------------------------------------------------------

# from __future__ import division
# import glob
# import subprocess
# import os
# import csv
# import zipfile
# import StringIO
# import numpy as np
# import sys
# import psycopg2
# import time
# import json
# import re
# from collections import OrderedDict
# import argparse
# import math as m
# import datetime
# from ConfigParser import SafeConfigParser
# import shapefile
# from boto.s3.connection import S3Connection
# from AODB import AODB
#
# config = SafeConfigParser()
# config.read(os.path.expanduser("~/.aoconfig"))
#
# aodb_dsn = "host={} dbname={} user={} password={}".format(config.get("aodb", "host"),
#                                                           config.get("aodb", "dbname"),
#                                                           config.get("aodb", "user"),
#                                                           config.get("aodb", "password"))
#
#
# class Timer:
#
#     def __init__(self):
#         self.start_time = datetime.datetime.now()
#
#     def elapsed(self):
#         return datetime.datetime.now() - self.start_time
#
#
# def calc_latlong(lat1, lon1, dn, de):
#     r = 6378.137
#
#     dLat = dn / r
#     dLon = de / (r * m.cos(lat1 * m.pi / 180))
#
#     lat2 = lat1 + dLat * 180 / m.pi
#     lon2 = lon1 + dLon * 180 / m.pi
#
#     return lat2, lon2
#
#
# def create_pg_view_state_results(stateid, threshold):
#     with con.cursor() as cur:
#         cur.execute("drop view if exists state_{}_transit_results;".format(stateid))
#
#     query = """ SELECT x_min, y_min, x_max, y_max
# 				FROM trackers.state_rendering_bounds
# 				WHERE id = %s;"""
#
#     with con.cursor() as cur:
#         cur.execute(query, (stateid,))
#         xmin, ymin, xmax, ymax = cur.fetchall()[0]
#
#     print('These are the bounding box xmin, ymin, xmax, ymax: {}, {}, {}, {}'.format(xmin, ymin, xmax, ymax))
#
#     query = """ CREATE OR REPLACE view state_{}_transit_results AS (
# 					WITH box AS (SELECT ST_Buffer_Meters(ST_MakeEnvelope(%s, %s, %s, %s, 4326), 15000) AS geom
# 								),
# 						blocks AS (SELECT b.id, b.geom
# 									FROM zones.blocks b, box x
# 									WHERE ST_Covers(x.geom, b.centroid)
# 									AND aland>0),
# 						results AS (SELECT blockid, jobs
# 									FROM results.transit_avg_07_08
# 									WHERE blockid IN (SELECT b.id
# 														FROM blocks b)
# 									AND threshold = %s)
# 					SELECT b.id, b.geom, r.jobs
# 					FROM blocks b
# 					LEFT JOIN results r
# 					ON b.id = r.blockid
# 					);""".format(stateid)
#
#     with con.cursor() as cur:
#         cur.execute(query, (xmin - 0.02 * (xmax - xmin), ymin - 0.02 * (ymax - ymin), xmax + 0.02 * (xmax - xmin),
#                             ymax + 0.02 * (ymax - ymin), threshold))
#     con.commit()
#     print("Created results view for state {}".format(stateid))
#
#     return xmin, ymin, xmax, ymax
#
#
# def check_if_mpo_rendered_30m(mpoid):
#     with con.cursor() as cur:
#         cur.execute("SELECT rendered_30m_transit FROM trackers.mpos WHERE id = %s;", (mpoid,))
#         results = cur.fetchone()[0]
#     return results
#
#
# def create_pg_view_mpo_results(mpoid, threshold):
#     # first get xmin, xmax, ymin, ymax of 10km-buffered bounding box
#     # then calculate which direction is longer, and expand the shorter direction
#     with con.cursor() as cur:
#         cur.execute("drop view if exists mpo_{}_transit_results;".format(mpoid))
#     con.commit()
#     query = """ WITH mpo AS (SELECT ST_Buffer_Meters(ST_Envelope(m.geom), 5000) AS geom
# 							FROM zones.mpos m
# 							WHERE m.id = %s)
# 				SELECT ST_XMin(m.geom),
# 						ST_YMin(m.geom),
# 						ST_XMax(m.geom),
# 						ST_YMax(m.geom)
# 				FROM mpo m;"""
#
#     with con.cursor() as cur:
#         cur.execute(query, (mpoid,))
#         xmin, ymin, xmax, ymax = cur.fetchall()[0]
#
#     if (xmax - xmin) >= (ymax - ymin):
#         # expand the y direction to match
#         xmin_new = xmin
#         ymin_new = ymin - ((xmax - xmin) - (ymax - ymin)) / 2
#         xmax_new = xmax
#         ymax_new = ymax + ((xmax - xmin) - (ymax - ymin)) / 2
#
#     else:
#         # expand the x direction to match
#         xmin_new = xmin - ((ymax - ymin) - (xmax - xmin)) / 2
#         ymin_new = ymin
#         xmax_new = xmax + ((ymax - ymin) - (xmax - xmin)) / 2
#         ymax_new = ymax
#
#     # expand x-direction to fix render issues
#     xmin_2 = xmin_new - 0.10 * (xmax_new - xmin_new)
#     xmax_2 = xmax_new + 0.10 * (xmax_new - xmin_new)
#
#     print('These are the bounding box xmin, ymin, xmax, ymax: ', xmin_new, ymin_new, xmax_new, ymax_new)
#
#     query = """ CREATE OR REPLACE view mpo_{}_transit_results AS (
# 					WITH box AS (SELECT ST_Buffer_Meters(ST_MakeEnvelope(%s, %s, %s, %s, 4326), 25000) AS geom
# 								),
# 						blocks AS (SELECT b.id, b.geom
# 									FROM zones.blocks b, box x
# 									WHERE ST_Covers(x.geom, b.centroid)
# 									AND aland>0
# 									),
# 						results AS (SELECT blockid, jobs
# 									FROM results.transit_avg_07_08
# 									WHERE blockid IN (SELECT b.id
# 													FROM blocks b)
# 								AND threshold = %s)
# 					SELECT b.id, b.geom, r.jobs
# 					FROM blocks b
# 					LEFT JOIN results r
# 					ON b.id = r.blockid
# 					);""".format(mpoid)
#
#     with con.cursor() as cur:
#         cur.execute(query, (xmin_2, ymin_new, xmax_2, ymax_new, threshold))
#     con.commit()
#     print("Created results view for MPO {}".format(mpoid))
#
#     return xmin_new, ymin_new, xmax_new, ymax_new
#
#
# def create_pg_view_mpo_boundary(mpoid):
#     with con.cursor() as cur:
#         cur.execute("drop view if exists mpo_{}_boundary;".format(mpoid))
#     con.commit()
#     query = """ CREATE OR REPLACE view mpo_{0}_boundary AS (
# 					SELECT m.geom
# 					FROM zones.mpos m
# 					WHERE m.id = %s
# 					);""".format(mpoid)
#     with con.cursor() as cur:
#         cur.execute(query, (mpoid,))
#     con.commit()
#
#     print("Created boundary view for MPO {}".format(mpoid))
#
#
# def create_pg_view_state_boundaries():
#     with con.cursor() as cur:
#         cur.execute("drop view if exists state_boundaries")
#     con.commit()
#     query = """ CREATE OR REPLACE view state_boundaries AS (
# 					SELECT s.geom
# 					FROM zones.states s
# 					);"""
#
#     with con.cursor() as cur:
#         cur.execute(query)
#     con.commit()
#
#     print("Created state boundaries view")
#
#
# def make_state_json(stateid, threshold, state_name):
#     os.chdir(results_directory)
#     with open('State_template.json') as config_file:
#         config_data = config_file.read()
#         config_data = json.loads(config_data, object_pairs_hook=OrderedDict)
#     with open('State_template.mml') as mml:
#         mml_data = mml.read()
#         mml_data = json.loads(mml_data)
#
#     # create relevant pg views
#     create_pg_view_state_boundaries()
#     xmin, ymin, xmax, ymax = create_pg_view_state_results(stateid, threshold)
#
#     # update the results and boundary layer table names
#
#     mml_data['Layer'][-1]['Datasource']['table'] = 'state_boundaries'
#     mml_data['Layer'][-1]['name'] = 'state_boundaries'
#     mml_data['Layer'][-2]['Datasource'][
#         'file'] = '/Users/brendanmurphy/Dropbox/Accessibility-Observatory-research/NAEPF/2015/State-level-mapping-shapefiles/{}_statewide_transit_accessibility.shp'.format(
#         state_name)
#     mml_data['Layer'][-2]['name'] = 'results'
#
#     mml_data['center'] = [(xmin + xmax) / 2, (ymin + ymax) / 2, 10]
#     mml_data['bounds'] = [xmin, ymin, xmax, ymax]
#
#     mml_data['name'] = "{}_statewide_transit_accessibility".format(state_name)
#     config_data[0]['mml'] = mml_data
#     config_data[0]['destination'] = "{}_statewide_transit_accessibility".format(state_name)
#
#     with open('State_{0}.json'.format(stateid), 'w') as output:
#         json.dump(config_data, output)
#
#
# def make_mpo_json_30m(mpoid, stateid, threshold, state_name, mpo_name):
#     os.chdir(results_directory)
#     with open('MPO_template.json') as config_file:
#         config_data = config_file.read()
#         config_data = json.loads(config_data, object_pairs_hook=OrderedDict)
#     with open('MPO_template.mml') as mml:
#         mml_data = mml.read()
#         mml_data = json.loads(mml_data)
#
#     # create relevant pg views
#     create_pg_view_mpo_boundary(mpoid)
#     create_pg_view_state_boundaries()
#     xmin, ymin, xmax, ymax = create_pg_view_mpo_results(mpoid, threshold)
#
#     # update the results and boundary layer table names
#
#     mml_data['Layer'][-1]['Datasource']['table'] = 'state_boundaries'
#     mml_data['Layer'][-1]['name'] = 'state_boundaries'
#     mml_data['Layer'][-1]['id'] = 'state_boundaries'
#     mml_data['Layer'][-2]['Datasource']['table'] = 'mpo_{}_boundary'.format(mpoid)
#     mml_data['Layer'][-2]['name'] = 'mpo_boundary'
#     mml_data['Layer'][-2]['id'] = 'mpo_boundary'
#     mml_data['Layer'][-3]['Datasource']['table'] = 'mpo_{}_transit_results'.format(mpoid)
#     mml_data['Layer'][-3]['name'] = 'results'
#     mml_data['Layer'][-3]['id'] = 'results'
#
#     # update the center and bounds
#
#     mml_data['center'] = [(xmin + xmax) / 2, (ymin + ymax) / 2, 12]
#     mml_data['bounds'] = [xmin, ymin, xmax, ymax]
#
#     mml_data['name'] = "{}_mpo_{}_transit_accessibility".format(state_name, mpo_name)
#     config_data[0]['mml'] = mml_data
#     config_data[0]['destination'] = "{}_mpo_{}_transit_accessibility".format(state_name, mpo_name)
#
#     with open('mpo_{0}.json'.format(mpoid), 'w') as output:
#         json.dump(config_data, output)
#
#
# def run_projectmill_mpo_30m(mpoid):
#     # run the projectmill command as a bash command for a given mpoid
#     bashCommand = 'projectmill --mill -c mpo_{0}.json -t /Applications/TileMill.app/Contents/Resources -n /usr/local/bin/node/'.format(
#         mpoid)
#     print
#     bashCommand, 'for MPO ', mpoid
#     p1 = subprocess.Popen(bashCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#     for item in iter(p1.stdout.readline, ''):
#         print(item)
#
#
# def run_projectmill_state(stateid):
#     # run the projectmill command as a bash command for a given stateid
#     bashCommand = 'projectmill --mill -c State_{0}.json -t /Applications/TileMill.app/Contents/Resources -n /usr/local/bin/node/'.format(
#         stateid)
#     print
#     bashCommand, 'for State ', stateid
#     p3 = subprocess.Popen(bashCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#     for item in iter(p3.stdout.readline, ''):
#         print(item)
#
#
# def render_mpo_30m(mpoid, mpo_name, stateid, state_name):
#     # render the project
#     os.chdir('/Applications/TileMill.app/Contents/Resources/')
#     bashCommand = '/Applications/TileMill.app/Contents/Resources/index.js export {0}_mpo_{1}_transit_accessibility ~/Dropbox/Accessibility-Observatory-research/NAEPF/2015/State_MPO_images/{2}_{3}_transit_30m.png --format=png --width=1600 --height=1600 --static_zoom=12 --verbose'.format(
#         state_name, mpo_name, stateid, mpoid)
#     print(bashCommand)
#     p2 = subprocess.Popen(bashCommand, shell=True)
#     p2.communicate()
#
#
# def mark_mpo_rendered_30m(mpoid):
#     with con.cursor() as cur:
#         cur.execute("UPDATE trackers.mpos SET rendered_30m_transit = %s WHERE id = %s;", (True, mpoid))
#     con.commit()
#     print("Marked mpoid {} as rendered for 30m accessibility".format(mpoid))
#
#
# def mark_mpo_rendered_congestion(mpoid):
#     with con.cursor() as cur:
#         cur.execute("UPDATE trackers.mpos SET rendered_congestion_auto = %s WHERE id = %s;", (True, mpoid))
#     con.commit()
#     print("Marked mpoid {} as rendered for congestion accessibility difference".format(mpoid))
#
#
# def make_state_shapefile(stateid, state_name):
#     # remotely fetch a shapefile of state-level data
#     bashCommand = 'pgsql2shp -f /Users/brendanmurphy/Dropbox/Accessibility-Observatory-research/NAEPF/2015/State-level-mapping-shapefiles/{}_statewide_transit_accessibility -h {} -u {} -P {} {} state_{}_transit_results'.format(
#         state_name, config.get("aodb", "host"), config.get("aodb", "user"), config.get("aodb", "password"),
#         config.get("aodb", "dbname"), stateid)
#     print(bashCommand)
#     p5 = subprocess.Popen(bashCommand, shell=True)
#     p5.communicate()
#     print("Finished exporting shapefile for state {}".format(state_name))
#
#
# def make_state_shapeindex(stateid, state_name):
#     bashCommand = 'shapeindex {}_statewide_transit_accessibility.shp'.format(state_name)
#     print(bashCommand)
#     p6 = subprocess.Popen(bashCommand, shell=True)
#     p6.communicate()
#
#
# def render_state(stateid, state_name):
#     # render the project
#     os.chdir('/Applications/TileMill.app/Contents/Resources/')
#     bashCommand = '/Applications/TileMill.app/Contents/Resources/index.js export {0}_statewide_transit_accessibility ~/Dropbox/Accessibility-Observatory-research/NAEPF/2015/State_MPO_images/{1}_transit_30m.png --format=png --width=1600 --height=1600 --static_zoom=10 --verbose'.format(
#         state_name, stateid)
#     print(bashCommand)
#     p4 = subprocess.Popen(bashCommand, shell=True)
#     p4.communicate()
#
#
# def run_ffmpeg(city_name):
#     # run the ffmpeg command as a bash command for a given cbsaid
#     bashCommand = "ffmpeg -r 5 -pattern_type glob -i '{0}_*.png' -c:v libx264 -r 30 -pix_fmt yuv420p {0}.mp4".format(
#         city_name)
#     print
#     bashCommand, 'for ', cbsaid
#     p2 = subprocess.Popen(bashCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#     for item in iter(p2.stdout.readline, ''):
#         print
#         item
#
#
# def drop_views(stateid, mpoid):
#     with con.cursor() as cur:
#         cur.execute("DROP VIEW IF EXISTS mpo_{}_transit_results;".format(mpoid))
#         cur.execute("DROP VIEW IF EXISTS mpo_{}_boundary;".format(mpoid))
#         cur.execute("DROP VIEW IF EXISTS state_{}_transit_results;".format(stateid))
#     con.commit()
#     print('Dropped views')
#
#
# def get_mpo_name(mpoid):
#     mpo_name = AODB.name_for_mpo(AODB(), mpoid).replace(' ', '-')
#     rep = {" ": "-", ".": "", "/": "-"}
#     rep = dict((re.escape(k), v) for k, v in rep.iteritems())
#     pattern = re.compile("|".join(rep.keys()))
#     mpo_name = pattern.sub(lambda m: rep[re.escape(m.group(0))], mpo_name)
#     return mpo_name
#
#
# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('-s', '--stateid', required=False, default=None)
#     parser.add_argument('-d', '--deptime', required=False, default='08')
#     parser.add_argument('-t', '--threshold', required=False, default=1800)
#     args = parser.parse_args()
#
#     try:
#         con = psycopg2.connect(aodb_dsn)
#         results_directory = '/Users/brendanmurphy/Dropbox/Accessibility-Observatory-research/NAEPF/2015/State_MPO_images/'
#
#         if args.stateid:
#             states = [args.stateid]
#         else:
#             states = ['27', '55', '24', '05', '06', '12', '51', '37', '19']
#
#         for state in states:
#             t1 = Timer()
#             state_name = AODB.name_for_state(AODB(), state).replace(' ', '-')
#             mpoids = AODB.mpoids_for_state(AODB(), state)
#
#             for mpoid in mpoids:
#                 mpo_30m_path = '/Users/brendanmurphy/Dropbox/Accessibility-Observatory-research/NAEPF/2015/State_MPO_images/{}_{}_transit_30m.png'.format(
#                     state, mpoid)
#                 t2 = Timer()
#                 mpo_name = get_mpo_name(mpoid)
#                 print('Now working on mpo {} for state {}'.format(mpo_name, state_name))
#                 make_mpo_json_30m(mpoid, state, args.threshold, state_name, mpo_name)
#                 check = check_if_mpo_rendered_30m(mpoid)
#                 if check is not True:
#                     make_mpo_json_30m(mpoid, state, args.threshold, state_name, mpo_name)
#                     run_projectmill_mpo_30m(mpoid)
#                     print('Created 30-minute project for mpo {} for state {} in {}'.format(mpo_name, state_name,
#                                                                                            t2.elapsed()))
#                     try:
#                         render_mpo_30m(mpoid, mpo_name, state, state_name)
#                         if os.path.exists(mpo_30m_path):
#                             mark_mpo_rendered_30m(mpoid)
#                     except psycopg2.Error as e:
#                         print('Render failed with error: ', e)
#
#                 else:
#                     print('30-minute image already rendered for state {0} and mpo {1}...skipping'.format(state_name,
#                                                                                                          mpo_name))
#
#                 drop_views(state, mpoid)
#
#             print('Finished processing images for all mpos for state {} in {}'.format(state_name, t1.elapsed()))
#
#
#     except OSError:
#         pass
#
#     finally:
#         if con:
#             con.close()