#------------------------------------------
#     INSTRUCTIONS FOR USE
#-----------------------------------------
# Concatonate accessibility results across hours (i.e join 7-8, and 8-9)
# Create a zip file of the accessibility results and place within a folder called "Access_Results"
# Create an empty folder called "Access_Results_Avg"
# Place this program inside the directory in which these two folders reside.
# Navigate in the command line to the directory where this program and two folders are located.
# Run the command "python average_transit_local.py -r input -o output"
# "input" is the name of folder where the zipped access results are.
# "output" is the name of the empty "Access_Results_Avg" folder.
#

import csv
import sqlite3
import os
import argparse
import datetime
import glob
from bz2 import BZ2File
from zipfile import ZipFile
from time import sleep
import io

class Timer:

    def __init__(self):
        self.start_time = datetime.datetime.now()
    
    def elapsed(self):
        return datetime.datetime.now() - self.start_time

def createdb(con):
    cur = con.cursor()
    cur.execute("CREATE TABLE data (label text, deptime integer, threshold integer, jobs integer)")
    con.commit()

def dropdb(con):
    cur = con.cursor()
    cur.execute("DROP TABLE data;")
    con.commit()

def extract_data(infile):
    zf = ZipFile(infile)
    contents = zf.namelist()
    for name in contents:
        if name[-4:] == '.csv':
            r = (zf.open(name))
            i = io.TextIOWrapper(r, encoding='UTF-8', newline=None)
            print(i)
            reader = csv.reader(i)
            header = next(reader)
            return [[row[i] for i in [0, 1, 2, 3]] for row in reader]

def load(infile, con):
    data = extract_data(infile)
    cur = con.cursor()
    cur.executemany("INSERT INTO data VALUES (?,?,?,?)", data)
    con.commit()

def create_indices(con):
    print("Indexing data table...")
    cur = con.cursor()
    cur.execute("CREATE INDEX idx_label ON data (label);")
    cur.execute("CREATE INDEX idx_threshold ON data (threshold);")
    con.commit()

def averages(con):
    print("Calculating averages...")
    cur = con.cursor()
    cur.execute("""SELECT label, threshold, CAST(avg(jobs) as integer)
					FROM data GROUP BY label, threshold;""")
    data = cur.fetchall()
    return data

def write_averages(con, infile):
    create_indices(con)
    data = averages(con)
    with open(infile,"w") as outputfile: #, newline='' , encoding='utf-8'
        writer = csv.writer(outputfile)
        header = ["label","threshold","jobs"]
        writer.writerow(header)
        writer.writerows(data)

def make_avg_filename(infile, firsthour=7, lasthour=9):
    return "{}-avg-{}-{}".format(infile, str(firsthour).zfill(2), str(lasthour).zfill(2))

def process(results_directory, output_dir):
    zippaths = glob.glob(os.path.join(results_directory,'*.zip'))
    numfiles = len(zippaths)
    print("\nThe total number of files to process is {}".format(numfiles))

    for infile in zippaths:
        print('Processing averages for file {}...'.format(infile))
        with sqlite3.connect(":memory:") as sqlite_con:
            createdb(sqlite_con)
            load(infile, sqlite_con)
            outfile = os.path.join(output_dir,"{}.csv".format(make_avg_filename(os.path.splitext(os.path.basename(infile))[0])))
            write_averages(sqlite_con, outfile)
            print("\n 	Wrote averages to {} for infile {}".format(outfile, infile))
            dropdb(sqlite_con)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--results_directory', required=True, default=None)
    parser.add_argument('-o', '--output_dir', required=True, default=None)
    args = parser.parse_args()

    t = Timer()
    process(args.results_directory, args.output_dir)
    print("\nCompleted processing all files in {})".format(t.elapsed()))
