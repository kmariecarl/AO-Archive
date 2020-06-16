#------------------------------------------
#     INSTRUCTIONS FOR USE
#-----------------------------------------
# Concatenates accessibility results across hours (i.e join 7-8, and 8-9) and accommodates multiple jobs fields
# 1. Create a zip file of the accessibility results and place within a folder called "Access_Results"
# 2. Create an empty folder called "Access_Results_Avg"
# 3. Navigate in the command line to the directory where this program and two folders are located.
# 4. Run the command "python average_transit_local.py -r input -o output
# 5. "input" is the name of folder where the zipped access results are.
# 6. "output" is the name of the empty "Access_Results_Avg" folder.
#

import csv
import sqlite3
import os
import argparse
import datetime
import glob
from progress import bar
from zipfile import ZipFile
import io

class Timer:

    def __init__(self):
        self.start_time = datetime.datetime.now()
    
    def elapsed(self):
        return datetime.datetime.now() - self.start_time

class ProgressBar:
    def __init__(self, lines):
        self.bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=lines)
    def add_progress(self):
        self.bar.next()
    def end_progress(self):
        self.bar.finish()


def createdb(con, fieldnames):
    print('Creating DB Table')
    cur = con.cursor()
    fieldnames = fieldnames[3:] # Remove label, deptime, threshold and explicitly write them below
    # cur.execute("CREATE TABLE data (label text, deptime integer, threshold integer, {} integer)".format(JOBS_LABEL))
    fieldname_list = []
    for f in fieldnames:
        fieldname_list.append(f + " integer")
    fieldname_str = ""
    for f2 in fieldname_list:
        fieldname_str += f2
        fieldname_str += ","
    fieldname_str = fieldname_str[:-1]
    print("fieldname_str: ", fieldname_str)
    print(f"CREATE TABLE data (label text, deptime integer, threshold integer,{fieldname_str} )")
    # cur.execute(f"CREATE TABLE data (label text, deptime integer, threshold integer, {fieldname_str} )")
    cur.execute(f"CREATE TABLE data (label text, deptime integer, threshold integer,{fieldname_str} )")
    con.commit()

    #wC000 integer, wCA01 integer, wCA02 integer, wCA03 integer, wCE01 integer, wCE02 integer, wCE03 integer, wCNS01 integer, wCNS02 integer, wCNS03 integer, wCNS04 integer, wCNS05 integer, wCNS06 integer, wCNS07 integer, wCNS08 integer, wCNS09 integer, wCNS10 integer, wCNS11 integer, wCNS12 integer, wCNS13 integer, wCNS14 integer, wCNS15 integer, wCNS16 integer, wCNS17 integer, wCNS18 integer, wCNS19 integer,wCNS20 integer, wCR01 integer, wCR02 integer, wCR03 integer, wCR04 integer, wCR05 integer, wCR07 integer, wCT01 integer, wCT02 integer, wCD01 integer, wCD02 integer, wCD03 integer, wCD04 integer, wCS01 integer, wCS02 integer, wCFA01 integer, wCFA02 integer, wCFA03 integer, wCFA04 integer, wCFA05 integer, wCFS01 integer, wCFS02 integer, wCFS03 integer, wCFS04 integer, wCFS05 integer

def dropdb(con):
    cur = con.cursor()
    cur.execute("DROP TABLE data;")
    con.commit()

def extract_data_and_load(infile, con):
    print('Extacting data from zipfile')
    cur = con.cursor()
    zf = ZipFile(infile)
    contents = zf.namelist()
    header = None
    for name in contents:
        if name[-4:] == '.csv':
            r = (zf.open(name))
            i = io.TextIOWrapper(r, encoding='UTF-8', newline=None)
            print(i)
            # p = ProgressBar(os.system(f"wc -l < {i}"))
            reader = csv.reader(i)
            header = next(reader, None)
            print("Header row \n: ", header)
            # Create string of ? for the number of fields
            insert_row = ""
            for x in header:
                insert_row += " ?,"
            insert_row = insert_row[1:-1]

            createdb(con, header)

            for row in reader:
                # print(row)
                # print("\n")
                # data = [x[1] for x in row]
                # print(data)
                # data = [row["label"], row["deptime"], row["threshold"], row["wC000"], row["wCA01"], row["wCA02"],
                #         row["wCA03"], row["wCE01"], row["wCE02"], row["wCE03"], row["wCNS01"], row["wCNS02"],
                #         row["wCNS03"], row["wCNS04"], row["wCNS05"], row["wCNS06"], row["wCNS07"], row["wCNS08"],
                #         row["wCNS09"], row["wCNS10"], row["wCNS11"], row["wCNS12"], row["wCNS13"], row["wCNS14"],
                #         row["wCNS15"], row["wCNS16"], row["wCNS17"], row["wCNS18"], row["wCNS19"], row["wCNS20"],
                #         row["wCR01"], row["wCR02"], row["wCR03"], row["wCR04"], row["wCR05"],
                #         row["wCR07"], row["wCT01"], row["wCT02"], row["wCD01"], row["wCD02"], row["wCD03"],
                #         row["wCD04"], row["wCS01"], row["wCS02"], row["wCFA01"], row["wCFA02"], row["wCFA03"],
                #         row["wCFA04"], row["wCFA05"], row["wCFS01"], row["wCFS02"], row["wCFS03"], row["wCFS04"],
                #         row["wCFS05"]]
                # cur.execute("""INSERT INTO data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row)
                cur.execute(f"""INSERT INTO data VALUES ({insert_row})""", row)
                # p.add_progress()
    con.commit()
    return header


def create_indices(con):
    print("Indexing data table...")
    cur = con.cursor()
    cur.execute("CREATE INDEX idx_label ON data (label);")
    cur.execute("CREATE INDEX idx_threshold ON data (threshold);")
    con.commit()

def averages(con, header):
    print("Calculating averages...")
    header = header[3:]
    avg_str = ""
    for x in header:
        avg_str += f" CAST(avg({x}) as integer),"
    avg_str = avg_str[1:-1]
    print("average string \n ", avg_str)
    cur = con.cursor()
    cur.execute(f"""SELECT label, threshold, {avg_str} FROM data GROUP BY label, threshold;""")
    # cur.execute("""SELECT label, threshold, CAST(avg(wC000) as integer), CAST(avg(wCA01) as integer), CAST(avg(wCA02) as integer), CAST(avg(wCA03) as integer), CAST(avg(wCE01) as integer), CAST(avg(wCE02) as integer), CAST(avg(wCE03) as integer),
    #  CAST(avg(wCNS01) as integer), CAST(avg(wCNS02) as integer), CAST(avg(wCNS03) as integer), CAST(avg(wCNS04) as integer), CAST(avg(wCNS05) as integer), CAST(avg(wCNS06) as integer), CAST(avg(wCNS07) as integer), CAST(avg(wCNS08) as integer), CAST(avg(wCNS09) as integer), CAST(avg(wCNS10) as integer),
    #  CAST(avg(wCNS11) as integer), CAST(avg(wCNS12) as integer), CAST(avg(wCNS13) as integer), CAST(avg(wCNS14) as integer), CAST(avg(wCNS15) as integer), CAST(avg(wCNS16) as integer), CAST(avg(wCNS17) as integer), CAST(avg(wCNS18) as integer), CAST(avg(wCNS19) as integer), CAST(avg(wCNS20) as integer),
    #  CAST(avg(wCR01) as integer), CAST(avg(wCR02) as integer), CAST(avg(wCR03) as integer), CAST(avg(wCR04) as integer), CAST(avg(wCR05) as integer), CAST(avg(wCR07) as integer), CAST(avg(wCT01) as integer), CAST(avg(wCT02) as integer),
    #  CAST(avg(wCD01) as integer), CAST(avg(wCD02) as integer), CAST(avg(wCD03) as integer), CAST(avg(wCD04) as integer), CAST(avg(wCS01) as integer), CAST(avg(wCS02) as integer), CAST(avg(wCFA01) as integer), CAST(avg(wCFA02) as integer), CAST(avg(wCFA03) as integer), CAST(avg(wCFA05) as integer),
    #  CAST(avg(wCFS01) as integer), CAST(avg(wCFS02) as integer), CAST(avg(wCFS03) as integer), CAST(avg(wCFS04) as integer), CAST(avg(wCFS05) as integer)
    #  FROM data GROUP BY label, threshold;""")
    data = cur.fetchall()
    print("THIS IS THE DATA: \n", data)  # currently a list of tuples, tuples that are really long
    return data

def write_averages(con, infile, header):
    print("Header: \n", header)
    header2 = header.copy()
    create_indices(con)
    data = averages(con, header2)
    with open(infile, "w") as outputfile: #, newline='' , encoding='utf-8'
        writer = csv.writer(outputfile)
        header.remove("deptime")
        print(header)
        # header = ["label","threshold","wC000","wCA01","wCA02","wCA03","wCE01","wCE02","wCE03",
        #           "wCNS01","wCNS02","wCNS03","wCNS04","wCNS05","wCNS06","wCNS07","wCNS08","wCNS09","wCNS10",
        #           "wCNS11","wCNS12","wCNS13","wCNS14","wCNS15","wCNS16","wCNS17","wCNS18","wCNS19","wCNS20",
        #           "wCR01","wCR02","wCR03","wCR04","wCR05","wCR07","wCT01","wCT02","wCD01","wCD02","wCD03","wCD04",
        #           "wCS01","wCS02","wCFA01","wCFA02","wCFA03","wCFA04","wCFA05",
        #           "wCFS01","wCFS02","wCFS03","wCFS04","wCFS05"]
        writer.writerow(header)
        for row in data:
            writer.writerow(row)
        # writer.writerows(data)

def make_avg_filename(infile):
    return "1-{}-avg".format(infile)

def process(p, results_directory, output_dir):

    zippaths = glob.glob(os.path.join(results_directory,'*.zip'))
    numfiles = len(zippaths)
    print("\nThe total number of files to process is {}".format(numfiles))

    for infile in zippaths:
        print('Processing averages for file {}...'.format(infile))
        with sqlite3.connect(":memory:") as sqlite_con:
            # createdb(sqlite_con)
            p.add_progress()
            header = extract_data_and_load(infile, sqlite_con)
            p.add_progress()
            outfile = os.path.join(output_dir,"{}.csv".format(make_avg_filename(os.path.splitext(os.path.basename(infile))[0])))
            write_averages(sqlite_con, outfile, header)
            p.add_progress()
            print("\n 	Wrote averages to {} for infile {}".format(outfile, infile))
            dropdb(sqlite_con)
            p.add_progress()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--results_directory', required=True, default=None)
    parser.add_argument('-o', '--output_dir', required=True, default=None)
    args = parser.parse_args()


    t = Timer()
    p = ProgressBar(4)
    # fieldnames = input("Fieldnames: Copy propertyFields from config.json file, use quotes and commas: ")
    # print("\n", fieldnames)
    process(p, args.results_directory, args.output_dir)
    # print("\nCompleted processing all files in {})".format(t.elapsed()))
