# This file connects with the aodb and results schemas and computes the dual accessibility given a threshold number of
# jobs to reach.

# Example usage: 1.

# To test a query without pulling all blocks, use limit 100 after the end of the blocks subquery like...
# and c.id = '33460'
# and b.aland > 0
# limit 100

# Formatted WAC/RAC Fieldnames

# fieldnames = ['threshold', 'rac', 'w_c000', 'w_ca01', 'w_ca02', 'w_ca03', 'w_ce01', 'w_ce01', 'w_ce03', 'w_cns01',
#               'w_cns02', 'w_cns03', 'w_cns04', 'w_cns05', 'w_cns06', 'w_cns07', 'w_cns08', 'w_cns09', 'w_cns10',
#               'w_cns11', 'w_cns12', 'w_cns13', 'w_cns14', 'w_cns15', 'w_cns16', 'w_cns17', 'w_cns18', 'w_cns19',
#               'w_cns20', 'w_cr01', 'w_cr02', 'w_cr03', 'w_cr04', 'w_cr05', 'w_cr07', 'w_ct01', 'w_ct02', 'w_cd01', 'w_cd02', 'w_cd03',
#               'w_cd04', 'w_cs01', 'w_cs02', 'w_cfa01', 'w_cfa02', 'w_cfa03', 'w_cfa04', 'w_cfa05', 'w_cfs01', 'w_cfs02', 'w_cfs03', 'w_cfs04', 'w_cfs05']

# fieldnames2 = ["threshold", "C000", "CA01", "CA02", "CA03", "CE01", "CE02", "CE03", "CNS01", "CNS02",
#                "CNS03", "CNS04", "CNS05", "CNS06",
#                "CNS07", "CNS08", "CNS09", "CNS10", "CNS11", "CNS12", "CNS13", "CNS14", "CNS15", "CNS16",
#                "CNS17", "CNS18",
#                "CNS19", "CNS20", "CR01", "CR02", "CR03", "CR04", "CR05", "CR07", "CT01", "CT02", "CD01",
#                "CD02",
#                "CD03", "CD04", "CS01", "CS02"]

#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import psycopg2
import time
from progress import bar
import math


#################################
#           FUNCTIONS           #
#################################

def calcDualAccess(cur):

    zone = input("Enter aggregation level as either: states, cbsas, mn_ctus: ")
    zone_id_name = input("Enter field name for zone identifier, i.e. state = id, cbsas = id, mn_ctus = gnis_id: ")
    zone_id = input("Enter the zone id associated with the aggregation level: MN = 27 MPLS MPO = 33460 "
                    "Minneapolis = 2395345 Saint Paul = 2396511: ")
    block_id_name = input("Enter block identifier, such as blockid for the transit results: ")
    jobs_limit = int(input("Enter the threshold number of jobs (integer): "))

    writer = writerR(zone, zone_id)
    block_list = makeBlockList(zone, zone_id_name, zone_id)
    dual_dict = runDualQueries(block_id_name, block_list, jobs_limit, cur)
    writeResults(dual_dict, writer)



def writerR(zone, zone_id):
    fieldnames2 = ["GEOID10", "dual_time"]
    writer = mod.mkDictOutput('{}_dual_access_{}_{}'.format(MODE, zone, zone_id), fieldname_list=fieldnames2)
    print("Made empty output file")
    return writer

def makeBlockList(zone, zone_id_name, zone_id):
    query = """select b.id, b.geom
          from zones.blocks b, zones.{} c
          where ST_Covers(c.geom, b.centroid)
          and c.{} = '{}'
          and b.aland > 0;"""

    try:
        cur.execute(query.format(zone, zone_id_name, zone_id))
        out = cur.fetchall()
        block_list = []
        for i in out:
            block_list.append(str(i[0]))

        return block_list

    except:
        print('Something went wrong')




# Creates a table of all-worker-weighted accessibility by travel threshold and time-weighted, does not assess different job sectors
def runDualQueries(block_id_name, block_list, jobs_limit, cur):

    wac = "w_c000"
    print("Job sectors included in this analysis: ", wac)

    # Results_dict of form: {GEOID10: 300, GEOID10: 600, GEOID10: 300}
    dual_dict = {}
    for id in block_list:
        count = 300
        access = makeQuery(wac, block_id_name, id, count, cur)

        if access == None:
            access = 0
            print("signal 1")
        while access < jobs_limit and count <= 3600:
            count = count + 300
            access = makeQuery(wac, block_id_name, id, count, cur)
            if access == None:
                access = 0
                print("signal 2")
        dual_dict[id] = count
    print(dual_dict)
    return dual_dict


def writeResults(results_dict, writer):
    # Write out results dictionary
    for key, item in results_dict.items():
        entry = {"GEOID10": key, "dual_time": item}
        writer.writerow(entry)






def makeQuery(wac, block_id_name, id, thresh, cur):
    query = """select {} from {}.{}
                where {} like '{}' and threshold = {};"""

    try:
        cur.execute(query.format(wac, RESULTS_SCHEMA, MODE, block_id_name, id, thresh))
        out = cur.fetchall()
        result = int(out[0][0])
        #print('this is the output for block {} thresh {}: '.format(id, thresh), result)

        return result

    except:
        print('Something went wrong')


if __name__ == '__main__':
    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    t0 = time.time()
    print("Started at:", readable)
    bar = bar.Bar(message='Processing', fill='@', suffix='%(percent)d%%', max=160)

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  # ENTER AS aodb

    parser.add_argument('-results_schema', '--RESULTS_SCHEMA', required=True,
                        default=None)  # ENTER AS results or tomtomresults_2017
    parser.add_argument('-mode', '--MODE_TABLE_NAME', required=True,
                        default=None)  # i.e. transit_avg_07_08_2017, bike_2017_lts1, data_am_avg

    args = parser.parse_args()
    DB_NAME = args.DB_NAME

    # LEHD = args.LEHD_SCHEMA
    # WAC = args.WAC_TABLE_NAME
    # RAC = args.RAC_TABLE_NAME

    RESULTS_SCHEMA = args.RESULTS_SCHEMA
    MODE = args.MODE_TABLE_NAME

    try:
        con = psycopg2.connect(
            "dbname = '{}' user='aodb_readonly' host='160.94.200.145' password='yEQX6Jg53E9nDdy'".format(
                DB_NAME))  # replace host='localhost' with ip address of instance when running externally
        con.set_session(autocommit=True)
    except:
        print('I am unable to connect to the database')

    # Initiate cursor object on db
    cur = con.cursor()

    calcDualAccess(cur)

    print("Finished at:", time.ctime(time.time()))

    bar.finish()
    cur.close()
