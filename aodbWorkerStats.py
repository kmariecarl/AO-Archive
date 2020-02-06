# This program connects with aodb and schemas and computes the sum of workers in each LEHD group, along with summing
# the blocks in each geography

# Example usage:

# To test a query without pulling all blocks, use limit 100 after the end of the blocks subquery like...
# and c.id = '33460'
# and b.aland > 0
# limit 100

#Formatted WAC/RAC Fieldnames

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
import sys
import os

#################################
#           FUNCTIONS           #
#################################

# Create your own exception to call when something goes wrong with PostGreSQL query, inherit from ValueError
class IllegalArgumentError(ValueError):
    pass


def get_worker_stats(bar, cur):

    rac_list = ["C000", "CA01", "CA02", "CA03", "CE01", "CE02", "CE03", "CR01", "CR02", "CR03", "CR04", "CR05", "CR07",
                "CT01", "CT02", "CD01", "CD02", "CD03", "CD04", "CS01", "CS02"]

    print("Worker groups included: ", rac_list)
    # fieldnames = ["RAC", "SUM"]

    if input("Sum RAC and blocks for nation? Type y/n: ") == "y":


        # writer = mod.mkDictOutput('NATION_WORKER_STATS')

        states = select_distinct()
        print("Number of states", len(states))

        # Begin iterating through states
        nation = {}
        for r in rac_list:
            print("Rac = {}".format(r))
            nation[r] = national_sum_query(r, cur)
            # ----BEEP----
            os.system(f"say sum finished")
            bar.next()
            print(nation)

        # Sum all blocks
        query = """select count(*) from zones.blocks"""
        try:
            cur.execute(query)
            out = cur.fetchall()
            count = int(out[0][0])
            print('Total blocks in nation: ', count)
            # Add observation to dictionary
            nation["total_blocks"] = count

        except IllegalArgumentError:
            print('Something went wrong')

        with open('NATION_WORKER_STATS.csv', 'w') as f:

            for key in nation.keys():
                f.write(f"{key},{nation[key]}\n")

    else:
        zone = input("Enter aggregation level as either: states, cbsas, mn_ctus: ")
        zone_id_name = input("Enter field name for zone identifier, i.e. state = id, cbsas = id, mn_ctus = gnis_id: ")
        zone_id = input("Enter the zone id associated with the aggregation level: MN = 27 MPLS MPO = 33460 "
                        "Minneapolis = 2395345 Saint Paul = 2396511: ")

        results_dict = run_rac_queries(zone, zone_id_name, zone_id, cur)
        results_dict["total_blocks"] = blocks_in_zone(zone, zone_id_name, zone_id, cur)

        with open(f'{zone_id}_WORKER_STATS.csv', 'w') as f:

            for key in results_dict.keys():
                f.write(f"{key},{results_dict[key]}\n")


def select_distinct():
    query = """select distinct id from zones.states;"""
    try:
        cur.execute(query.format())
        out = cur.fetchall()
        states = []
        states_sorted = []
        for i in out:
            states_sorted.append(int(i[0]))
        states_sorted.sort()
        for s in states_sorted:
            if s < 10:
                num_str = "0"+str(s)
            else:
                num_str = str(s)

            states.append(num_str)
        print('these are the unique state identifiers: ', states)
        return states

    except IllegalArgumentError:
        print('Something went wrong with unique state identifiers')

# Sum the rac fields across the nation
def national_sum_query(rac, cur):
    query = """select sum ({}) from {}.{} as total"""
    try:
        cur.execute(query.format(rac, LEHD, RAC))
        out = cur.fetchall()
        result = int(out[0][0])
        print('Total jobs for rac {}: '.format(rac), result)
        return result

    except IllegalArgumentError:
        print('Something went wrong')
        print('RAC {}: '.format(rac))

# Returns the sum of workers for each RAC field and within the specified zone
def run_rac_queries(zone, zone_id_name, zone_id, cur):
    
    rac_list = ["C000", "CA01", "CA02", "CA03", "CE01", "CE02", "CE03", "CR01", "CR02", "CR03", "CR04", "CR05",
                "CR07", "CT01", "CT02", "CD01", "CD02", "CD03", "CD04", "CS01", "CS02"]
    
    wac = "w_c000"
    print("Job sectors included in this analysis: ", wac)

    # Results_dict of form: {C000: 25,000, CA01: 24,500, CA02: 22,000}
    results_dict = {}
    # Make results dictionary, very easy

    for rac in rac_list:
        result = make_query(zone, zone_id_name, zone_id, rac, cur)
        results_dict[rac] = result

    return results_dict

# Find the total number of workers of each type depending on the zone, slow method but works
def make_query(zone, zone_id_name, zone_id, rac, cur):
    query = """with blocks as (select b.id, b.geom
		  from zones.blocks b, zones.{} c
		  where ST_Covers(c.geom, b.centroid)
		  and c.{} = '{}'
		  and b.aland > 0  
		  ),
		lehd as (select geocode, {} as workers
		from {}.{}
		where geocode in (select id from blocks)
		)
        select sum(workers) from lehd; """

    try:
        cur.execute(query.format(zone, zone_id_name, zone_id, rac, LEHD, RAC))
        out = cur.fetchall()
        result = int(out[0][0])
        print('this is the output for {}: {}, rac {}: '.format(zone_id_name, zone_id, rac), result)

        return result

    except IllegalArgumentError:
        print('Something went wrong')
        print('zone {} RAC {}: '.format(zone_id, rac))


def blocks_in_zone(zone, zone_id_name, zone_id, cur):
    query = """with blocks as (select b.id, b.geom
    		  from zones.blocks b, zones.{} c
    		  where ST_Covers(c.geom, b.centroid)
    		  and c.{} = '{}'
    		  and b.aland > 0  
    		  )
    		  select count(*) from blocks; """

    try:
        cur.execute(query.format(zone, zone_id_name, zone_id))
        out = cur.fetchall()
        result = int(out[0][0])
        print('Number of blocks in {}: {}: '.format(zone_id_name, zone_id), result)

        return result

    except IllegalArgumentError:
        print('Something went wrong')
        print('zone {}: '.format(zone_id))


if __name__ == '__main__':
    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    t0 = time.time()
    print("Started at:", readable)
    bar = bar.Bar(message='Processing', fill='@', suffix='%(percent)d%%', max=160)

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  # ENTER AS aodb
    parser.add_argument('-lehd_schema', '--LEHD_SCHEMA', required=True, default="lehd")  # ENTER AS lehd
    parser.add_argument('-rac', '--RAC_TABLE_NAME', required=True, default="rac2015")  # i.e. rac2015

    args = parser.parse_args()

    DB_NAME = args.DB_NAME
    LEHD = args.LEHD_SCHEMA
    RAC = args.RAC_TABLE_NAME

    con = None
    try:
        con = psycopg2.connect("dbname = '{}' user='aodb_readonly' host='160.94.200.145' "
                               "password='yEQX6Jg53E9nDdy'".format(DB_NAME))  # replace host='localhost' with ip address
        # of instance when running externally
        con.set_session(autocommit=True)
    except:
        print('I am unable to connect to the database')


    # Initiate cursor object on db
    cur = con.cursor()

    # Begin operations

    get_worker_stats(bar, cur)

    print("Finished at:", time.ctime(time.time()))
        
    bar.finish()
    cur.close()







