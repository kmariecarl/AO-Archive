# This file connects with views created in aodb views schema and computes the metro-wide weighted accessibility for
# various LEHD variables, travel time thresholds, and modes.

# This program was used in the AccessEquity work submitted to WSTLUR and presented at TRB 2020

# Another program called aodbWorkerStats looks similar to this but simply sums the number of workers of each group
# per state, CBSA, etc.

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

def calcWWAvg(bar, cur):

    # User input for zone and zone id
    rac_wac_flag = input("Type 'r' to calculate RAC to w_c000, type 'rw' to calculate RAC to WAC: ")

    rac_list = ["C000", "CA01", "CA02", "CA03", "CE01", "CE02", "CE03", "CR01", "CR02", "CR03",
                   "CR04", "CR05", "CR07", "CT01", "CT02", "CD01", "CD02", "CD03", "CD04", "CS01", "CS02"]


    #flag = input("Calculate worker-weighted averages for nation? Type y/n")
    if input("Calculate worker-weighted averages for nation? Type y/n: ") == "y":

        fieldnames = ["tm_wt", "avg"]
        writer = mod.mkDictOutput('{}_NATION_TM_WT_AVG'.format(MODE), fieldname_list=fieldnames)

        states = selectDistinct()
        print("Number of states", len(states))

        # Begin iterating through states
        nation = {}
        for s in states:
            print("State = {}".format(s))
            nation[s] = runRacQueries("states", "id", s, cur, bar)
            #----BEEP----
            os.system(f"say {s}")
            bar.next()
            print(nation)

        # Begin averaging
        print("Begin averaging of national dictionary")
        avg_dict = {}
        for rac in rac_list:
            avg_list = []
            for s in states:
                avg_list.append(nation[s]["tm_wt"][rac])
            list_sum = sum(avg_list)
            rac_avg = list_sum/len(avg_list)
            avg_dict[rac] = rac_avg
            print("Average for rac {} ".format(rac), rac_avg)

        for key, value in avg_dict.items():
            entry = {"tm_wt": key, "avg": round(value, 0)}
            writer.writerow(entry)



    else:
        zone = input("Enter aggregation level as either: states, cbsas, mn_ctus: ")
        zone_id_name = input("Enter field name for zone identifier, i.e. state = id, cbsas = id, mn_ctus = gnis_id: ")
        zone_id = input("Enter the zone id associated with the aggregation level: MN = 27 MPLS MPO = 33460 "
                        "Minneapolis = 2395345 Saint Paul = 2396511: ")

        if rac_wac_flag == 'rw':
            writer = writerRW(zone, zone_id)
            runRacWacQueries(cur, writer, bar)
        else:
            writer = writerR(zone, zone_id)
            results_dict = runRacQueries(zone, zone_id_name, zone_id, cur, bar)
            writeResults(results_dict, writer)

def selectDistinct():
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
            if (s < 10):
                num_str = "0"+str(s)
            else:
                num_str = str(s)

            states.append(num_str)
        print('these are the unique state identifiers: ', states)
        return states

    except:
        print('Something went wrong with unique state identifiers')



def writerRW(zone, zone_id):
    fieldnames1 = ['threshold', 'rac', 'w_c000', 'w_cns01', 'w_cns02', 'w_cns03', 'w_cns04', 'w_cns05',
                   'w_cns06',
                   'w_cns07', 'w_cns08', 'w_cns09', 'w_cns10',
                   'w_cns11', 'w_cns12', 'w_cns13', 'w_cns14', 'w_cns15', 'w_cns16', 'w_cns17', 'w_cns18',
                   'w_cns19',
                   'w_cns20']
    writer = mod.mkDictOutput('{}_RAC_WAC_MATRIX_{}_{}'.format(MODE, zone, zone_id), fieldname_list=fieldnames1)
    print("Made empty output file")
    return writer


def writerR(zone, zone_id):
    fieldnames2 = ["threshold", "C000", "CA01", "CA02", "CA03", "CE01", "CE02", "CE03", "CR01", "CR02", "CR03",
                   "CR04", "CR05", "CR07", "CT01", "CT02", "CD01", "CD02", "CD03", "CD04", "CS01", "CS02"]
    writer = mod.mkDictOutput('{}_RAC_WT_{}_{}'.format(MODE, zone, zone_id), fieldname_list=fieldnames2)
    print("Made empty output file")
    return writer


# Creates a table of sector-worker-weighted accessibility for 1 travel time thrshold, includes different job sectors,
# but not travel time weighted measure. Works 10/31/19
def runRacWacQueries(cur, writer, bar):
    # 90 minutes not available in the transit 2017 dataset
    # thresh = [1800] fix to be all thresholds
    print("Thresholds included in this analysis: ", thresh)
    # the rac_list is taken from the general lehd schema, not access results tables
    rac_list = ["C000", "CA01", "CA02", "CA03", "CE01", "CE02", "CE03", "CR01", "CR02", "CR03", "CR04", "CR05",
                "CR07", "CT01", "CT02", "CD01", "CD02", "CD03", "CD04", "CS01", "CS02"]
    print("Worker groups included in this analysis: ", rac_list)

    # Work sector LEHD variables
    wac_list = ['w_c000', 'w_cns01', 'w_cns02', 'w_cns03', 'w_cns04', 'w_cns05', 'w_cns06', 'w_cns07', 'w_cns08', 'w_cns09', 'w_cns10',
                'w_cns11', 'w_cns12', 'w_cns13', 'w_cns14', 'w_cns15', 'w_cns16', 'w_cns17', 'w_cns18', 'w_cns19','w_cns20']
    print("Job sectors included in this analysis: ", wac_list)
    wac_dict = {}

    for i in thresh:
        for j in rac_list:
            for k in wac_list:
                bar.next()
                result = makeQuery(i, j, k, cur)
                wac_dict['{}'.format(k)] = result
            rac_dict = {'threshold': i, 'rac': j}
            entry = {}
            entry.update(rac_dict)
            entry.update(wac_dict)
            writer.writerow(entry)

# Creates a table of all-worker-weighted accessibility by travel threshold and time-weighted, does not assess different job sectors
def runRacQueries(zone, zone_id_name, zone_id, cur, bar):
    thresh = [600, 1200, 1800, 2400, 3000, 3600]
    print("Thresholds included in this analysis: ", thresh)
    
    rac_list = ["C000", "CA01", "CA02", "CA03", "CE01", "CE02", "CE03", "CR01", "CR02", "CR03", "CR04", "CR05",
                "CR07", "CT01", "CT02", "CD01", "CD02", "CD03", "CD04", "CS01", "CS02"]

    print("Worker groups included in this analysis: ", rac_list)
    
    wac = "w_c000"
    print("Job sectors included in this analysis: ", wac)

    # Results_dict of form: {600: {C000: 25,000, CA01: 24,500, CA02: 22,000}, 1200: {}}
    results_dict = {}
    # Make results dictionary, very easy
    for t in thresh:
        results_dict[t] = {}
        for rac in rac_list:
            result = makeQuery(zone, zone_id_name, zone_id, t, rac, wac, cur)
            results_dict[t][rac] = result

    results_dict["tm_wt"] = {}

    # Iterate through results dict to computed time-weighted value for each LEHD variable
    for rac in rac_list:
        time_wt_list = []
        result = 0
        for t in thresh:
            if not time_wt_list:
                result = results_dict[t][rac]
                diff = result
                exp = (math.exp(-0.08 * (t/60)))
                term = diff * exp
                time_wt_list.append(term)
            else:
                previous = result
                result = results_dict[t][rac]
                diff = result - previous
                exp = (math.exp(-0.08 * (t/60)))
                term = diff * exp
                time_wt_list.append(term)
            tm_wt = round(sum(time_wt_list), 1)
            print('this is the output for thresh {}, RAC {}, WAC {}: '.format("tm_wt", rac, wac), tm_wt)
            #sys.stdout.write('\a')
            results_dict["tm_wt"][rac] = tm_wt


    return results_dict



def writeResults(results_dict, writer):

    # Write out results dictionary
    #Breaks out nested dictionary, each threshold is a row
    for thresh, rac_dict in results_dict.items():
        #print("This is the rac_dict: ", rac_dict)
        # Create two dictionaries and combine to one then write out using DictWriter
        entry = {}
        dict1 = {"threshold": thresh}
        entry.update(dict1)
        entry.update(rac_dict)
        writer.writerow(entry)


def makeQuery(zone, zone_id_name, zone_id, t, rac, wac, cur):
    query = """with blocks as (select b.id, b.geom
		  from zones.blocks b, zones.{} c
		  where ST_Covers(c.geom, b.centroid)
		  and c.{} = '{}'
		  and b.aland > 0  
		  ),
		results as (select blockid, {} as jobs from {}.{}
		 where blockid in (select id from blocks)
		  and threshold = {}
		 ),
		lehd as (select geocode, {} as workers
		from {}.{}
		where geocode in (select id from blocks)
		),
		data as (

		select b.id, coalesce(r.jobs, 0) as jobs, coalesce(l.workers, 0) as workers
		from blocks b
		left join results r
		on b.id = r.blockid
		left join lehd l
		on b.id = l.geocode
		)

		select case when sum(workers) = 0 then 0 else round(sum(jobs::bigint * workers::bigint) / sum(workers::bigint)) end
		from data as weighted;
		; """

    try:
        cur.execute(query.format(zone, zone_id_name, zone_id, wac, RESULTS_SCHEMA, MODE, t, rac, LEHD, RAC))
        out = cur.fetchall()
        result = int(out[0][0])
        print('this is the output for thresh {}, RAC {}, WAC {}: '.format(t, rac, wac), result)

        return result

    except:
        print('Something went wrong')
        print('zone {} thresh {}, RAC {}: '.format(zone_id, t, rac))


if __name__ == '__main__':
    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    t0 = time.time()
    print("Started at:", readable)
    bar = bar.Bar(message='Processing', fill='@', suffix='%(percent)d%%', max=160)

    # Parameterize file paths
    parser = argparse.ArgumentParser()

    parser.add_argument('-db', '--DB_NAME', required=True, default=None)  # ENTER AS aodb

    parser.add_argument('-lehd_schema', '--LEHD_SCHEMA', required=True, default=None)  # ENTER AS lehd
    parser.add_argument('-wac', '--WAC_TABLE_NAME', required=True, default=None)  # i.e. wac2015
    parser.add_argument('-rac', '--RAC_TABLE_NAME', required=True, default=None)  # i.e. rac2015

    parser.add_argument('-results_schema', '--RESULTS_SCHEMA', required=True, default=None)  # ENTER AS results or tomtomresults_2017
    parser.add_argument('-mode', '--MODE_TABLE_NAME', required=True, default=None)  # i.e. transit_avg_07_08_2017, bike_2017_lts1, data_am_avg


    args = parser.parse_args()
    DB_NAME = args.DB_NAME

    LEHD = args.LEHD_SCHEMA
    WAC = args.WAC_TABLE_NAME
    RAC = args.RAC_TABLE_NAME

    RESULTS_SCHEMA = args.RESULTS_SCHEMA
    MODE = args.MODE_TABLE_NAME




    try:
        con = psycopg2.connect("dbname = '{}' user='aodb_readonly' host='160.94.200.145' password='yEQX6Jg53E9nDdy'".format(DB_NAME)) #replace host='localhost' with ip address of instance when running externally
        con.set_session(autocommit=True)
    except:
        print('I am unable to connect to the database')


    #Initiate cursor object on db
    cur = con.cursor()


    calcWWAvg(bar, cur)

    print("Finished at:", time.ctime(time.time()))

        
    bar.finish()
    cur.close()







