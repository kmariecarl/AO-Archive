# This script reads in file output from workWeightAccess -group and performs a variety of ranking and formatting functions
# Pseudo code
# read in file
# read in rac file
# ask user what function they want to perform (top n by pct time weight, top by population)

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

from numpy import average
import sys
import argparse
import csv
from progress import bar
import pandas as pd

from myToolsPackage import matrixLinkModule as mod
from collections import defaultdict, OrderedDict

# --------------------------
#       FUNCTIONS
# --------------------------

def main():
    start, curtime = mod.startTimer()


    parser = argparse.ArgumentParser()
    parser.add_argument('-access', '--ACCESS_FILE', required=True, default=None)
    parser.add_argument('-rac', '--RAC_FILE', required=False, default=None) #/Users/kristincarlson/Dropbox/DataSets/WAC_RAC/2015/CTU_Total_RAC.csv
    parser.add_argument('-ctu', '--CTU_FILE', required=False, default=None) # /Users/kristincarlson/Dropbox/AO_Projects/met_council_analyses/2_transit/scenarios/GEOID10_GNISID.csv
    parser.add_argument('-ctu_names', '--CTU_NAMES_FILE', required=False, default=None) #/Users/kristincarlson/Dropbox/AO_Projects/met_council_analyses/2_transit/scenarios/CTU_NAMES.csv
    args = parser.parse_args()

    if '-rac' in sys.argv:

        # Former code allowed user input, not fieldnames are set

        # field = input("Type field name to rank observations on, i.e. (pctchgtmwt): ") or 'pctchgtmwt'
        # n = int(input(f"Return the top <N> observations of field {field}: Type N "))

        # field2 = input("Type field of RAC data to rank observations on, i.e. (TotalRAC2015): ") or 'TotalRAC2015'
        # n2 = int(input(f"Return the top <N> observations of field {field2}: Type N: "))

        # field3 = input("Type field name of group identifier (CTU): ") or 'CTU'

        field = 'pctchgtmwt'
        n = 20

        field2 = 'TotalRAC2015'
        n2 = 10

        field3 = 'CTU'

        # DF object: Access file
        access = pd.read_csv(args.ACCESS_FILE)
        print("First 10 rows of accessibility file \n", access.head())
        print("Data types \n", access.dtypes)

        # DF object: CTU name file
        names = pd.read_csv(args.CTU_NAMES_FILE)
        print("First 10 rows of CTU name file  \n", names.head())
        print("Data types \n", names.dtypes)

        # DF object: RAC file
        rac = pd.read_csv(args.RAC_FILE)
        print("First 10 rows of RAC name file  \n", rac.head())
        print("Data types \n", rac.dtypes)

        # Join ctu names to access results
        access_name = pd.merge(access, names, on='GNIS_ID', how='left')

        # Pivot df, drop GNIS_ID
        access_name_pivot = access_name.pivot(index='CTU', columns='measure', values='value')
        print("First 10 rows of pivoted table \n", access_name_pivot.head())

        # Solution found here: https://www.ritchieng.com/pandas-selecting-multiple-rows-and-columns/
        access_name_subset = access_name_pivot.loc[:, ['abschg900', 'pctchg900', 'abschg1800', 'pctchg1800', 'abschg2700', 'pctchg2700',
                                          'abschg3600', 'pctchg3600','pctchgtmwt', 'abschgtmwt']]
        print("First 10 rows of pivot table subset \n", access_name_subset.head())

        # --------Get N by Time-weighted Percent Change-------
        getNByPctChg(access_name_subset, field, n)


        # --------Begin RAC Work------------

        # Join CTU names to RAC df
        rac_name = pd.merge(names, rac, on='GNIS_ID', how='left')
        print("First 10 rows of rac_name df \n", rac_name.head())

        # Get to N by Time-weighted Percent Change
        getNByPop(rac_name, access_name_subset, field2, n2)

        # --------Begin Alphabetical Work------------
        getAllByAlpha(access_name_subset, field3)

    # Default is create tables for metro-wide accessibility
    else:
        #name = input("Type 'all' or 'halfm' to append to file name ")

        # DF object: Access file
        access = pd.read_csv(args.ACCESS_FILE)
        print("First 10 rows of accessibility file \n", access.head())
        print("Data types \n", access.dtypes)
        access['id2'] = access.index
        access.set_index('measure', inplace=True)

        # # Pivot df
        # access_pivot = access.pivot(index= 1, columns='measure', values='value')
        # print("First 10 rows of pivoted table \n", access_pivot.head())

        # Transpose DF
        access_t = access.transpose()
        print("First 10 rows of transposed accessibility file \n", access_t.head())



        # Solution found here: https://www.ritchieng.com/pandas-selecting-multiple-rows-and-columns/
        access_subset = access_t.loc[:,
                             ['bs_900', 'alt_900', 'abschg900', 'pctchg900',
                              'bs_1800', 'alt_1800', 'abschg1800', 'pctchg1800',
                              'bs_2700', 'alt_2700', 'abschg2700', 'pctchg2700',
                              'bs_3600', 'alt_3600','abschg3600', 'pctchg3600',
                              'wt_bs', 'wt_alt', 'pctchgtmwt', 'abschgtmwt']]
        access_subset.fillna(0, inplace=True)
        print("First 10 rows of pivot table subset \n", access_subset.head())

        list_of_strings = []
        count = 0
        for index, row in access_subset.iterrows():
            # Drop last row which is the set of old indicies
            if count > 0:
                break
            else:
                count = count + 1
                out = myRegFormat(int(row['bs_900'])) + "* " + myRegFormat(
                    int(row['alt_900'])) + "* " + myAbsFormat(int(row['abschg900'])) + " (" + myPctFormat(
                    row['pctchg900']) + ")" + "* " \
                      + myRegFormat(int(row['bs_1800'])) + "* " + myRegFormat(
                    int(row['alt_1800'])) + "* " + myAbsFormat(int(row['abschg1800'])) + " (" + myPctFormat(row['pctchg1800']) + ")" + "* " \
                      + myRegFormat(int(row['bs_2700'])) + "* " + myRegFormat(
                    int(row['alt_2700'])) + "* " + myAbsFormat(int(row['abschg2700'])) + " (" + myPctFormat(row['pctchg2700']) + ")" + "* " \
                      + myRegFormat(int(row['bs_3600'])) + "* " + myRegFormat(
                    int(row['alt_3600'])) + "* " + myAbsFormat(int(row['abschg3600'])) + " (" + myPctFormat(row['pctchg3600']) + ")" + "* " \
                      + myRegFormat(int(row['wt_bs'])) + "* " + myRegFormat(
                    int(row['wt_alt'])) + "* " + myAbsFormat(int(row['abschgtmwt'])) + " (" + myPctFormat(row['pctchgtmwt']) + ")"
                print(out)
                list_of_strings.append(out)

        # Write results to file
        # Make the new file name derived from the input file name
        file_name = str(sys.argv[2]).replace(".csv", "")
        file_name2 = file_name.replace("3-", "4-")
        f = open(f"{file_name2}-table.csv", 'w')
        w = csv.writer(f, delimiter=',')
        header = ['Base900', 'Alt900', 'Chg900', 'Base1800', 'Alt1800', 'Chg1800', 'Base2700', 'Alt2700', 'Chg2700',
                  'Base3600', 'Alt3600', 'Chg3600', 'wt_base', 'wt_alt', 'wt_chg']
        w.writerow(header)
        w.writerows([x.split("*") for x in list_of_strings])
        f.close()



def getNByPctChg(df_subset, field, n):
    top_n = df_subset.nlargest(n, f'{field}')
    print("top_n: \n", top_n)

    list_of_strings = []

    for index, row in top_n.iterrows():
        out = index + "* " + myAbsFormat(int(row['abschg900'])) + " (" + myPctFormat(row['pctchg900']) + ")" + "* " \
              + myAbsFormat(int(row['abschg1800'])) + " (" + myPctFormat(row['pctchg1800']) + ")" + "* " \
              + myAbsFormat(int(row['abschg2700'])) + " (" + myPctFormat(row['pctchg2700']) + ")" + "* " \
              + myAbsFormat(int(row['abschg3600'])) + " (" + myPctFormat(row['pctchg3600']) + ")" + "* " \
              + myAbsFormat(int(row['abschgtmwt'])) + " (" + myPctFormat(row['pctchgtmwt']) + ")"
        print(out)
        list_of_strings.append(out)

    f = open(f"4-top{n}by{field}.csv", 'w')
    w = csv.writer(f, delimiter=',')
    header = ['CTU', '15 min', '30 min', '45 min', '60 min', 'Time-weighted']
    w.writerow(header)
    w.writerows([x.split("*") for x in list_of_strings])
    f.close()

def getNByPop(rac_name, access_name_subset, field, n):
    top_n = rac_name.nlargest(n, f'{field}')

    top_list = []
    for index, row in top_n.iterrows():
        top_list.append(row['CTU'])
        print("CTU: ", row['CTU'], " RAC: ", row['TotalRAC2015'])

    list_of_strings = []
    for item in top_list:
        for index, row in access_name_subset.iterrows():
            if index == item:
                out = index + "* " + myAbsFormat(int(row['abschg900'])) + " (" + myPctFormat(
                    row['pctchg900']) + ")" + "* " \
                      + myAbsFormat(int(row['abschg1800'])) + " (" + myPctFormat(row['pctchg1800']) + ")" + "* " \
                      + myAbsFormat(int(row['abschg2700'])) + " (" + myPctFormat(row['pctchg2700']) + ")" + "* " \
                      + myAbsFormat(int(row['abschg3600'])) + " (" + myPctFormat(row['pctchg3600']) + ")" + "* " \
                      + myAbsFormat(int(row['abschgtmwt'])) + " (" + myPctFormat(row['pctchgtmwt']) + ")"
                print(out)
                list_of_strings.append(out)
    f = open(f"4-top{n}by{field}.csv", 'w')
    w = csv.writer(f, delimiter=',')
    header = ['CTU', '15 min', '30 min', '45 min', '60 min', 'Time-weighted']
    w.writerow(header)
    w.writerows([x.split("*") for x in list_of_strings])
    f.close()

def getAllByAlpha(df_subset, field):
    all_alpha = df_subset.sort_values(f'{field}')
    print("Head of alphabetical results \n", all_alpha.head())
    # handle missing values
    all_alpha.fillna(0, inplace=True)

    list_of_strings = []

    for index, row in all_alpha.iterrows():
        out = index + "* " + myAbsFormat(int(row['abschg900'])) + " (" + myPctFormat(row['pctchg900']) + ")" + "* " \
              + myAbsFormat(int(row['abschg1800'])) + " (" + myPctFormat(row['pctchg1800']) + ")" + "* " \
              + myAbsFormat(int(row['abschg2700'])) + " (" + myPctFormat(row['pctchg2700']) + ")" + "* " \
              + myAbsFormat(int(row['abschg3600'])) + " (" + myPctFormat(row['pctchg3600']) + ")" + "* " \
              + myAbsFormat(int(row['abschgtmwt'])) + " (" + myPctFormat(row['pctchgtmwt']) + ")"
        print(out)
        list_of_strings.append(out)

    f = open(f"4-alphabetical_by_{field}.csv", 'w')
    w = csv.writer(f, delimiter=',')
    header = ['CTU', '15 min', '30 min', '45 min', '60 min', 'Time-weighted']
    w.writerow(header)
    w.writerows([x.split("*") for x in list_of_strings])
    f.close()

def myRegFormat(value):
    str1 = f'{value:,}'
    return str1

def myAbsFormat(value):
    str1 = f'{value:+,}'
    return str1

def myPctFormat(value):
    str1 = f'{value:+.2%}'
    return str1






if __name__ == '__main__':
    main()