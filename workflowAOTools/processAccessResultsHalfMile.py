# This script reads in processed_results file and a blocks within a half mile of transit stops file, and returns a file
# with only the blocks and results for the blocks within a half mile.

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

import sys
import argparse
import pandas as pd
from myToolsPackage import matrixLinkModule as mod


# --------------------------
#       FUNCTIONS
# --------------------------

def main():
    start, curtime = mod.startTimer()

    parser = argparse.ArgumentParser()
    parser.add_argument('-access', '--ACCESS_FILE', required=True, default=None)
    parser.add_argument('-blocks', '--BLOCK_ID_FILE', required=True, default=None) #/Users/kristincarlson/Dropbox/AO_Projects/met_council_analyses/2_transit/scenarios/HALFMILE_BLOCKS_FUNDED_BASELINE.csv
    args = parser.parse_args()

    # DF object: Access file
    access = pd.read_csv(args.ACCESS_FILE)
    print("First 10 rows of accessibility file \n", access.head())
    print("Data types \n", access.dtypes)

    # DF object: Block ID file
    blocks = pd.read_csv(args.BLOCK_ID_FILE)
    print("First 10 rows of block id file  \n", blocks.head())
    print("Data types \n", blocks.dtypes)

    # Join access results and block ids on GEOID10 using inner join, only retain matching records
    access_halfm = pd.merge(access, blocks, left_on='label', right_on='GEOID10', how='inner')
    print("First 10 rows of blocks within half mile accessibility results \n", access_halfm.head())

    # Drop extra columns
    access_halfm_write = access_halfm.drop(['GEOID10', 'Unnamed: 1'], axis=1)
    print("Header write to file \n")
    header = list(access_halfm_write.columns.values)
    print(header)

    # Statistics
    print("Rows in original accessibility file: ", len(access.index))
    print("Rows in half mile accessibility file: ", len(access_halfm_write))

    # Write results to file
    # Make the new file name derived from the input file name
    file_name = str(sys.argv[2]).replace(".csv", "")

    access_halfm_write.to_csv(f'{file_name}-halfm.csv', index=None, header=header)




if __name__ == '__main__':
    main()