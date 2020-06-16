# This program calculates the percent and absolute difference between dual accessibility scenarios.

# Example use: Dual access to 1–10 grocery destinations for scenario 1 compared to dual access to 1–10 grocery
# destinations for scenario 2 where the difference shows the change in minimum travel time needed to reach 1, 2, 3...
# destinations due to the transportation or land use change implemented in scenario 2.

# Data comes in the form:
# GEOID10,1,2,3,4,5,6,7,8,9,10
# 270530267143004,0,1,1,0,0,0,0,1,0,0,0,0

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------

from datetime import datetime
import argparse
import pandas as pd

# --------------------------
#       GENERAL FUNCTIONS
# --------------------------

if __name__ == '__main__':
    print(datetime.now())

    parser = argparse.ArgumentParser()
    parser.add_argument('-bs', '--BASE_FILE', required=True, default=None)  # Baseline scenario dual access file
    parser.add_argument('-updt', '--UPDATE_FILE', required=True,
                        default=None)  # Modified scneario dual access results file
    args = parser.parse_args()

    print("Processing results \n")

    base = pd.read_csv(args.BASE_FILE)
    base.sort_values("origin", axis=0)
    column_names = list(base.columns)
    column_names.remove("origin")
    print("Column names \n", column_names)


    updt = pd.read_csv(args.UPDATE_FILE)
    updt.sort_values("origin", axis=0)

    output = pd.DataFrame() # Create empty dataframe

    output["origin"] = base["origin"]  # Other than the origin ID field, all others are added iteratively
    for i in column_names:
        output[f"bs_{i}"] = base[f"{i}"]  # Add the original baseline value to the output
        output[f"updt_{i}"] = updt[f"{i}"]  # Add the alternative scenario values to the output
        # Abs change
        output[f"abschg{i}"] = updt[f"{i}"] - base[f"{i}"]  # Take the difference of the scenarios
        output.loc[output[f'abschg{i}'] < -5401, f'abschg{i}'] = -6000  # Fill in where tt went from maxtime to reachable
        # Percent change
        output[f"pctchg{i}"] = (updt[f"{i}"] - base[f"{i}"])/base[f"{i}"]  # Take the percent difference
        output.loc[output[f'pctchg{i}'] < -0.99999, f'pctchg{i}'] = -1  # Fill in where tt went from maxtime to reachable



    file_name_base = args.BASE_FILE.replace(".csv", "")
    file_name_updt = args.UPDATE_FILE.replace(".csv", "")
    output.to_csv(f"dual_access_change_{datetime.now()}.csv")  # Output file name


