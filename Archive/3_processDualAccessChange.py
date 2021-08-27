# This program calculates the percent and absolute difference between dual accessibility scenarios.

# Example use: Dual access to 1–10 grocery destinations for scenario 1 compared to dual access to 1–10 grocery
# destinations for scenario 2 where the difference shows the change in minimum travel time needed to reach 1, 2, 3...
# destinations due to the transportation or land use change implemented in scenario 2.

# Data comes in the form:
# BlockID,1,2,3,4,5,6,7,8,9,10
# 270530267143004,0,1,1,0,0,0,0,1,0,0,0,0

# This program assumes that the number of rows in the base and update files are the same.
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
    parser.add_argument('-bs_short', '--SHORT_BASE_NAME', required=True, default=None)
    parser.add_argument('-updt', '--UPDATE_FILE', required=True,
                        default=None)  # Modified scneario dual access results file
    parser.add_argument('-updt_short', '--SHORT_UPDT_NAME', required=True, default=None)
    parser.add_argument('-other_name', '--OTHER_NAME_TAG', required=False, default=None)
    args = parser.parse_args()

    print("Processing results \n")

    base = pd.read_csv(args.BASE_FILE)
    base.sort_values("origin", axis=0)
    column_names = list(base.columns)
    column_names.remove("origin")
    print("Column names \n", column_names)

    print("this is the base file: \n", base)
    updt = pd.read_csv(args.UPDATE_FILE)
    updt.sort_values("origin", axis=0)
    print("this is the updt file: \n", updt)
    output = pd.DataFrame() # Create empty dataframe

    output["origin"] = base["origin"]  # Other than the origin ID field, all others are added iteratively
    for i in column_names:
        output[f"bs_{i}"] = base[f"{i}"]  # Add the original baseline value to the output
        output.loc[output[f"bs_{i}"] > 5400, f"bs_{i}"] = 2147483647  # Replace any values that were interpolated to be between 5400 and 2147483647, these should still be marked as unreachable
        output[f"updt_{i}"] = updt[f"{i}"]  # Add the alternative scenario values to the output
        output.loc[output[f"updt_{i}"] > 5400, f"updt_{i}"] = 2147483647  # Replace any values that were interpolated to be between 5400 and 2147483647, these should still be marked as unreachable

        # Abs change
        output[f"abschg{i}"] = output[f"updt_{i}"] - output[f"bs_{i}"]
        output.loc[output[f'abschg{i}'] < -5401, f'abschg{i}'] = -6000  # Fill in where tt went from maxtime to reachable
        # Percent change
        output[f"pctchg{i}"] = (output[f"updt_{i}"] - output[f"bs_{i}"])/output[f"bs_{i}"]  # Take the percent difference
        output.loc[output[f'pctchg{i}'] < -0.99999, f'pctchg{i}'] = -1  # Fill in where tt went from maxtime to reachable

    # file_name_base = args.BASE_FILE.replace(".csv", "")
    # file_name_updt = args.UPDATE_FILE.replace(".csv", "")
    if args.OTHER_NAME_TAG:

        output.to_csv(f"2_change_{args.SHORT_BASE_NAME}_{args.SHORT_UPDT_NAME}_{args.OTHER_NAME_TAG}.csv")  # Output file name
    else:
        output.to_csv(f"2_change_{args.SHORT_BASE_NAME}_{args.SHORT_UPDT_NAME}.csv")  # Output file name


