# This script is designed to create unique field names for TomTom POI data based on a concatenation of "package",
# "Feature type", and "Service Sub Category" fields. Creates approximately 620 fields.

# Output filetype is a csv maybe a sqlite db

# ---------------------------------
#   GLOBAL VARIABLES & PACKAGES
# ---------------------------------
import argparse
from myToolsPackage import matrixLinkModule as mod
import csv
from progress import bar



# --------------------------
#       CLASSES
# --------------------------
class ProgressBar:
    def __init__(self, lines):
        self.bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=lines)
    def add_progress(self):
        self.bar.next()
    def end_progress(self):
        self.bar.finish()

class InputFile:
    # Instance attribute
    def __init__(self, file_name):
        self.file_name = file_name

        with open(file_name, 'r') as f:
            in_dict = csv.DictReader(f)
            field_names = in_dict.fieldnames
            my_dict = {}
            for row in in_dict:
                my_dict[row['ID']] = {}
                for field in field_names:
                    my_dict[row['ID']][f'{field}'] = row[f'{field}']


        self.dict = my_dict


    def generate_fields(self):
        fields_list = []
        for id, row in self.dict.items():
            if int(row['SUBCAT']) == 0:
                new_field_str = "{}".format(row['PACKAGE']) + "{}".format(row['FEATTYP']) + "{}".format(row['SUBCAT'])
                new_field = int(new_field_str)
                if new_field not in fields_list:
                    fields_list.append(new_field)
            else:
                new_field_str = "{}".format(row['PACKAGE']) + "{}".format(row['FEATTYP']) + "{}".format(row['SUBCAT'])
                new_field = int(new_field_str)
                if new_field not in fields_list:
                    fields_list.append(new_field)

        # Sort by first digit in ascending order
        # This converts each element into a string,
        # takes the first character (which would be the first digit),
        # and converts that back into an integer.
        # Python then sorts the numbers based on that first digit.
        # Solution from:
        # https://stackoverflow.com/questions/44622694/sort-a-list-on-the-basis-of-first-digit-of-the-elements
        fields_list.sort(key=lambda x: int(str(x)[0]))
        print("Fields list: \n", fields_list)
        print("Length of fields list: ", len(fields_list))
        return fields_list

    def generate_GEOID10_list(self):
        geoid10_list = []
        for id, row in self.dict.items():
            if int(row['GEOID10']) not in geoid10_list:
                geoid10_list.append(int(row['GEOID10']))
        print("Number of GEOID10: ", len(geoid10_list))
        return geoid10_list

    def sum_over_fields(self, geoid10_list, output):
        # Instantiate progress bar
        bar = ProgressBar(len(self.dict.keys()))
        for id, row in self.dict.items():
            field_str = "{}".format(row['PACKAGE']) + "{}".format(row['FEATTYP']) + "{}".format(row['SUBCAT'])
            field = int(field_str)
            output[int(row['GEOID10'])][field] += 1
            bar.add_progress()
        bar.end_progress()

        return output

def make_empty_output(fields_list, geoid10_list):
    output = {}
    for g in geoid10_list:
        output[g] = {}
        for f in fields_list:
            output[g][f] = 0
    return output

def write_out(output_filled, writer):
    for g, f in output_filled.items():
        writer.writerow(f)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--FILE', required=True, default=None)  # Provide name of TomTom POI file
    args = parser.parse_args()

    file_name = args.FILE

    # Instantiate file object
    file = InputFile(file_name)
    print(file.file_name)


    fields_list = file.generate_fields()
    geoid10_list = file.generate_GEOID10_list()
    output = make_empty_output(fields_list, geoid10_list)

    output_filled = file.sum_over_fields(geoid10_list, output)

    writer = mod.mkDictOutput("sumPOI2Block", fields_list)
    write_out(output_filled, writer)
