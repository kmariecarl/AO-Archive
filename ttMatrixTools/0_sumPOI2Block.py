# This script is designed to create unique field names for TomTom POI data based on a concatenation of "package",
# "Feature type", and "Service Sub Category" fields. Creates approximately 384 fields. Then sums the number of each
# type of destination per block.

# Output filetype is a csv

# Previous usage was with TomTom data but the current usage expects a file containing two columns: POI name, GEOID10 it is associated with
# Since the GEOID10 might appear multiple times in that list, this program sums up the number of POIs that are within each unique block ID

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
            print("Select the Unique ID field (not the grouping ID field like GEOID10) from the following: \n", field_names)
            id = input("Type unique id field: ")
            my_dict = {}
            for row in in_dict:
                my_dict[row[f'{id}']] = {}
                for field in field_names:
                    my_dict[row[f'{id}']][f'{field}'] = row[f'{field}']


        self.dict = my_dict

    # Create concatenated name for the destination types
    def generate_fields(self, cat_id):
        fields_list = []
        for id, row in self.dict.items():
            new_field_str = "_{}".format(row[f'{cat_id}'])
            new_field = int(new_field_str)
            if new_field not in fields_list:
                fields_list.append(new_field)

        fields_list.sort()
        fields_list.insert(0, 'GEOID10')
        print("Fields list: \n", fields_list)
        print("Length of fields list: ", len(fields_list))
        return fields_list

    def generate_GEOID10_list(self):
        print("Generating GEOID10 list")
        geoid10_list = []
        for id, row in self.dict.items():
            if int(row['GEOID10']) not in geoid10_list:
                geoid10_list.append(int(row['GEOID10']))
        print("Number of GEOID10: ", len(geoid10_list))
        return geoid10_list

    def sum_over_fields(self, cat_id, output):  # cat_id is also new_field if there is no category id field provided
        print("Summing over POI fields")
        # Instantiate progress bar
        bar = ProgressBar(len(self.dict.keys()))
        for id, row in self.dict.items():
            field_str = "_{}".format(row[f'{cat_id}'])
            field = int(field_str)
            output[int(row['GEOID10'])][field] += 1
            bar.add_progress()
        bar.end_progress()

        return output

    #* This one will likely be used as opposed to the version above
    def sum_over_field(self, new_field, output):  # cat_id is also new_field if there is no category id field provided
        print("Summing all features to each GEOID10")
        # Instantiate progress bar
        bar = ProgressBar(len(self.dict.keys()))
        for id, row in self.dict.items():
            output[int(row['GEOID10'])][f'{new_field}'] += 1
            bar.add_progress()
        bar.end_progress()

        return output

def make_empty_output(fields_list, geoid10_list):
    output = {}
    print(fields_list)
# if isinstance(fields_list, list):  # Use this to distinguish if there is a single or multiple sum fields to include
    print("here1")
    for g in geoid10_list:
        output[g] = {}
        for f in fields_list:
            output[g][f] = 0

    else:  # If fields_list is not actually a list, it is just the name of the new_fld
        print("here2")
        for g in geoid10_list:
            output[g] = {}
        # This is a dumb way of writing code
        for g in geoid10_list:
            output[g][fields_list[0]] = 0
            print(output)
    return output

def write_out(output_filled, writer):
    for g, f in output_filled.items():
        print(writer.fieldnames)
        print(g, f)
        entry = {'GEOID10': g} #Attach the outtermost dict key (GEOID10) to output
        f.update(entry)
        writer.writerow(f)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--FILE', required=True, default=None)  # Provide name of POI file
    parser.add_argument('-cat_id', '--CATEGORY_ID_FIELD', required=False, default=None)  # Name of field associates the destination with the type of destination, i.e. SUBCAT
    args = parser.parse_args()

    file_name = args.FILE

    # Instantiate file object
    file = InputFile(file_name)
    print(file.file_name)

    geoid10_list = file.generate_GEOID10_list()

    if args.CATEGORY_ID_FIELD:
        fields_list = file.generate_fields(args.CATEGORY_ID_FIELD)
        writer = mod.mkDictOutput("sumPOI2Block", fields_list)

        output = make_empty_output(fields_list, geoid10_list)
        output_filled = file.sum_over_fields(args.CATEGORY_ID_FIELD, output)

        write_out(output_filled, writer)
    else:
        new_fld = input("Type new fieldname for summed destinations (probably should match input POI fieldname: ")
        writer = mod.mkDictOutput("sumPOI2Block", ['GEOID10', new_fld])

        output = make_empty_output([new_fld], geoid10_list)
        output_filled = file.sum_over_field(new_fld, output)

        write_out(output_filled, writer)
