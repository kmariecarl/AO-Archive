# This script reads in a CSV origins or destinations file and converts the ID field to binary then writes the result to
# CSV

# Does NOT work as of 2/17/2020

import argparse
import csv
# Credit to https://www.geeksforgeeks.org/python-program-to-covert-decimal-to-binary-number/
# Function to convert decimal number
# to binary using recursion
def DecimalToBinary(num):
    if num > 1:
        DecimalToBinary(num // 2)
    return num % 2

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--FILE', required=True, default=None)
    args = parser.parse_args()

    file = args.FILE

    my_dict1 = {}
    my_dict2 = {}
    with open(file, 'r') as f:
        csv_reader = csv.DictReader(f)
        print(csv_reader.fieldnames)
        field1 = input("Type name of first field to change to binary: ")
        count = 0
        for row in csv_reader:
            unique_id = count
            my_dict1[field1] = row[field1]
            my_dict2['id'] = unique_id

            count += 1
            # writer.writerow(DecimalToBinary(row[field2]))

    with open('output.csv', 'w') as output_file:
        writer = csv.writer(output_file, delimiter=',')
        writer.writerow(csv_reader.fieldnames)
        print("Input fieldnames: ")
        for row1, row2 in zip(my_dict1, my_dict2):
            writer
        writer.writerow(entry)
        # field2 = input("Type name of second field to change to binary: ")

