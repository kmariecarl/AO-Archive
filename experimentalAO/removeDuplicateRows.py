# This program reads in a text file and deletes duplicate rows, then writes a new file.
import csv


def deleteDuplicates():
    with open('852_156_TC_Baseline_merge.csv') as input:
        reader = csv.DictReader(input)
        with open('no_dup.csv', 'w', encoding='UTF-8') as output:
            # fieldnames = ['label', 'deptime', 'threshold', 'C000']
            # writer = csv.DictWriter(output, fieldnames=fieldnames)
            # writer.writeheader()
            lines_seen = set()
            for line in input:
                # print(line)
                if line not in lines_seen:
                    output.writelines(line)
                    # writer.writerow(line)
                    lines_seen.add(line)
                    # print(lines_seen)
            print('Duplicate Rows Removed!')


if __name__ == "__main__":
    deleteDuplicates()
