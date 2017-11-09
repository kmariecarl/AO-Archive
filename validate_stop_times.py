from __future__ import print_function
from csv import DictReader, DictWriter
import argparse

def validate(reader):
    completed_trips = []
    prev_trip = None
    prev_stop = None
    index = 1 # 0 is the header row

    for row in reader:
        cur_trip = row['trip_id']
        cur_stop = int(row['stop_sequence'])

        if cur_trip == prev_trip:
            # Make sure stop sequence is greater than the previous stop sequence
            if cur_stop <= prev_stop:
                print("ERROR: Trip {} has non-increasing stop sequence (line {})".format(cur_trip, index))
                return

            # Check if stop sequence skips values
            if cur_stop > prev_stop + 1:
                print("WARNING: Trip {} has skipped stop sequence (line {})".format(cur_trip, index))

        else:
            # Record that we completed a block of the old trip
            completed_trips.append(prev_trip)

            # Make sure we haven't seen this new trip before
            if cur_trip in completed_trips:
                print("ERROR: Trip {} has non-contiguous stop time entries (line {})".format(cur_trip, index))
                return

        # prepare for the next row
        prev_trip = cur_trip
        prev_stop = cur_stop
        index += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", type=file)
    args = parser.parse_args()

    reader = DictReader(args.input_file)
    validate(reader)
