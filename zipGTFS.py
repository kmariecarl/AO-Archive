#This code opens GTFS files, reads stop_times & trips .txt files then writes a new stop_times.txt file and closes the zip.

import zipfile
import tempfile
import csv
import os
from shutil import copyfile

def print_info(archive_name):
    zf = zipfile.ZipFile(archive_name)
    for info in zf.infolist():
        print(info.filename)
        print('\tCompressed:\t', info.compress_size, 'bytes')
        print('\tUncompressed:\t', info.file_size, 'bytes')

if __name__ == '__main__':
    namelist = [ 'agency.txt', 'calendar.txt', 'calendar_dates.txt','routes.txt', 'shapes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']
    #Make parameters for directory
    copyfile('gtfs_09_06_16.zip', 'gtfs_09_06_16_copy')
    with zipfile.ZipFile ('gtfs_write.zip', mode='w', compression=zipfile.ZIP_DEFLATED) as myzip2:
        #Open trips file from folder and make into reader
        for name in namelist:
            with open(os.path.join('gtfs_09_06_16', name), 'r') as myfile:
                reader = csv.reader(myfile, delimiter=',')
                # Make a new file called "temp" and write to it
                with open('temp.txt', 'w', newline='') as outfile:
                    writer = csv.writer(outfile)
                    # Iterate through the reader and assign to writer file.
                    for row in reader:
                        #The brackets around row make sure that each row is read together, not each character.
                        writer.writerows([row])
            myzip2.write('temp.txt', name)

