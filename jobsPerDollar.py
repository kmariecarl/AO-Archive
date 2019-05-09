#This script imports an averaged accessibility file and returns the jobs/dollar figure in replacement of the accessibility value
#Assumes that the 'threshold' field is in terms of cents, i.e. $5.00 is shown as 500

#################################
#           IMPORTS             #
#################################

from myToolsPackage import matrixLinkModule as mod
import argparse
import time
from progress import bar


#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    start_time, curtime = mod.startTimer()
    readable = time.ctime(start_time)
    print(readable)
    bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=972000) #54000 origins x 18 thresholds

    # Parameterize file paths
    parser = argparse.ArgumentParser()
    parser.add_argument('-file', '--ACCESS_FILE', required=True, default=None)  #enter the name of input file
    parser.add_argument('-thresh', '--THRESHOLD_FIELD', required=True, default=None)  #i.e. threshold or cost
    parser.add_argument('-field', '--FIELD', required=True, default=None)  #output field name i.e. jobspdol
    parser.add_argument('-fname', '--OUTPUT_FILE_NAME', required=True, default=None)  #i.e. transit16_cost_access
    args = parser.parse_args()

    threshold = args.THRESHOLD_FIELD
    fieldnames = ['label', '{}'.format(args.THRESHOLD_FIELD,),'{}'.format(args.FIELD)]
    writer = mod.mkDictOutput('jobs_per_dollar_{}_{}'.format(args.OUTPUT_FILE_NAME, curtime), fieldname_list=fieldnames)


    reader = mod.readInToDict(args.ACCESS_FILE)

    #Step through each origin_cost_jobs row and convert the cost threshold to dollars then divide jobs by that figure
    for i in reader:
        dollar = int(i[threshold])/100
        jobsperdollar = int(int(i['jobs'])/dollar)
        entry = {'label': i['label'], '{}'.format(threshold): i[threshold], '{}'.format(args.FIELD): jobsperdollar}
        writer.writerow(entry)
        bar.next()
    bar.finish()
    print('Finished converting transit time-based accessibility to cost-based accessibility')