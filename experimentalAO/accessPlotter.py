# -*- coding: utf-8 -*-
#This script is designed to read in processed access results and plot the trend 
# of access magnitude, absolute, and percent change versus the travel time threshold

import csv
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import fnmatch

import sys

with open(sys.argv[1], 'r') as f:

    df = pd.read_csv(f, header=0, names=['name', 'value'])
    print(df.head())
    print(type(df))
    print(list(df))
    
    #Initiate X,Y columns for scatter plot
    x = []
    y = []
    
    for index, row in df.iterrows():
        if fnmatch.fnmatch(row['name'], "abschgtmwt") == True:
            print('here', row['name'])
            pass
        elif fnmatch.fnmatch(row['name'], "abschg*") == True:

            if len(row['name']) == 10:
                x.append(int(row['name'][-4:]))
            if len(row['name']) == 9:
                x.append(int(row['name'][-3:]))
            y.append(float(row['value']))

    scatter = pd.DataFrame(list(zip(x,y)), columns = ['name', 'value'])
    scatter.sort_values('name', ascending = True)  
    minutes = list(val/ 60 for val in x )
    print(minutes)

    plt.close()
    plt.plot(scatter['name'], scatter['value'])
    plt.xticks(x)
    plt.show()
    plt.close()

