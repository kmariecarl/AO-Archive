# This file reads in a processedAccessResults.csv file and converts the order of the fields and field names to a format
# that is easier for the public to read.

import csv
import sys


def main():

    with open(sys.argv[1], 'r') as f:
        mydict = csv.DictReader(f)
        file_name = str(sys.argv[1])
        print('This is the file name: ', file_name)
        header = mydict.fieldnames
        print('Current field names', header)
        print('---------------------------')

        newfields = ['blockid', 'bs_5min', 't1_5min', 'abschg_5min', 'pctchg_5min', 'bs_10min', 't1_10min', 'abschg_10min', 'pctchg_10min',
                     'bs_15min', 't1_15min', 'abschg_15min', 'pctchg_15min', 'bs_20min', 't1_20min', 'abschg_20min', 'pctchg_20min',
                     'bs_25min', 't1_25min', 'abschg_25min', 'pctchg_25min', 'bs_30min', 't1_30min', 'abschg_30min', 'pctchg_30min',
                     'bs_35min', 't1_35min', 'abschg_35min', 'pctchg_35min', 'bs_40min', 't1_40min', 'abschg_40min',
                     'pctchg_40min', 'bs_45min', 't1_45min', 'abschg_45min', 'pctchg_45min', 'bs_50min', 't1_50min', 'abschg_50min',
                     'pctchg_50min', 'bs_55min', 't1_55min', 'abschg_55min', 'pctchg_55min', 'bs_60min', 't1_60min', 'abschg_60min',
                     'pctchg_60min', 'bs_65min', 't1_65min', 'abschg_65min', 'pctchg_65min', 'bs_70min', 't1_70min', 'abschg_70min',
                     'pctchg_70min', 'bs_75min', 't1_75min', 'abschg_75min', 'pctchg_75min', 'bs_80min', 't1_80min', 'abschg_80min',
                     'pctchg_80min', 'bs_85min', 't1_85min', 'abschg_85min', 'pctchg_85min', 'bs_90min', 't1_90min', 'abschg_90min', 'pctchg_90min',
                     'bs_wt', 't1_wt', 'abschg_wt', 'pctchg_wt']

        print('New field names', newfields)


        with open('{}_out.csv'.format(file_name[:-4]), 'w', newline='') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=newfields)

            writer.writeheader()
            for row in mydict:
                entry = {'blockid': row['label'], 'bs_5min': row['bs_300'], 't1_5min': row['alt_300'],
                         'abschg_5min': row['abschg300'],
                         'pctchg_5min': row['pctchg300'], 'bs_10min': row['bs_600'], 't1_10min': row['alt_600'],
                         'abschg_10min': row['abschg600'],
                         'pctchg_10min': row['pctchg600'], 'bs_15min': row['bs_900'], 't1_15min': row['alt_900'],
                         'abschg_15min': row['abschg900'],
                         'pctchg_15min': row['pctchg900'], 'bs_20min': row['bs_1200'], 't1_20min': row['alt_1200'],
                         'abschg_20min': row['abschg1200'],
                         'pctchg_20min': row['pctchg1200'], 'bs_25min': row['bs_1500'], 't1_25min': row['alt_1500'],
                         'abschg_25min': row['abschg1500'],
                         'pctchg_25min': row['pctchg1500'], 'bs_30min': row['bs_1800'], 't1_30min': row['alt_1800'],
                         'abschg_30min': row['abschg1800'],
                         'pctchg_30min': row['pctchg1800'], 'bs_35min': row['bs_2100'], 't1_35min': row['alt_2100'],
                         'abschg_35min': row['abschg2100'],
                         'pctchg_35min': row['pctchg2100'], 'bs_40min': row['bs_2400'], 't1_40min': row['alt_2400'],
                         'abschg_40min': row['abschg2400'],
                         'pctchg_40min': row['pctchg2400'], 'bs_45min': row['bs_2700'], 't1_45min': row['alt_2700'],
                         'abschg_45min': row['abschg2700'],
                         'pctchg_45min': row['pctchg2700'], 'bs_50min': row['bs_3000'], 't1_50min': row['alt_3000'],
                         'abschg_50min': row['abschg3000'],
                         'pctchg_50min': row['pctchg3000'], 'bs_55min': row['bs_3300'], 't1_55min': row['alt_3300'],
                         'abschg_55min': row['abschg3300'],
                         'pctchg_55min': row['pctchg3300'], 'bs_60min': row['bs_3600'], 't1_60min': row['alt_3600'],
                         'abschg_60min': row['abschg3600'],
                         'pctchg_60min': row['pctchg3600'], 'bs_65min': row['bs_3900'], 't1_65min': row['alt_3900'],
                         'abschg_65min': row['abschg3900'],
                         'pctchg_65min': row['pctchg3900'], 'bs_70min': row['bs_4200'], 't1_70min': row['alt_4200'],
                         'abschg_70min': row['abschg4200'],
                         'pctchg_70min': row['pctchg4200'], 'bs_75min': row['bs_4500'], 't1_75min': row['alt_4500'],
                         'abschg_75min': row['abschg4500'],
                         'pctchg_75min': row['pctchg4500'], 'bs_80min': row['bs_4800'], 't1_80min': row['alt_4800'],
                         'abschg_80min': row['abschg4800'],
                         'pctchg_80min': row['pctchg4800'], 'bs_85min': row['bs_5100'], 't1_85min': row['alt_5100'],
                         'abschg_85min': row['abschg5100'],
                         'pctchg_85min': row['pctchg5100'], 'bs_90min': row['bs_5400'], 't1_90min': row['alt_5400'],
                         'abschg_90min': row['abschg5400'],
                         'pctchg_90min': row['pctchg5400'], 'bs_wt': row['wt_bs'], 't1_wt': row['wt_alt'],
                         'abschg_wt': row['abschgtmwt'],
                         'pctchg_wt': row['pctchgtmwt']}
                writer.writerow(entry)


    return mydict


if __name__ == '__main__':
        main()