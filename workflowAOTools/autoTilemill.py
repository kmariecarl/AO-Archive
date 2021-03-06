# This script automates the generation of Tilemill images for individual project types, i.e. external sales
# Created by Kristin Carlson 9/18/2019


# This program assumes the user has 1. created the Tilemill project folder from "template"
# 2. added all static layers to the "static" sheet, and added access results layers to the project in order for the
# "dynamic" .mss sheets to find layers based on user input 3. identified and set the bounding box within the project.

# This program should be run from Terminal which is open to the Tilemill project folder.
# This program will export tilemill .png images into the MapBox/export folder

# If project is not found, you may need to change the hard coded path below


import subprocess
import os
import csv
from datetime import datetime


def main(project_name, layer_list, zoom, width, height, workflow, options):

    if workflow == 'primal':
        # ----------For access cost mapping----------
        # For access cost mapping, $1.50 increments transit + VOT
        # abs_val_fields = ["rwchg500", "rwchg650", "rwchg800", "rwchg950", "rwchg1100", "rwchg1250", "rwchg1400",
        #                   "rwchg1550", "rwchg1700", "rwchg1850", "rwchg2000", "rwchg2150", "rwchg2300", "rwchg2450",
        #                   "rwchg2600", "rwchg2750", "rwchg2900", "rwchg3050"]

        #Use when not dealing with VOT
        # cost_thresh_list = [200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000,
        #                        1050, 1100, 1150, 1200, 1250, 1300, 1350, 1400, 1450, 1500, 1550, 1600, 1650, 1700, 1750,
        #                        1800, 1850, 1900, 1950, 2000, 2050, 2100, 2150, 2200, 2250, 2300, 2350, 2400, 2450, 2500,
        #                        2550, 2600, 2650, 2700, 2750, 2800, 2850, 2900, 2950, 3000]

        # For access cost mapping, $0.50 increments for auto + VOT and PNR + VOT
        # cost_thresh_list = [200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000,
        #                        1050, 1100, 1150, 1200, 1250, 1300, 1350, 1400, 1450, 1500, 1550, 1600, 1650, 1700, 1750,
        #                        1800, 1850, 1900, 1950, 2000, 2050, 2100, 2150, 2200, 2250, 2300, 2350, 2400, 2450, 2500,
        #                        2550, 2600, 2650, 2700, 2750, 2800, 2850, 2900, 2950, 3000, 3050, 3100, 3150, 3200, 3250, 3300,
        #                        3350, 3400, 3450, 3500, 3550, 3600, 3650, 3700, 3750, 3800, 3850, 3900, 3950, 4000, 4050, 4100,
        #                        4150, 4200, 4250, 4300, 4350, 4400, 4450, 4500, 4550, 4600, 4650, 4700, 4750, 4800, 4850, 4900,
        #                        4950, 5000]
        # For access cost mapping to add prefix to field names
        # prefix = ["rwbs", "rwchg"]
        # abs_val_fields = []
        # for p in prefix:
        #     for i in cost_thresh_list:
        #         fieldstring = f"{p}{i}"
        #         abs_val_fields.append(fieldstring)

        # ----------For access time mapping----------
        if options[0] == "Y":
            bs_val_fields = ["wt_bs", "bs_5400", "bs_5100", "bs_4800", "bs_4500",  "bs_4200", "bs_3900", "bs_3600",
                             "bs_3300", "bs_3000", "bs_2700", "bs_2400", "bs_2100", "bs_1800", "bs_1500", "bs_1200",
                             "bs_900","bs_600", "bs_300"]

            for layer in layer_list:
                for field in bs_val_fields:
                    # Remove any existing dynamic style file
                    if os.path.exists(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss'):
                        os.remove(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss')

                    else:
                        print("Style file does not exist")
                    mssAbsVal(layer, field)
                    renderMap(project_name, layer, field, zoom, width, height,)
        else:
            print("Absolute accessibility baseline mapping not selected")

        if options[1] == "Y":
            alt_val_fields = ["wt_alt", "alt_5400", "alt_5100", "alt_4800", "alt_4500",  "alt_4200", "alt_3900", "alt_3600",
                             "alt_3300", "alt_3000", "alt_2700", "alt_2400", "alt_2100", "alt_1800", "alt_1500", "alt_1200",
                             "alt_900","alt_600", "alt_300"]

            for layer in layer_list:
                for field in alt_val_fields:
                    # Remove any existing dynamic style file
                    if os.path.exists(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss'):
                        os.remove(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss')

                    else:
                        print("Style file does not exist")
                    mssAbsVal(layer, field)
                    renderMap(project_name, layer, field, zoom, width, height,)
        else:
            print("Absolute accessibility alternative mapping not selected")

        if options[2] == "Y":
            abs_chg_fields = ["abschgtmwt", "abschg5400", "abschg5100", "abschg4800", "abschg4500", "abschg4200",
                              "abschg3900",
                              "abschg3600", "abschg3300", "abschg3000", "abschg2700", "abschg2400", "abschg2100",
                              "abschg1800",
                              "abschg1500", "abschg1200", "abschg900", "abschg600", "abschg300", ]

            for layer in layer_list:
                for field in abs_chg_fields:
                    # Remove any existing dynamic style file
                    if os.path.exists(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss'):
                        os.remove(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss')

                    else:
                        print("Style file does not exist")

                    mssAbsChg(layer, field)
                    renderMap(project_name, layer, field, zoom, width, height,)
        else:
            print("Absolute change mapping not selected")

        if options[3] == "Y":

            pct_chg_fields = ["pctchgtmwt", "pctchg5400", "pctchg5100", "pctchg4800", "pctchg4500", "pctchg4200", "pctchg3900",
                              "pctchg3600", "pctchg3300", "pctchg3000", "pctchg2700", "pctchg2400", "pctchg2100", "pctchg1800",
                              "pctchg1500", "pctchg1200", "pctchg900", "pctchg600", "pctchg300"]

            for layer in layer_list:
                for field in pct_chg_fields:
                    # Remove any existing dynamic style file
                    if os.path.exists(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss'):
                        os.remove(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss')

                    else:
                        print("Style file does not exist")

                    mssPctChg(layer, field)
                    renderMap(project_name, layer, field, zoom, width, height,)
        else:
            print("Percent change mapping not selected")

    # ----Dual Access Workflow----
    if workflow == 'dual':

        if options[0] == "Y":
            bs_val_fields = ["bs_1", "bs_2", "bs_3", "bs_4", "bs_5", "bs_6", "bs_7", "bs_8", "bs_9", "bs_10"]
            for layer in layer_list:
                for field in bs_val_fields:
                    # Remove any existing dynamic style file
                    if os.path.exists(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss'):
                        os.remove(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss')

                    else:
                        print("Style file does not exist")
                    mssDualAbsVal(layer, field)
                    renderDualMap(project_name, layer, field, zoom, width, height,)
        else:
            print("Absolute accessibility baseline mapping not selected")

        if options[1] == "Y":
            alt_val_fields = ["alt_1", "alt_2", "alt_3", "alt_4", "alt_5", "alt_6", "alt_7", "alt_8", "alt_9", "alt_10"]
            for layer in layer_list:
                for field in alt_val_fields:
                    # Remove any existing dynamic style file
                    if os.path.exists(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss'):
                        os.remove(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss')

                    else:
                        print("Style file does not exist")
                    mssDualAbsVal(layer, field)
                    renderDualMap(project_name, layer, field, zoom, width, height, )
        else:
            print("Absolute accessibility alternative mapping not selected")

        if options[2] == "Y":
            abs_chg_fields = ["abschg1", "abschg2", "abschg3", "abschg4", "abschg5",
                              "abschg6", "abschg7", "abschg8", "abschg9", "abschg10"]

            for layer in layer_list:
                for field in abs_chg_fields:
                    # Remove any existing dynamic style file
                    if os.path.exists(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss'):
                        os.remove(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss')

                    else:
                        print("Style file does not exist")

                    mssDualAbsChg(layer, field)
                    renderDualMap(project_name, layer, field, zoom, width, height,)
        else:
            print("Absolute change mapping not selected")

        if options[3] == "Y":

            pct_chg_fields = ["pctchg1", "pctchg2", "pctchg3", "pctchg4", "pctchg5",
                              "pctchg6", "pctchg7", "pctchg8", "pctchg9", "pctchg10"]

            for layer in layer_list:
                for field in pct_chg_fields:
                    # Remove any existing dynamic style file
                    if os.path.exists(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss'):
                        os.remove(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss')

                    else:
                        print("Style file does not exist")

                    mssDualPctChg(layer, field)
                    renderDualMap(project_name, layer, field, zoom, width, height,)
        else:
            print("Percent change mapping not selected")

def mssDualAbsVal(layer, field):
    with open(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        style = f"""#{layer} {{
        line-width: 0;
        polygon-opacity: 1;
        [{field}=null]{{polygon-fill:#ffffff;}}
        [{field}>=0]{{polygon-fill:#f9f871;}} 
        [{field}>=300]{{polygon-fill:#ecbc77;}}
        [{field}>=600]{{polygon-fill:#df7b7d;}}
        [{field}>=1200]{{polygon-fill:#a65182;}}
        [{field}>=2400]{{polygon-fill:#423e85;}}
        [{field}>=3600]{{polygon-pattern-file:url(/Users/kristincarlson/Documents/MapBox/project/TIRP/images/hatch_abs.png);}}
        }}"""
        style_split = style.splitlines()

        for row in style_split:
            print(row)

            spamwriter.writerow([row])

def mssDualAbsChg(layer, field):
    # here values of change >= 3600 are colored white
    # because polygons with this value in tilemill are
    # missing and otherwise would be colored dark red
    with open(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        style = f"""#{layer} {{
        line-width: 0;
        polygon-opacity: 1;
        [{field}=null]{{polygon-fill:#ffffff;}}
        [{field}=-6000]{{polygon-pattern-file:url(/Users/kristincarlson/Documents/MapBox/project/TIRP/images/hatch_abschg.png);}} 
        [{field}>=-3600]{{polygon-fill:#26244e;}}
        [{field}>=-2400]{{polygon-fill:#4a468a;}}
        [{field}>=-1200]{{polygon-fill:#7472a6;}}
        [{field}>=-600]{{polygon-fill:#a19fc2;}}
        [{field}>=-300]{{polygon-fill:#cfcee0;}}
        [{field}>=-30]{{polygon-fill:#f7f7f7;}}
        [{field}=0]{{polygon-fill:#f7f7f7;}}
        [{field}>0]{{polygon-fill:#f7f7f7;}}
        [{field}>=60]{{polygon-fill:#e7c7d4;}}
        [{field}>=300]{{polygon-fill:#d090a9;}}
        [{field}>=600]{{polygon-fill:#b8577d;}}
        [{field}>=1200]{{polygon-fill:#9a1047;}}
        [{field}>=2400]{{polygon-fill:#590023;}}
        [{field}>=3600]{{polygon-fill:#f7f7f7;}}
        }}"""
        style_split = style.splitlines()

        for row in style_split:
            print(row)

            spamwriter.writerow([row])

def mssDualPctChg(layer, field):
    with open(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        style = f"""#{layer} {{
        line-width: 0;
        polygon-opacity: 1;
        [{field}=null]{{polygon-fill:#ffffff;}}
        [{field}=-1]{{polygon-pattern-file:url(/Users/kristincarlson/Documents/MapBox/project/TIRP/images/hatch_abschg.png);}} 
        [{field}>=-0.99]{{polygon-fill:#26244e;}}
        [{field}>=-0.4]{{polygon-fill:#4a468a;}}
        [{field}>=-0.2]{{polygon-fill:#7472a6;}}
        [{field}>=-0.1]{{polygon-fill:#a19fc2;}}
        [{field}>=-0.05]{{polygon-fill:#cfcee0;}}
        [{field}>=-0.005]{{polygon-fill:#f7f7f7;}}
        [{field}=0]{{polygon-fill:#f7f7f7;}}
        [{field}>0]{{polygon-fill:#f7f7f7;}}
        [{field}>=0.005]{{polygon-fill:#e7c7d4;}}
        [{field}>=0.05]{{polygon-fill:#d090a9;}}
        [{field}>=0.1]{{polygon-fill:#b8577d;}}
        [{field}>=0.2]{{polygon-fill:#9a1047;}}
        [{field}>=0.4]{{polygon-fill:#590023;}}
        [{field}>=0.99]{{polygon-fill:#f7f7f7;}}
        }}"""
        style_split = style.splitlines()

        for row in style_split:
            print(row)

            spamwriter.writerow([row])


def mssAbsVal(layer, field):

    with open(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        style = f"""#{layer} {{
        line-width: 0;
        polygon-opacity: 0.75;
        [{field}=null]{{polygon-fill:#DDFBFC;}}  
        [{field}>=-1]{{polygon-fill:#DDFBFC;}}
        [{field}>=1]{{polygon-fill:#D8FAFB;}}
        [{field}>=4]{{polygon-fill:#D4FAFB;}}
        [{field}>=8]{{polygon-fill:#CFF9FB;}}
        [{field}>=16]{{polygon-fill:#CBF9FA;}}
        [{field}>=32]{{polygon-fill:#C6F8FA;}}
        [{field}>=63]{{polygon-fill:#C2F8FA;}}
        [{field}>=126]{{polygon-fill:#BDF7F9;}}
        [{field}>=251]{{polygon-fill:#B9F7F9;}}
        [{field}>=501]{{polygon-fill:#B4F6F9;}}
        [{field}>=1000]{{polygon-fill:#B0F6F9;}}
        [{field}>=1100]{{polygon-fill:#AEF6F6;}}
        [{field}>=1200]{{polygon-fill:#ACF6F4;}}
        [{field}>=1320]{{polygon-fill:#ABF6F2;}}
        [{field}>=1440]{{polygon-fill:#A9F6F0;}}
        [{field}>=1580]{{polygon-fill:#A8F7EE;}}
        [{field}>=1730]{{polygon-fill:#A6F7EC;}}
        [{field}>=1900]{{polygon-fill:#A4F7EA;}}
        [{field}>=2080]{{polygon-fill:#A3F7E8;}}
        [{field}>=2280]{{polygon-fill:#A1F7E6;}}
        [{field}>=2500]{{polygon-fill:#A0F8EA;}}
        [{field}>=2680]{{polygon-fill:#9EF7E0;}}
        [{field}>=2870]{{polygon-fill:#9CF7DD;}}
        [{field}>=3080]{{polygon-fill:#9BF7DA;}}
        [{field}>=3300]{{polygon-fill:#99F6D7;}}
        [{field}>=3540]{{polygon-fill:#98F6D4;}}
        [{field}>=3790]{{polygon-fill:#96F6D0;}}
        [{field}>=4060]{{polygon-fill:#94F5CD;}}
        [{field}>=4350]{{polygon-fill:#93F5CA;}}
        [{field}>=4670]{{polygon-fill:#91F5C7;}}
        [{field}>=5000]{{polygon-fill:#90F5C4;}}
        [{field}>=5210]{{polygon-fill:#8EF4C0;}}
        [{field}>=5420]{{polygon-fill:#8CF4BC;}}
        [{field}>=5650]{{polygon-fill:#8AF4B8;}}
        [{field}>=5880]{{polygon-fill:#89F4B4;}}
        [{field}>=6120]{{polygon-fill:#87F4B0;}}
        [{field}>=6380]{{polygon-fill:#85F3AC;}}
        [{field}>=6640]{{polygon-fill:#84F3A8;}}
        [{field}>=6920]{{polygon-fill:#82F3A4;}}
        [{field}>=7200]{{polygon-fill:#80F3A0;}}
        [{field}>=7500]{{polygon-fill:#7FF39D;}}
        [{field}>=7720]{{polygon-fill:#7EF299;}}
        [{field}>=7950]{{polygon-fill:#7EF296;}}
        [{field}>=8180]{{polygon-fill:#7DF293;}}
        [{field}>=8420]{{polygon-fill:#7DF28F;}}
        [{field}>=8660]{{polygon-fill:#7CF28C;}}
        [{field}>=8910]{{polygon-fill:#7CF289;}}
        [{field}>=9170]{{polygon-fill:#7BF285;}}
        [{field}>=9440]{{polygon-fill:#7BF282;}}
        [{field}>=9720]{{polygon-fill:#7AF27F;}}
        [{field}>=10000]{{polygon-fill:#7AF27C;}}
        [{field}>=11000]{{polygon-fill:#7BF17A;}}
        [{field}>=12000]{{polygon-fill:#7CF178;}}
        [{field}>=13200]{{polygon-fill:#7DF176;}}
        [{field}>=14400]{{polygon-fill:#7FF074;}}
        [{field}>=15800]{{polygon-fill:#80F072;}}
        [{field}>=17300]{{polygon-fill:#81F070;}}
        [{field}>=19000]{{polygon-fill:#83EF6E;}}
        [{field}>=20800]{{polygon-fill:#84EF6C;}}
        [{field}>=22800]{{polygon-fill:#85EF6A;}}
        [{field}>=25000]{{polygon-fill:#87EF68;}}
        [{field}>=26800]{{polygon-fill:#8AEE66;}}
        [{field}>=28700]{{polygon-fill:#8DEE65;}}
        [{field}>=30800]{{polygon-fill:#90EE64;}}
        [{field}>=33000]{{polygon-fill:#93EE62;}}
        [{field}>=35400]{{polygon-fill:#96EE61;}}
        [{field}>=37900]{{polygon-fill:#99ED60;}}
        [{field}>=40600]{{polygon-fill:#9CED5E;}}
        [{field}>=43500]{{polygon-fill:#9FED5D;}}
        [{field}>=46700]{{polygon-fill:#A2ED5C;}}
        [{field}>=50000]{{polygon-fill:#A6ED5B;}}
        [{field}>=52100]{{polygon-fill:#A9EC5A;}}
        [{field}>=54200]{{polygon-fill:#ACEC59;}}
        [{field}>=56500]{{polygon-fill:#AFEC58;}}
        [{field}>=58800]{{polygon-fill:#B2EC57;}}
        [{field}>=61200]{{polygon-fill:#B5EC56;}}
        [{field}>=63800]{{polygon-fill:#B8EB55;}}
        [{field}>=66400]{{polygon-fill:#BBEB54;}}
        [{field}>=69200]{{polygon-fill:#BEEB53;}}
        [{field}>=72000]{{polygon-fill:#C1EB52;}}
        [{field}>=75000]{{polygon-fill:#C5EB51;}}
        [{field}>=77200]{{polygon-fill:#C8EA50;}}
        [{field}>=79500]{{polygon-fill:#CCE94F;}}
        [{field}>=81800]{{polygon-fill:#D0E84E;}}
        [{field}>=84200]{{polygon-fill:#D3E74D;}}
        [{field}>=86600]{{polygon-fill:#D7E74D;}}
        [{field}>=89100]{{polygon-fill:#DBE64C;}}
        [{field}>=91700]{{polygon-fill:#DEE54B;}}
        [{field}>=94400]{{polygon-fill:#E2E44A;}}
        [{field}>=97200]{{polygon-fill:#E6E349;}}
        [{field}>=100000]{{polygon-fill:#EAE349;}}
        [{field}>=110000]{{polygon-fill:#E9DE47;}}
        [{field}>=120000]{{polygon-fill:#E9DA46;}}
        [{field}>=132000]{{polygon-fill:#E9D544;}}
        [{field}>=144000]{{polygon-fill:#E9D143;}}
        [{field}>=158000]{{polygon-fill:#E9CC41;}}
        [{field}>=173000]{{polygon-fill:#E9C840;}}
        [{field}>=190000]{{polygon-fill:#E9C33E;}}
        [{field}>=208000]{{polygon-fill:#E9BF3D;}}
        [{field}>=228000]{{polygon-fill:#E9BA3B;}}
        [{field}>=250000]{{polygon-fill:#E9B63A;}}
        [{field}>=268000]{{polygon-fill:#E8B038;}}
        [{field}>=287000]{{polygon-fill:#E8AA36;}}
        [{field}>=308000]{{polygon-fill:#E7A435;}}
        [{field}>=330000]{{polygon-fill:#E79E33;}}
        [{field}>=354000]{{polygon-fill:#E69832;}}
        [{field}>=379000]{{polygon-fill:#E69230;}}
        [{field}>=406000]{{polygon-fill:#E58C2E;}}
        [{field}>=435000]{{polygon-fill:#E5862D;}}
        [{field}>=467000]{{polygon-fill:#E4802B;}}
        [{field}>=500000]{{polygon-fill:#E47A2A;}}
        [{field}>=521000]{{polygon-fill:#E37328;}}
        [{field}>=542000]{{polygon-fill:#E26C27;}}
        [{field}>=565000]{{polygon-fill:#E16526;}}
        [{field}>=588000]{{polygon-fill:#E15F24;}}
        [{field}>=612000]{{polygon-fill:#E05823;}}
        [{field}>=638000]{{polygon-fill:#DF5122;}}
        [{field}>=664000]{{polygon-fill:#DF4B20;}}
        [{field}>=692000]{{polygon-fill:#DE441F;}}
        [{field}>=720000]{{polygon-fill:#DD3D1E;}}
        [{field}>=750000]{{polygon-fill:#DD371D;}}
        [{field}>=772000]{{polygon-fill:#DA321C;}}
        [{field}>=794000]{{polygon-fill:#D82E1B;}}
        [{field}>=818000]{{polygon-fill:#D62A1B;}}
        [{field}>=866000]{{polygon-fill:#D2211A;}}
        [{field}>=891000]{{polygon-fill:#CF1D19;}}
        [{field}>=917000]{{polygon-fill:#CD1818;}}
        [{field}>=944000]{{polygon-fill:#CB1418;}}
        [{field}>=972000]{{polygon-fill:#C91017;}}
        [{field}>=1000000]{{polygon-fill:#C70C17;}}
        [{field}>=1100000]{{polygon-fill:#BD0A19;}}
        [{field}>=1200000]{{polygon-fill:#B3091C;}}
        [{field}>=1320000]{{polygon-fill:#A9081F;}}
        [{field}>=1440000]{{polygon-fill:#A00722;}}
        [{field}>=1580000]{{polygon-fill:#960625;}}
        [{field}>=1730000]{{polygon-fill:#8C0427;}}
        [{field}>=1900000]{{polygon-fill:#83032A;}}
        [{field}>=2080000]{{polygon-fill:#79022D;}}
        [{field}>=2280000]{{polygon-fill:#6F0130;}}
        [{field}>=2500000]{{polygon-fill:#660033;}}
        [{field}>=2680000]{{polygon-fill:#660038;}}
        [{field}>=2870000]{{polygon-fill:#66003D;}}
        [{field}>=3080000]{{polygon-fill:#660042;}}
        [{field}>=3300000]{{polygon-fill:#660047;}}
        [{field}>=3540000]{{polygon-fill:#66004C;}}
        [{field}>=3790000]{{polygon-fill:#660051;}}
        [{field}>=4060000]{{polygon-fill:#660056;}}
        [{field}>=4350000]{{polygon-fill:#66005B;}}
        [{field}>=4670000]{{polygon-fill:#660060;}}
        [{field}>=5000000]{{polygon-fill:#660066;}}
        [{field}>=5210000]{{polygon-fill:#751675;}}
        [{field}>=5420000]{{polygon-fill:#842C84;}}
        [{field}>=5650000]{{polygon-fill:#934293;}}
        [{field}>=5880000]{{polygon-fill:#A358A3;}}
        [{field}>=6120000]{{polygon-fill:#B26EB2;}}
        [{field}>=6380000]{{polygon-fill:#C184C1;}}
        [{field}>=6640000]{{polygon-fill:#D19AD1;}}
        [{field}>=6920000]{{polygon-fill:#E0B0E0;}}
        [{field}>=7200000]{{polygon-fill:#EFC6EF;}}
        [{field}>=7500000]{{polygon-fill:#FFDCFF;}}
        [{field}>=7720000]{{polygon-fill:#FDDEFF;}}
        [{field}>=7940000]{{polygon-fill:#FBE0FF;}}
        [{field}>=8180000]{{polygon-fill:#F9E3FF;}}
        [{field}>=8660000]{{polygon-fill:#F7E5FF;}}
        [{field}>=8910000]{{polygon-fill:#F5E8FF;}}
        [{field}>=9170000]{{polygon-fill:#F3EAFF;}}
        [{field}>=9440000]{{polygon-fill:#F1EDFF;}}
        [{field}>=9720000]{{polygon-fill:#EFEFFF;}}
        [{field}>=10000000]{{polygon-fill:#EDF2FF;}}
        }}"""

        style_split = style.splitlines()

        for row in style_split:
            print(row)

            spamwriter.writerow([row])

# Kristin, on 1/6/20 this scale was changed to line up with Pixelmator scale and adjust so that negative change bins
# look "forward" as in a.k.a. between -1,000% and -100%, color is #9b5a18, between -100% and -50% is #ae6f2e. Discussed
# this with Andrew. 4/1/2020 "no change" zone updated from -0.5%--+0.5% to -1%--+0.5% to accommodate random access loss
# that shows up in scenario evaluations
def mssPctChg(layer, field):
    with open(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        style = f"""#{layer} {{
        line-width: 0;
        polygon-opacity: 0.75;
        [{field}=null]{{polygon-fill:#ffffff;}}
        [{field}>=-10.0]{{polygon-fill:#9b5a18;}} 
        [{field}>=-1.0]{{polygon-fill:#ae6f2e;}}
        [{field}>=-0.5]{{polygon-fill:#b67d43;}}
        [{field}>=-0.25]{{polygon-fill:#c69a6d;}}
        [{field}>=-0.1]{{polygon-fill:#d6b796;}}
        [{field}>=-0.05]{{polygon-fill:#e6d3c0;}}
        [{field}>=-0.01]{{polygon-fill:#FFFFFF;}}
        [{field}=0]{{polygon-fill:#FFFFFF;}}
        [{field}>0]{{polygon-fill:#FFFFFF;}}
        [{field}>=0.005]{{polygon-fill:#b9ddd8;}}
        [{field}>=0.05]{{polygon-fill:#8bc7be;}}
        [{field}>=0.1]{{polygon-fill:#5db1a4;}}
        [{field}>=0.25]{{polygon-fill:#2f9b8a;}}
        [{field}>=0.5]{{polygon-fill:#18907d;}}
        [{field}>=1.0]{{polygon-fill:#018571;}}
        }}"""
        style_split = style.splitlines()

        for row in style_split:
            print(row)

            spamwriter.writerow([row])


 # One 12/30/19 we removed coloring for change +/- 200 of change
def mssAbsChg(layer, field):
    with open(f'/Users/kristincarlson/Documents/MapBox/project/{project_name}/dynamic.mss', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        style = f"""#{layer} {{
        line-width: 0;
        polygon-opacity: 0.75;
        [{field}=null]{{polygon-fill:#ffffff;}}
        [{field}>=-10000000]{{polygon-fill:#A6611A;}}
        [{field}>=-250000]{{polygon-fill:#AE6F2E;}}
        [{field}>=-100000]{{polygon-fill:#B67D43;}}
        [{field}>=-75000]{{polygon-fill:#BE8C58;}}
        [{field}>=-50000]{{polygon-fill:#C69A6D;}}
        [{field}>=-25000]{{polygon-fill:#CEA882;}}
        [{field}>=-10000]{{polygon-fill:#D6B796;}}
        [{field}>=-7500]{{polygon-fill:#DEC5AB;}}
        [{field}>=-5000]{{polygon-fill:#E6D3C0;}}
        [{field}>=-2500]{{polygon-fill:#EEE2D5;}}
        [{field}>=-1000]{{polygon-fill:#F6F0EA;}}
        [{field}>=-200]{{polygon-fill:#FFFFFF;}}
        [{field}=0]{{polygon-fill:#FFFFFF;}}
        [{field}>0]{{polygon-fill:#FFFFFF;}}
        [{field}>=200]{{polygon-fill:#E7F3F2;}}
        [{field}>=1000]{{polygon-fill:#D0E8E5;}}
        [{field}>=2500]{{polygon-fill:#B9DDD8;}}
        [{field}>=5000]{{polygon-fill:#A2D2CB;}}
        [{field}>=7500]{{polygon-fill:#8BC7BE;}}
        [{field}>=10000]{{polygon-fill:#74BCB1;}}
        [{field}>=25000]{{polygon-fill:#5DB1A4;}}
        [{field}>=50000]{{polygon-fill:#46A697;}}
        [{field}>=75000]{{polygon-fill:#2F9B8A;}}
        [{field}>=100000]{{polygon-fill:#18907D;}}
        [{field}>=250000]{{polygon-fill:#018571;}}
        }}"""
        style_split = style.splitlines()

        for row in style_split:
            print(row)

            spamwriter.writerow([row])


# # Render the project
def renderMap(project_name, layer, field, zoom, width, height,):
    # os.chdir('/Applications/TileMill-2.app/Contents/Resources/') old iMac path to Tilemill
    os.chdir('/Users/kristincarlson/Applications/TileMill.app/Contents/Resources')
    bash_command = f'./index.js export {project_name} ~/Documents/MapBox/export/{project_name}_{layer}_{field}.png --format=png --width={width} ' \
                   f'--height={height} --static_zoom={zoom} --verbose'
    p4 = subprocess.Popen(bash_command, shell=True)
    p4.communicate()

# The only difference is '_TT_' is part of the file name
def renderDualMap(project_name, layer, field, zoom, width, height,):

    os.chdir('/Users/kristincarlson/Applications/TileMill.app/Contents/Resources')
    bash_command = f'./index.js export {project_name} ~/Documents/MapBox/export/{project_name}_TT_{layer}_{field}.png --format=png --width={width} ' \
                   f'--height={height} --static_zoom={zoom} --verbose'
    p4 = subprocess.Popen(bash_command, shell=True)
    p4.communicate()


if __name__ == '__main__':

    # Get user input
    project_name = input("Enter project name: ")

    workflow = input("Enter workflow, access to jobs (primal) OR access to non-work destinations (dual): ")
    if workflow == 'primal':
        bs_switch = input("Make cumulative accessibility to jobs baseline maps? (Y/N) ")
        alt_switch = input("Make cumulative accessibility to jobs alternative maps? (Y/N) ")
        chg_switch = input("Make absolute CHANGE accessibility to jobs maps? (Y/N) ")
        pct_switch = input("Make PERCENT change accessibility to jobs maps? (Y/N) ")
        options = [bs_switch, alt_switch, chg_switch, pct_switch]
    elif workflow == 'dual':
        bs_switch = input("Make access to non-work destinations baseline maps? (Y/N) ")
        alt_switch = input("Make access to non-work destinations alternative maps? (Y/N) ")
        chg_switch = input("Make absolute CHANGE to non-work destinations maps? (Y/N) ")
        pct_switch = input("Make PERCENT change to non-work destinations maps? (Y/N) ")
        options = [bs_switch, alt_switch, chg_switch, pct_switch]
    else:
        print("Please enter 'primal' or 'dual'")
        options = [None]

    zoom = input("Type zoom level 1-30 (12): ") or 12
    width = input("Enter width in pixels (1600): ") or 1600
    height = input("Enter height in pixels (1600): ") or 1600

    print("Note: Default zoom: 12")
    print("Note: Default image dimensions: 1600 pxl X 1600 pxl")

    layer_list = []
    
    new_layer = ''
    while new_layer != "commit":

        new_layer = input("Add accessibility results layer or enter 'commit': ")
        if new_layer != "commit":
            layer_list.append(new_layer)

    print("Layer list: ", layer_list)


    main(project_name, layer_list, zoom, width, height, workflow, options)
    print(datetime.now())
    os.system('say "mapping complete"')


