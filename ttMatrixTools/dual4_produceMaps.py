# This script takes maps generated using the autoTilemill.py script and existing legends to produce final images with
# map, legend, and title.

# The program functions by reading the name of the raw map images, and based on the name, selects the legend type and
# additional information to compose the map with.

# Note: user must provide a config json file for the scneario combinations and appropriate text for maps
# See /Users/kristincarlson/Dropbox/AO_Projects/TIRP/Mapping/09112020/scenario_config.json

# If this program is used outside of the TIRP project, there are some hard coded values that will need to be changed in this
# script when it comes to reading the Tilemill map files and converting them into information that determines how the image is
# compiled

# Assumption: This program expects the raw map file name to be of the form: TIRP_TT_goldrush_79_grocery_abschg3.png
# Specifically that the scenario (goldrush), time (79), poi (grocery), and measure (abschg3) are separated.

#################################
#           IMPORTS             #
#################################
from datetime import datetime
from progress import bar
from PIL import Image, ImageDraw, ImageFont
from glob import glob
import json



#################################
#           CLASSES           #
#################################

class ProgressBar:
    def __init__(self, lines):
        self.bar = bar.Bar(message ='Processing', fill='@', suffix='%(percent)d%%', max=lines)
    def add_progress(self):
        self.bar.next()
    def end_progress(self):
        self.bar.finish()

# Name split should be [project (0), type (1), scen (2), time (3), poi (4), measure (5-6)
class MapObject:
    def __init__(self, file_name, scen_name_dict, poi_dict):
        self.file_name = file_name
        name_split0 = file_name.split('/')
        name_split = name_split0[-1].split('_')
        remove_ext = name_split[-1].split('.')
        name_split[-1] = remove_ext[0] # Remove the extension .png, .jpeg etc.
        print("Name split: ", name_split)
        for p_short, p_long in poi_dict.items():
            if p_short in name_split[4]:
                self.poi_short = p_short
                self.poi_long = p_long

        # print('poi: ', self.poi)

        self.scen_bs = scen_name_dict["scenarios"][name_split[2]]["scen_bs"]
        self.scen_updt = scen_name_dict["scenarios"][name_split[2]]["scen_updt"]
        self.scen_chg = scen_name_dict["scenarios"][name_split[2]]["scen_chg"]
        self.scen_short = scen_name_dict["scenarios"][name_split[2]]["scen_short"]

        print(scen_name_dict["scenarios"][name_split[2]]["scen_bs"], scen_name_dict["scenarios"][name_split[2]]["scen_updt"],
              scen_name_dict["scenarios"][name_split[2]]["scen_chg"], scen_name_dict["scenarios"][name_split[2]]["scen_short"])

            # Don't delete this until you have moved away from the file name format used in BDE analysis
            # if 'bde' in name_split[2]:
            #     self.scen_bs = 'Funded baseline'
            #     self.scen_updt = scen_name_dict['bde']
            #     self.scen_chg = scen_name_dict['bde']
            #     self.scen_short = 'bde'
            # elif 'bd' in name_split[2] and 'bde' not in name_split[2]:
            #     self.scen_bs = 'B Line, D Line'
            #     self.scen_updt = scen_name_dict['bde']
            #     self.scen_chg = scen_name_dict['bd']
            #     self.scen_short = 'bd'
            # elif 'be' in name_split[2]:
            #     self.scen_bs = 'B Line, E Line'
            #     self.scen_updt = scen_name_dict['bde']
            #     self.scen_chg = scen_name_dict['be']
            #     self.scen_short = 'be'
            # elif 'de' in name_split[2] and 'bde' not in name_split[2]:
            #     self.scen_bs = 'D Line, E Line'
            #     self.scen_updt = scen_name_dict['bde']
            #     self.scen_chg = scen_name_dict['de']
            #     self.scen_short = 'de'

        # print('scen: ', self.scen)

        if '79' in name_split[3] and 'pm' not in name_split[3]:
            self.time = "7:00 – 9:00 AM"
            self.time_short = '79'
        elif '111' in name_split[3]:
            self.time = "11:00 AM – 1:00 PM"
            self.time_short = '111'
        elif '46' in name_split[3]:
            self.time = "4:00 – 6:00 PM"
            self.time_short = '46'
        elif '79pm' in name_split[3]:
            self.time = "7:00 – 9:00 PM"
            self.time_short = '79pm'

        # print('time: ', self.time)

        if 'bs' in name_split[5] and 'chg' not in name_split[5]:
            self.title = 'Minimum travel time'
            self.type = 'bs'
        elif 'updt' in name_split[5]:
            self.title = 'Minimum travel time'
            self.type = 'updt'
        elif 'abschg' in name_split[5]:
            self.title = 'Change in minimum travel time'
            self.type = 'abschg'
        elif 'pctchg' in name_split[5]:
            self.title = 'Change in minimum travel time'
            self.type = 'pctchg'

        # print('type: ', self.type)

        if len(name_split) > 6:
            self.destnum = name_split[6]
        else:
            letters = name_split[5].split('g')
            self.destnum = letters[-1]

        # print("destnum: ", self.destnum)



#################################
#           FUNCTIONS           #
#################################
def make_lists(input):
    out = []
    for i in input:
        out.append(str(i))
    return out

def make_scenario_name_dict(dir):
    with open(f'{dir}/scenario_config.json') as json_file:
        scen_dict = json.load(json_file)
        print(scen_dict)
    return scen_dict

def make_poi_name_dict(dir):
    with open(f'{dir}/poi_names.json') as json_file:
        poi_dict = json.load(json_file)
        print(poi_dict)
    return poi_dict

def get_legend(map_type, map_poi, pxm_abs, pxm_abschg, pxm_pctchg):
    legend = None
    if map_type == 'bs' or map_type == 'updt':
        for legend_file in pxm_abs:
            if map_poi in legend_file:
                legend = legend_file
    # abschg
    elif map_type == 'pctchg':
        for legend_file in pxm_pctchg:
            if map_poi in legend_file:
                legend = legend_file
    else:
        for legend_file in pxm_abschg:
            if map_poi in legend_file:
                legend = legend_file
    return legend

#################################
#           OPERATIONS          #
#################################

if __name__ == '__main__':

    print(datetime.now())

    raw_maps = input("Type path to where the autoTilemill maps are located (default provided): ") or "/Users/kristincarlson/Dropbox/AO_Projects/TIRP/Mapping/09112020"
    print("Raw map folder: ", raw_maps)
    raw_maps_list = glob(f"{raw_maps}/*.png")

    output_folder = input("Type path to where the map images will be placed (default provided): ") or "/Users/kristincarlson/Dropbox/AO_Projects/TIRP/Mapping/09112020_finished"
    print('Output folder: ', output_folder)

    pxm = input("Type path to where the pixelmator template JPEG files are located (default provided): ") or "/Users/kristincarlson/Dropbox/AO_Projects/TIRP/Mapping/pxm_templates"
    print("Pixelmator template folder: ", pxm)

    pxm_abs = glob("/Users/kristincarlson/Dropbox/AO_Projects/TIRP/Mapping/pxm_templates/dual-abs-*-template.jpeg")  # creates list of file paths to each template

    pxm_abschg = glob("/Users/kristincarlson/Dropbox/AO_Projects/TIRP/Mapping/pxm_templates/dual-abschg-*-template.jpeg")

    pxm_pctchg = glob("/Users/kristincarlson/Dropbox/AO_Projects/TIRP/Mapping/pxm_templates/dual-pctchg-*-template.jpeg")

    print("Making dictionary mapping abreviations to scnenario headings")
    scen_name_dict = make_scenario_name_dict(raw_maps)

    print("Making dictionary mapping abbreviated poi names to heading text")
    poi_dict = make_poi_name_dict(raw_maps)  #raw_maps holds the general directory to the poi_names file

    print("Setting font...")
    font_title = ImageFont.truetype(r'/System/Library/Fonts/Avenir Next.ttc', 100, index=0)
    font_subtitle = ImageFont.truetype(r'/System/Library/Fonts/Avenir Next.ttc', 80, index=3)  # Font index 2, 5, 7

    for f in raw_maps_list:
        map = MapObject(f, scen_name_dict, poi_dict)
        legend = get_legend(map.type, map.poi_short, pxm_abs, pxm_abschg, pxm_pctchg)
        with Image.open(legend) as lg:
            draw_lg = ImageDraw.Draw(lg)
            map_img = Image.open(f)
            lg.paste(map_img, (10, 440))  # Leave 10px from left side and 10px from bottom
            # The if/else statement changes where the carriage return is placed in the title
            if map.type == 'bs':
                draw_lg.text((10, 10), f"{map.title} \nto {map.destnum} {map.poi_long} destinations", font=font_title, fill=(0, 0, 0, 255))
                draw_lg.text((10, 250), f"{map.scen_bs} \n{map.time}", font=font_subtitle, fill=(0, 0, 0, 255))
            elif map.type == 'updt':
                draw_lg.text((10, 10), f"{map.title} \nto {map.destnum} {map.poi_long} destinations", font=font_title, fill=(0, 0, 0, 255))
                draw_lg.text((10, 250), f"{map.scen_updt} \n{map.time}", font=font_subtitle, fill=(0, 0, 0, 255))
            elif map.type == 'abschg':
                draw_lg.text((10,10), f"{map.title} \nto {map.destnum} {map.poi_long} destinations", font=font_title, fill=(0,0,0,255))
                draw_lg.text((10,250), f"{map.scen_chg} \n{map.time}", font = font_subtitle, fill=(0,0,0,255))
            elif map.type == 'pctchg':
                draw_lg.text((10,10), f"{map.title} \nto {map.destnum} {map.poi_long} destinations", font=font_title, fill=(0,0,0,255))
                draw_lg.text((10,250), f"{map.scen_chg} \n{map.time}", font = font_subtitle, fill=(0,0,0,255))
            out_name = f"{map.scen_short}_{map.time_short}_{map.poi_short}_{map.type}_{map.destnum}.png"
            lg.save(f"{output_folder}/{out_name}")
            print("Saved map image", out_name)

    print(datetime.now())





