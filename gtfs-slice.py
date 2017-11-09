import pygtfs
import fiona
import argparse
import os
from collections import OrderedDict

SHAPE_PROPS = OrderedDict([ ('shape_id', 'str'),
                            ('route', 'str'),
                            ('n_trips', 'int') ])
STOP_PROPS = OrderedDict([ ('stop_id', 'str'),
                            ('route', 'str') ])
CRS_WGS84 = {'no_defs': True, 'ellps': 'WGS84', 'datum': 'WGS84', 'proj': 'longlat'}


def build_shape_data(G):
    shape_data = {}

    # Build shape IDs and geometry
    for p in G.shapes:
        shape_id = p.shape_id
        seq = p.shape_pt_sequence
        x = p.shape_pt_lon
        y = p.shape_pt_lat

        if shape_id in shape_data:
            shape_data[shape_id]['points'].append((seq, (x, y)))
        else:
            d = {}
            d['shape_id'] = shape_id
            d['points'] = [(seq, (x, y))]
            d['n_trips'] = 0
            shape_data[shape_id] = d

    # Tag shapes with data from routes/trips
    for t in G.trips:
        shape_id = t.shape_id
        route = t.route

        if shape_id in shape_data:
            shape_data[shape_id]['n_trips'] += 1
            shape_data[shape_id]['route'] = route.route_id
        else:
            print("WARNING: Trip {} uses shape ID {} which does not exist".format(t.trip_id, shape_id))

    return shape_data


def slice_shapes(G, output):
    print("Collecting shape data...")
    shape_data = build_shape_data(G)

    output_filename = "{}-shapes.shp".format(output)

    print("Writing shape data...")
    with fiona.open(output_filename,
                    'w',
                    driver='ESRI Shapefile',
                    crs=CRS_WGS84,
                    schema={'geometry': 'LineString',
                            'properties': SHAPE_PROPS}) as sf:
        for shape_id, data in shape_data.items():
            props = {'shape_id': shape_id,
                     'route': data['route'],
                     'n_trips': data['n_trips']}
            geom = {'type': 'LineString',
                    'coordinates': [x[1] for x in sorted(data['points'])]}
            sf.write({'properties': props,
                      'geometry': geom})
        

def build_stop_data(G):
    stop_records = []

    for route in G.routes:
        stop_ids = set()
        for trip in route.trips:
            for stop_time in trip.stop_times:
                stop_ids.add(stop_time.stop_id)

        for stop_id in stop_ids:
            stop = G.stops_by_id(stop_id)[0]
            props = {'stop_id': stop_id,
                     'route': route.route_id}
            geom = {'type': 'Point',
                    'coordinates': (stop.stop_lon, stop.stop_lat)}
            stop_records.append({'properties': props,
                                 'geometry': geom})

    return stop_records


def slice_stops(G, output):
    print("Collecting stop data...")
    stop_records = build_stop_data(G)

    output_filename = "{}-stops.shp".format(output)

    print("Writing stop data...")
    with fiona.open(output_filename,
                    'w',
                    driver='ESRI Shapefile',
                    crs=CRS_WGS84,
                    schema={'geometry': 'Point',
                            'properties': STOP_PROPS}) as sf:
        for stop_record in stop_records:
            sf.write(stop_record)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--gtfs', required=True)
    parser.add_argument('-o', '--output', required=False)
    args = parser.parse_args()

    if args.output:
        output = args.output
    else:
        base = os.path.basename(args.gtfs)
        output = os.path.splitext(base)[0]

    G = pygtfs.Schedule(":memory:")
    pygtfs.append_feed(G, args.gtfs)

    slice_shapes(G, output)
    slice_stops(G, output)
