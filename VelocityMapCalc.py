#!/usr/bin/env python

from CodernityDB.database import Database
from math import radians, cos, sin, asin, sqrt

import numpy as np
import struct

import time
start_time = time.time()


# Method for distance calculation using GPS coordinates
def haversine(lon1, lat1, lon2, lat2):

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


# Velocity map description
class VelocityMap:

    # Init velocity map: 3D array of (288, 80, 60)
    # Time resolution: 300 seconds (5 minutes interval)
    # Spacial resolution: 1 sq km
    def __init__(self, time_res, spacial_res):
        self.map = np.zeros((time_res, 8 * spacial_res, 6 * spacial_res))
        self.smpl_num = np.zeros((time_res, 8 * spacial_res, 6 * spacial_res))

        # Separate entities for distance and travel time
        self.t_map = np.zeros((time_res, 8 * spacial_res, 6 * spacial_res))
        self.s_map = np.zeros((time_res, 8 * spacial_res, 6 * spacial_res))

    # Method for velocity map calculation
    def vel_map_calc(self, veh_data):
        self.veh_data = veh_data

        # Calculate sequences in the same cells
        num_t = [sub_list[0] for sub_list in veh_data]
        num_x = [sub_list[1] for sub_list in veh_data]
        num_y = [sub_list[2] for sub_list in veh_data]

        tt = num_t[0]
        xx = num_x[0]
        yy = num_y[0]

        res = []

        i = 0

        # Calculate sequence in the same MESH cell
        for x in xrange(len(num_t)):
            if num_t[x] == tt and num_x[x] == xx and num_y[x] == yy:
                i += 1
            else:
                res.append(i)
                tt = num_t[x]
                xx = num_x[x]
                yy = num_y[x]
                i = 1

        # If all the samples in the same mesh cell
        if not res:
            res.append(len(num_t))

        i = 0
        dist = 0
        t_time = 0
        trip_x = []
        trip_y = []

        # Go through all the sequences and calculate average velocity
        for x in xrange(len(res)):

            for y in xrange(res[x]):
                trip_x.append(veh_data[i][4])
                trip_y.append(veh_data[i][5])
                i += 1

            dist = 0

            if len(trip_x) >= 2:
                for z in xrange(len(trip_x) - 1):
                    dist += haversine(trip_x[z], trip_y[z], trip_x[z + 1], trip_y[z + 1])

            # TODO: Check if correct!!!!!
            self.s_map[veh_data[i][0], veh_data[i][1], veh_data[i][2]] += dist
            self.t_map[veh_data[i][0], veh_data[i][1], veh_data[i][2]] += len(trip_x) * 60 * 60

            self.map[veh_data[i][0], veh_data[i][1], veh_data[i][2]] += dist / len(trip_x) * 60 * 60
            self.smpl_num[veh_data[i][0], veh_data[i][1], veh_data[i][2]] += 1

            # Print real time
            # m, s = divmod(veh_data[i][3], 60)
            # h, m = divmod(m, 60)
            # print "%d:%02d:%02d" % (h, m, s)

        for t in xrange(288):
            for x in xrange(80):
                for y in xrange(60):
                    if self.smpl_num[veh_data[x][0], veh_data[x][1], veh_data[x][2]] >= 1:
                        self.map[veh_data[x][0], veh_data[x][1], veh_data[x][2]] = \
                            self.map[veh_data[x][0], veh_data[x][1], veh_data[x][2]] \
                                / self.smpl_num[veh_data[x][0], veh_data[x][1], veh_data[x][2]]

        return self.map


# Database description
class DB:

    def __init__(self, db):
        self.num_rec = db.count(db.all, 'id')
        self.db = db

    # Display number of records in database
    def display_num_rec(self):
        print "Number of records in database: " + str(self.num_rec)

    # Get all vehicle ID's from database
    def get_all_id(self):
        # List of all vehicle IDs
        id_list = []

        for curr in self.db.all('id', with_doc=False):
            s = struct.Struct(curr['vmeta'])
            # unpacked data format: <id, YYYYMMHHmmss, latitude, longitude>
            unpacked_data = s.unpack(curr['vdata'])
            id_list.append(unpacked_data[0])
        id_list = list(set(id_list))

        return id_list

    # Get data for specific vehicle ID from database
    def get_data_by_id(self, vid):
        # List of all vehicle IDs
        veh_id = vid
        # List for vehicle data
        veh_data = []

        for curr in self.db.all('id', with_doc=False):
            tmp = []
            s = struct.Struct(curr['vmeta'])
            # unpacked data format: <id, YYYYMMHHmmss, latitude, longitude>
            unpacked_data = s.unpack(curr['vdata'])

            if unpacked_data[0] == veh_id:

                # Convert time to seconds
                t = str(unpacked_data[1]).split()[0][8:]
                tt = int(t[0:2]) * 60 * 60 + int(t[2:4]) * 60 + int(t[4:6])

                y = unpacked_data[2] - 35.25
                x = unpacked_data[3] - 138.875

                # Check boundary conditions
                if 0 <= x <= 1 and 0 <= y <= 0.5:

                    t = (tt / 300)

                    y = int(y / 0.008333333)
                    x = int(x / 0.0125)

                    # (t, x, y, t_position, x_position, y_position)
                    tmp = [t, x, y, tt, unpacked_data[3], unpacked_data[2]]
                    veh_data.append(tmp)

        return veh_data






def main():

    db = Database('/tmp/trafficDB')

    if db.exists():

        db.open()
        database = DB(db)

        # Create velocity map with
        # Time resolution: 300 sec
        # Spacial resolution: 1 sq km
        v_map = VelocityMap(288, 10)

        # Get vehicles id list
        vid_list = database.get_all_id()

        # Get data for specific vehicle id
        veh_data = database.get_data_by_id(vid_list[0])


        # TODO: add loop through all vehicle IDs
        # for x in xrange(len(vid_list)):
        #    veh_data = database.get_data_by_id(vid_list[x])
        #    av_vel_map = v_map.vel_map_calc(veh_data)


        av_vel_map = v_map.vel_map_calc(veh_data)

        f = open('results.txt', 'w')

        t_res = 300
        for t in xrange(288):
            for x in xrange(80):
                for y in xrange(60):
                    if av_vel_map[t, x, y] != 0:
                        f.write(str(t * t_res) + ' ' + str((t + 1) * t_res) + ' ' + str(x) + ' ' + str(y) + ' ' + str(av_vel_map[t, x, y]) + '\n')
            # f.write('\n')

        f.close()

    else:
        print "Database not found"

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()