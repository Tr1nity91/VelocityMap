#!/usr/bin/env python

from CodernityDB.database import Database
from CodernityDB.tree_index import TreeBasedIndex

import time
start_time = time.time()


# Database index description
class WithXIndex(TreeBasedIndex):

    def __init__(self, *args, **kwargs):
        kwargs['node_capacity'] = 15
        kwargs['key_format'] = 'I'
        super(WithXIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        t_val = data.get('x')
        if t_val is not None:
            return t_val, None
        return None

    def make_key(self, key):
        return key


# Database data input from CSV
class ParserCSV:

    # List of all CSV files
    csv_list = []

    # Get all CSV files
    def get_csv_files(self):
        import os

        dir_path = os.path.dirname(os.path.realpath(__file__))

        for subdir, dirs, files in os.walk(dir_path):
            for file in files:
                if file.endswith(".csv"):
                    self.csv_list.append(os.path.join(subdir, file))

    # Display all CSV files
    def print_csv_files(self):
        print self.csv_list

    # Import data from CSV files to database
    def db_import(self, db):
        import csv
        import re
        import struct

        i = 0
        while self.csv_list:
            with open(self.csv_list.pop(), 'r') as f:
                reader = csv.reader(f)

                # Create vehicle id in following format: <areaYYYYMMDD_filename>
                v_id = f.name.rsplit('\\', 2)[1] + \
                    re.sub('\.csv$', '', f.name.rsplit('\\', 2)[2])

                # Database import
                for row in reader:
                    i += 1
                    values = (v_id, str(row[0]), float(row[1]), float(row[2]))
                    s = struct.Struct(str(len(v_id)) + 's ' + str(len(row[0])) + 's ' + 'f f')
                    packed_data = s.pack(*values)
                    db.insert(dict(x=i, vdata=packed_data, vmeta=str(len(v_id)) + 's ' + str(len(row[0])) + 's ' + 'f f'))


def main():

    # Crate database
    db = Database('/tmp/trafficDB')
    db.create()
    x_ind = WithXIndex(db.path, 'x')
    db.add_index(x_ind)

    # Import data from CSV files to database
    parser = ParserCSV()
    parser.get_csv_files()
    parser.db_import(db)

    # Display total number of records in database
    print db.count(db.all, 'id')
    print("--- %s seconds ---" % (time.time() - start_time))

    # db.destroy()

if __name__ == '__main__':
    main()