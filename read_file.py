import mariadb
import sys
import csv
import pandas as pd
from db.connector import db_connect, db_execute
from db.data import insert_dict


with open('C:/JupyterProject/pandas_data/read_csv_sample.csv', mode='r') as sample:
    reader = csv.reader(sample)
    next(reader)
    dict_csv = {rows[0]:rows[1] for rows in reader}

csv_col_name = list(dict_csv.keys())
csv_value = list(dict_csv.values())
mapping_dict = {'loc' : csv_col_name}
# mapping_dict = {'loc': csv_col_name, 'num': csv_value}

db_col = list(mapping_dict.keys())
db_value = list(mapping_dict.values())

conn = db_connect('csvDB')
insert_success = insert_dict(conn, 'humans', mapping_dict)

conn.commit()