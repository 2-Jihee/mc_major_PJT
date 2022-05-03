import mariadb
import sys
import csv
import re
import pandas as pd

from pathlib import Path

from db.connector import db_connect, db_execute
from db.data import insert_dict

data_dir = 'C:/Phoenix Data/'


def read_csv_as_dict_list(file_path: Path):
    data = []
    with open(file_path) as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)

    return data


f_path = Path(data_dir, 'test', '시도_성_연령_각세_별_사망자수_2019.csv')
data = read_csv_as_dict_list(f_path)
for item in data:
    print(item)




# with open('C:/JupyterProject/pandas_data/read_csv_sample.csv', mode='r') as sample:
#     reader = csv.reader(sample)
#     next(reader)
#     dict_csv = {rows[0]:rows[1] for rows in reader}
#
# csv_col_name = list(dict_csv.keys())
# csv_value = list(dict_csv.values())
# mapping_dict = {'loc' : csv_col_name}
# # mapping_dict = {'loc': csv_col_name, 'num': csv_value}
#
# db_col = list(mapping_dict.keys())
# db_value = list(mapping_dict.values())
#
# conn = db_connect('csvDB')
# insert_success = insert_dict(conn, 'humans', mapping_dict)
#
# conn.commit()
