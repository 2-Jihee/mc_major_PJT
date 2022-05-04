import mariadb
import sys
import csv
import re
import pandas as pd

from pathlib import Path

from db.connector import db_connect, db_execute, db_executemany

from db.data import insert_dict, insert_many

data_dir = 'C:/Users/YJ/SynologyDrive'
path = 'C:/Users/YJ/SynologyDrive/test/시도_성_연령_각세_별_사망자수_2019.csv'
data =[]

mapping = {'시도별':'admin_div_num',
           '성별':'gender',
           '연령(5세,각세)별':'age',
           '2019':'deaths',
           '2020': 'deaths'}

def read_csv_mapping(path):
    file = open(path)
    r_data = csv.reader(file)
    header= next(r_data)
    year = header[-1]
    header = [mapping.get(item,item) for item in header]

    for i in r_data:
        dict1 = dict(zip(header,i))
        dict1['year'] = year
        data.append(dict1)

    header = list(dict1.keys())

    return data, header

def read_csv_as_dict_list(file_path: Path):
    data = []
    with open(file_path) as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)

    return data


f_path = Path(data_dir, 'test', '시도_성_연령_각세_별_사망자수_2019.csv')
# data = read_csv_as_dict_list(f_path)
'''
lst1 =[] 
# for row in data:
#     row['admin_div_num'] = row.pop('시도별')
#     lst1.append(row)
# print(lst1)
'''
# read_csv_mapping(path)
result = read_csv_mapping(path)

lst1 =result[0]
header = result[1]

admin_div_num ={}
div_code=[]
div_name=[]
#query1 ='INSERT INTO annual_deaths (admin_div_num,year,age,gender,deaths) VALUES (1000000000,2019,13,"M",1)'
#query2 ='Delete from annual_deaths'
query3 = 'Select admin_div_num, name from admin_division '

conn = db_connect('kor_population')
cur = conn.cursor()


cur.execute(query3)
result = cur.fetchall()


for r_data in result:
    div_code.append(r_data[0])
    div_name.append(r_data[1])

admin_div_num = dict(zip(div_name,div_code))

# for문으로 admin_div_num value num으로 바꾸기
for dict in lst1:
    #if 구문 사용해서 gender T,F,M로 바꾸기
    if dict['gender'] == '계':
        dict['gender'] = 'T'
    elif dict['gender'] == '남자':
        dict['gender'] = 'M'
    else:
        dict['gender'] = 'F'
    #age '%세' 제거
    if dict['age'] == '100세이상':
        dict['age'] = 100
    elif dict['age'] == '연령미상':
        dict['age'] = -1
    else:
        dict['age'] = dict['age'][:-1]


    for k,v in dict.items():
        if v in admin_div_num : dict[k]=admin_div_num[v]


def insert_many(conn: mariadb.connection, db_table: str, columns: list, many_values: list):
    # many_values is a list of values list
    select_str = list_to_select(columns)
    placeholder_str = ", ".join(['?'] * len(columns))
    # insert_many = [ tuple(y) for i in many_values for x,y in i.items()]

    insert_data = [tuple(d[k]for k in columns) for d in many_values]
    print(insert_data)

    query = f"INSERT INTO {db_table} ({select_str}) VALUES ({placeholder_str})"
    cur = conn.cursor()
    print(query)
    insert_success = db_executemany(cur, query, insert_data)

    return insert_success

## age ,death, year 값 int로 변환
for i in lst1:
    i['year'] = int(i['year'])
    i['deaths'] = int(i['deaths'])
    i['age'] = int(i['age'])

insert_many(conn,'annual_deaths',header,lst1)


cur.close()
conn.commit()
conn.close()

# conn = db_connect('kor_population')
# cur = conn.cursor()
#
# insert_many(conn,'annual_deaths',)

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
# db_value = list(mapping_dict.values())#
# conn = db_connect('csvDB')
# insert_success = insert_dict(conn, 'humans', mapping_dict)
#
# conn.commit()
