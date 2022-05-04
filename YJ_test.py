## 손영진 교육생의 함수 테스트 모듈입니다!!

from pathlib import Path

from db.connector import db_connect, db_execute

from db.data import insert_dict

import csv

mapping = {'시도별':'admin_div_num',
           '성별':'gender',
           '연령(5세,각세)별':'age',
           '2019':'deaths',}

path = 'C:/Users/YJ/SynologyDrive/test/시도_성_연령_각세_별_사망자수_2019.csv'
f_data =[]
def read_csv_mapping(path):
    file = open(path)
    data = csv.reader(file)
    header= next(data)
    header = [mapping.get(item,item) for item in header]

    for i in data:
        dict1 = dict(zip(header,i))
        f_data.append(dict1)

    return f_data


## 계 = 'T,M,F' , '시도별' = 'admin_div_num로 바꿔야됨

read_csv_mapping(path)
print(f_data)



