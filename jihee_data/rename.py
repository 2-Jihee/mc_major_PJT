import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

file_path = 'C:/Users/USER/Desktop/멀티캠퍼스 강의/전공 프로젝트/Data/kosis/movement_age/5000000000'
file_names = os.listdir(file_path)
print(file_names)

mydate = datetime(2022, 3, 1)
for name in file_names:
    original = os.path.join(file_path, name)
    change = '5000000000_' + mydate.strftime('%Y_%m') + '_movement' + '.csv'
    change = os.path.join(file_path, change)
    os.rename( original, change)
    mydate = mydate - relativedelta(months=1)
print('파일 이름이 변경되었습니다.')
file_names = os.listdir(file_path)
print(file_names)