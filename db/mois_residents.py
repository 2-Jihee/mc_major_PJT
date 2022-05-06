import requests
import codecs
import csv
from pathlib import Path
from time import sleep

from db.data_loc import data_dir

resp_encoding = 'ansi'
save_encoding = 'utf-8'
source_name = 'mois'
data_name = 'residents'
admin_div_num_list = [
    '1000000000',
    '1100000000',
    '2600000000',
    '2700000000',
    '2800000000',
    '2900000000',
    '3000000000',
    '3100000000',
    '3600000000',
    '4100000000',
    '4200000000',
    '4300000000',
    '4400000000',
    '4500000000',
    '4600000000',
    '4700000000',
    '4800000000',
    '5000000000',
]

for admin_div_num in admin_div_num_list:
    dir_path = Path(data_dir, source_name, data_name, admin_div_num)
    dir_path.mkdir(parents=True, exist_ok=True)

    # xlsStats -  1: 현재화면,  2: 전체시군구현황,  3: 전체읍면동현황
    if admin_div_num == '1000000000':
        sltOrgType = '1'
        sltOrgLvl1 = 'A'
        xlsStats = '1'
    else:
        sltOrgType = '2'
        sltOrgLvl1 = admin_div_num
        xlsStats = '2'

    url = f'https://jumin.mois.go.kr/downloadCsvAge.do?searchYearMonth=month&xlsStats={xlsStats}'
    headers = {
        'Accept-Language': 'en,ko-KR;q=0.9,ko;q=0.8',
        'Host': 'jumin.mois.go.kr',
        'Origin': 'https://jumin.mois.go.kr',
        'Referer': 'https://jumin.mois.go.kr/ageStatMonth.do',
    }
    cnt_per_admin_div = 0
    for year in range(2008, 2022):
        for month in range(1, 13):
            data = {
                'sltOrgType': sltOrgType,               # 1: 전국 / 2: 시도
                'sltOrgLvl1': sltOrgLvl1,               # A: 모든 시도 / '1100000000'-'5000000000'(시도 코드)
                'sltOrgLvl2': 'A',                      # A: 시군구 전체 / 시군구 코드
                'gender': 'gender',                     # '성별' 표시
                'sum': 'sum',                           # '계' 표시
                'sltUndefType': 'Y',                    # '': 전체, 'Y': 거주자, 'N': 거주불명자, 'O': 재외국민
                'searchYearStart': str(year),
                'searchMonthStart': f'{month:02}',
                'searchYearEnd': str(year),
                'searchMonthEnd': f'{month:02}',
                'sltOrderType': '1',
                'sltOrderValue': 'ASC',                 # 데이터 정렬 방식: ASC
                'sltArgTypes': '1',                     # 나이 단위: 1, 5, 10
                'sltArgTypeA': '0',                     # 나이 시작: 0-100
                'sltArgTypeB': '100',                   # 나이 끝: 0-100
                'category': 'month',                    # 데이터 단위: month / year
            }

            resp = requests.post(url, data, headers=headers)
            decoded_content = resp.content.decode(resp_encoding)

            fname = f'{admin_div_num}_{year}_{month}.csv'
            file_path = dir_path / fname
            with codecs.open(file_path.as_posix(), mode='x', encoding=save_encoding) as file:
                file.write(decoded_content)
            cnt_per_admin_div += 1
            print(f">>> Saved '{fname}' under '{dir_path}'.")
            sleep(5)
    print(f"Saved {cnt_per_admin_div} files for '{admin_div_num}'")
