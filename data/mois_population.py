import requests
import codecs
from pathlib import Path
from time import sleep

from data.data_loc import data_dir
from data.mois import resp_encoding, save_encoding, source_name, admin_div_num_list, jr_admin_div_num_dict

data_name_dict = {
    '': 'population_all',
    'Y': 'population_resident',
    'N': 'population_unknown',
    'O': 'population_overseas',
}


def get_mois_population(admin_div_numbers: list, resident_type: str, from_year=2008, from_month=1, till_year=2021, till_month=12):
    data_name = data_name_dict[resident_type]
    for admin_div_num in admin_div_numbers:
        dir_path = Path(data_dir, source_name, data_name, admin_div_num)
        dir_path.mkdir(parents=True, exist_ok=True)

        if admin_div_num == '0000000000':
            sltOrgType = '1'
            sltOrgLvl1 = 'A'
            sltOrgLvl2 = 'A'
            xlsStats = '1'                                  # xlsStats -  1: 현재화면,  2: 전체시군구현황,  3: 전체읍면동현황
        else:
            sltOrgType = '2'
            if admin_div_num[2:] == '0' * 8:
                sltOrgLvl1 = admin_div_num
                sltOrgLvl2 = 'A'
                xlsStats = '1'
            else:
                sltOrgLvl1 = jr_admin_div_num_dict[admin_div_num]
                sltOrgLvl2 = admin_div_num
                xlsStats = '1'

        url = f'https://jumin.mois.go.kr/downloadCsvAge.do?searchYearMonth=month&xlsStats={xlsStats}'
        headers = {
            'Accept-Language': 'en,ko-KR;q=0.9,ko;q=0.8',
            'Host': 'jumin.mois.go.kr',
            'Origin': 'https://jumin.mois.go.kr',
            'Referer': 'https://jumin.mois.go.kr/ageStatMonth.do',
        }
        cnt_per_admin_div = 0
        for year in range(from_year, till_year + 1):
            if year == from_year:
                start_month = from_month
            else:
                start_month = 1
            if year == till_year:
                end_month = till_month
            else:
                end_month = 12

            for month in range(start_month, end_month + 1):
                data = {
                    'sltOrgType': sltOrgType,               # 1: 전국 / 2: 시도
                    'sltOrgLvl1': sltOrgLvl1,               # A(모든 시도) or 시도 코드('1100000000'-'5000000000')
                    'sltOrgLvl2': sltOrgLvl2,               # A(모든 시군구) of 시군구 코드
                    'gender': 'gender',                     # '성별' 표시
                    'sum': 'sum',                           # '계' 표시
                    'sltUndefType': resident_type,        # '': 전체, 'Y': 거주자, 'N': 거주불명자, 'O': 재외국민
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

                resp = requests.post(url, data, headers=headers, timeout=5)
                decoded_content = resp.content.decode(resp_encoding)

                fname = f'{admin_div_num}_{year}_{month}_{data_name}.csv'
                file_path = dir_path / fname
                with codecs.open(file_path.as_posix(), mode='x', encoding=save_encoding) as file:
                    file.write(decoded_content)
                cnt_per_admin_div += 1
                print(f">>> Saved '{fname}' under '{dir_path}'.")
                sleep(3)
        print(f">>> Saved {cnt_per_admin_div} files for '{admin_div_num}'")
        print()


def get_mois_population_all(admin_div_numbers: list, from_year=2008, from_month=1, till_year=2021, till_month=12):
    get_mois_population(admin_div_numbers, '', from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_population_resident(admin_div_numbers: list, from_year=2010, from_month=10, till_year=2021, till_month=12):
    get_mois_population(admin_div_numbers, 'Y', from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_population_unknown(admin_div_numbers: list, from_year=2010, from_month=10, till_year=2021, till_month=12):
    get_mois_population(admin_div_numbers, 'N', from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_population_overseas(admin_div_numbers: list, from_year=2015, from_month=1, till_year=2021, till_month=12):
    get_mois_population(admin_div_numbers, 'O', from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)
