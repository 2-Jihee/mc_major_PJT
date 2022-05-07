import requests
import codecs
from pathlib import Path
from time import sleep

from db.data_loc import data_dir
from db.mois import resp_encoding, save_encoding, source_name, admin_div_num_list, jr_admin_div_num_dict

data_name_dict = {
    'birth': 'birth',              # birth: 주민등록기준 지역별 출생등록
    'death': 'death',              # death: 주민등록기준 지역별 사망말소
    'households': 'household',     # households: 지역별 세대원수별 세대수
}


def get_mois_population_etc(admin_div_numbers: list, category: str, from_year=2008, from_month=1, till_year=2021, till_month=12, population_type=None):
    if category == 'birth':
        referer = 'https://jumin.mois.go.kr/etcStatBirth.do'
    elif category == 'death':
        referer = 'https://jumin.mois.go.kr/etcStatDeath.do'
    elif category == 'households':
        referer = 'https://jumin.mois.go.kr/etcStatHouseholds.do'
    else:
        print(f">>> Unknown category '{category}'.")
        return
    data_name = data_name_dict[category]
    if category == 'households':
        if population_type == '':
            data_name += '_all'
        elif population_type == 'Y':
            data_name += '_resident'
        else:
            print(f">>> Unknown population_type '{population_type}'.")
            return

    for admin_div_num in admin_div_numbers:
        dir_path = Path(data_dir, source_name, data_name, admin_div_num)
        dir_path.mkdir(parents=True, exist_ok=True)

        if admin_div_num == '1000000000':
            sltOrgType = '1'
            sltOrgLvl1 = 'A'
            sltOrgLvl2 = 'A'
            xlsStats = '1'                                  # xlsStats -  1: 현재화면,  2: 전체시군구현황,  3: 전체읍면동현황
        else:
            sltOrgType = '2'
            if admin_div_num[2:] == '00000000':
                sltOrgLvl1 = admin_div_num
                sltOrgLvl2 = 'A'
                xlsStats = '1'
            else:
                sltOrgLvl1 = jr_admin_div_num_dict[admin_div_num]
                sltOrgLvl2 = admin_div_num
                xlsStats = '1'

        url = f'https://jumin.mois.go.kr/downloadCsvEtc.do?searchYearMonth=month&xlsStats={xlsStats}'
        headers = {
            'Accept-Language': 'en,ko-KR;q=0.9,ko;q=0.8',
            'Host': 'jumin.mois.go.kr',
            'Origin': 'https://jumin.mois.go.kr',
            'Referer': referer,
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
                    'sltOrgType': sltOrgType,                           # 1: 전국 / 2: 시도
                    'sltOrgLvl1': sltOrgLvl1,                           # A(모든 시도) or 시도 코드('1100000000'-'5000000000')
                    'sltOrgLvl2': sltOrgLvl2,                           # A(모든 시군구) of 시군구 코드
                    'searchYearStart': str(year),
                    'searchMonthStart': f'{month:02}',
                    'searchYearEnd': str(year),
                    'searchMonthEnd': f'{month:02}',
                    'sltOrderType': '1',
                    'sltOrderValue': 'ASC',                             # 데이터 정렬 방식: ASC
                    'category': category,
                }
                if category == 'households':
                    data.update({'sltUndefType': population_type})      # '': 전체, 'Y': 거주자

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


def get_mois_birth(admin_div_numbers: list, from_year=2008, from_month=1, till_year=2021, till_month=12):
    get_mois_population_etc(admin_div_numbers, 'birth', from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_death(admin_div_numbers: list, from_year=2008, from_month=1, till_year=2021, till_month=12):
    get_mois_population_etc(admin_div_numbers, 'death', from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_household_all(admin_div_numbers: list, from_year=2008, from_month=1, till_year=2021, till_month=12):
    get_mois_population_etc(admin_div_numbers, 'households', from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month, population_type='')


def get_mois_household_resident(admin_div_numbers: list, from_year=2010, from_month=1, till_year=2021, till_month=12):
    get_mois_population_etc(admin_div_numbers, 'households', from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month, population_type='Y')
