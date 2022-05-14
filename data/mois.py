import codecs
import re
import requests
import pandas as pd
from requests.adapters import Retry, HTTPAdapter
from pathlib import Path
from natsort import natsorted
from time import sleep

from data.data_loc import data_dir
from db.connector import pop_db, admin_div_table, db_connect
from db.query import select_one_row_one_column, insert_dict
from pop.model import str_to_int

source_name = 'mois'
resp_encoding = 'ansi'
save_encoding = 'utf-8'

date_patt_str = r'(?P<year>\d+)년\W*(?P<month>\d+)월'
household_patt_str = r'(?P<household_size>\d+)인세대'
last_age_patt_str = r'(?P<last_age>\d+)세\W*이상'
age_patt_str = r'(?P<age>\d+)세'

admin_div_code_list = [
    '0000000000',
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
jr_admin_div_code_dict = {
    '1111000000': '1100000000',
    '1114000000': '1100000000',
    '1117000000': '1100000000',
    '1120000000': '1100000000',
    '1121500000': '1100000000',
    '1123000000': '1100000000',
    '1126000000': '1100000000',
    '1129000000': '1100000000',
    '1130500000': '1100000000',
    '1132000000': '1100000000',
    '1135000000': '1100000000',
    '1138000000': '1100000000',
    '1141000000': '1100000000',
    '1144000000': '1100000000',
    '1147000000': '1100000000',
    '1150000000': '1100000000',
    '1153000000': '1100000000',
    '1154500000': '1100000000',
    '1156000000': '1100000000',
    '1159000000': '1100000000',
    '1162000000': '1100000000',
    '1165000000': '1100000000',
    '1168000000': '1100000000',
    '1171000000': '1100000000',
    '1174000000': '1100000000',
    '2611000000': '2600000000',
    '2614000000': '2600000000',
    '2617000000': '2600000000',
    '2620000000': '2600000000',
    '2623000000': '2600000000',
    '2626000000': '2600000000',
    '2629000000': '2600000000',
    '2632000000': '2600000000',
    '2635000000': '2600000000',
    '2638000000': '2600000000',
    '2641000000': '2600000000',
    '2644000000': '2600000000',
    '2647000000': '2600000000',
    '2650000000': '2600000000',
    '2653000000': '2600000000',
    '2671000000': '2600000000',
    '2711000000': '2700000000',
    '2714000000': '2700000000',
    '2717000000': '2700000000',
    '2720000000': '2700000000',
    '2723000000': '2700000000',
    '2726000000': '2700000000',
    '2729000000': '2700000000',
    '2771000000': '2700000000',
    '2811000000': '2800000000',
    '2814000000': '2800000000',
    '2817700000': '2800000000',
    '2818500000': '2800000000',
    '2820000000': '2800000000',
    '2823700000': '2800000000',
    '2824500000': '2800000000',
    '2826000000': '2800000000',
    '2871000000': '2800000000',
    '2872000000': '2800000000',
    '2911000000': '2900000000',
    '2914000000': '2900000000',
    '2915500000': '2900000000',
    '2917000000': '2900000000',
    '2920000000': '2900000000',
    '3011000000': '3000000000',
    '3014000000': '3000000000',
    '3017000000': '3000000000',
    '3020000000': '3000000000',
    '3023000000': '3000000000',
    '3111000000': '3100000000',
    '3114000000': '3100000000',
    '3117000000': '3100000000',
    '3120000000': '3100000000',
    '3171000000': '3100000000',
    '3611000000': '3600000000',
    '4111000000': '4100000000',
    '4113000000': '4100000000',
    '4115000000': '4100000000',
    '4117000000': '4100000000',
    '4119000000': '4100000000',
    '4121000000': '4100000000',
    '4122000000': '4100000000',
    '4125000000': '4100000000',
    '4127000000': '4100000000',
    '4128000000': '4100000000',
    '4129000000': '4100000000',
    '4131000000': '4100000000',
    '4136000000': '4100000000',
    '4137000000': '4100000000',
    '4139000000': '4100000000',
    '4141000000': '4100000000',
    '4143000000': '4100000000',
    '4145000000': '4100000000',
    '4146000000': '4100000000',
    '4148000000': '4100000000',
    '4150000000': '4100000000',
    '4155000000': '4100000000',
    '4157000000': '4100000000',
    '4159000000': '4100000000',
    '4161000000': '4100000000',
    '4163000000': '4100000000',
    '4165000000': '4100000000',
    '4167000000': '4100000000',
    '4180000000': '4100000000',
    '4182000000': '4100000000',
    '4183000000': '4100000000',
    '4211000000': '4200000000',
    '4213000000': '4200000000',
    '4215000000': '4200000000',
    '4217000000': '4200000000',
    '4219000000': '4200000000',
    '4221000000': '4200000000',
    '4223000000': '4200000000',
    '4272000000': '4200000000',
    '4273000000': '4200000000',
    '4275000000': '4200000000',
    '4276000000': '4200000000',
    '4277000000': '4200000000',
    '4278000000': '4200000000',
    '4279000000': '4200000000',
    '4280000000': '4200000000',
    '4281000000': '4200000000',
    '4282000000': '4200000000',
    '4283000000': '4200000000',
    '4311000000': '4300000000',
    '4313000000': '4300000000',
    '4315000000': '4300000000',
    '4372000000': '4300000000',
    '4373000000': '4300000000',
    '4374000000': '4300000000',
    '4374500000': '4300000000',
    '4375000000': '4300000000',
    '4376000000': '4300000000',
    '4377000000': '4300000000',
    '4380000000': '4300000000',
    '4413000000': '4400000000',
    '4415000000': '4400000000',
    '4418000000': '4400000000',
    '4420000000': '4400000000',
    '4421000000': '4400000000',
    '4423000000': '4400000000',
    '4425000000': '4400000000',
    '4427000000': '4400000000',
    '4471000000': '4400000000',
    '4476000000': '4400000000',
    '4477000000': '4400000000',
    '4479000000': '4400000000',
    '4480000000': '4400000000',
    '4481000000': '4400000000',
    '4482500000': '4400000000',
    '4511000000': '4500000000',
    '4513000000': '4500000000',
    '4514000000': '4500000000',
    '4518000000': '4500000000',
    '4519000000': '4500000000',
    '4521000000': '4500000000',
    '4571000000': '4500000000',
    '4572000000': '4500000000',
    '4573000000': '4500000000',
    '4574000000': '4500000000',
    '4575000000': '4500000000',
    '4577000000': '4500000000',
    '4579000000': '4500000000',
    '4580000000': '4500000000',
    '4611000000': '4600000000',
    '4613000000': '4600000000',
    '4615000000': '4600000000',
    '4617000000': '4600000000',
    '4623000000': '4600000000',
    '4671000000': '4600000000',
    '4672000000': '4600000000',
    '4673000000': '4600000000',
    '4677000000': '4600000000',
    '4678000000': '4600000000',
    '4679000000': '4600000000',
    '4680000000': '4600000000',
    '4681000000': '4600000000',
    '4682000000': '4600000000',
    '4683000000': '4600000000',
    '4684000000': '4600000000',
    '4686000000': '4600000000',
    '4687000000': '4600000000',
    '4688000000': '4600000000',
    '4689000000': '4600000000',
    '4690000000': '4600000000',
    '4691000000': '4600000000',
    '4711000000': '4700000000',
    '4713000000': '4700000000',
    '4715000000': '4700000000',
    '4717000000': '4700000000',
    '4719000000': '4700000000',
    '4721000000': '4700000000',
    '4723000000': '4700000000',
    '4725000000': '4700000000',
    '4728000000': '4700000000',
    '4729000000': '4700000000',
    '4772000000': '4700000000',
    '4773000000': '4700000000',
    '4775000000': '4700000000',
    '4776000000': '4700000000',
    '4777000000': '4700000000',
    '4782000000': '4700000000',
    '4783000000': '4700000000',
    '4784000000': '4700000000',
    '4785000000': '4700000000',
    '4790000000': '4700000000',
    '4792000000': '4700000000',
    '4793000000': '4700000000',
    '4794000000': '4700000000',
    '4812000000': '4800000000',
    '4817000000': '4800000000',
    '4822000000': '4800000000',
    '4824000000': '4800000000',
    '4825000000': '4800000000',
    '4827000000': '4800000000',
    '4831000000': '4800000000',
    '4833000000': '4800000000',
    '4872000000': '4800000000',
    '4873000000': '4800000000',
    '4874000000': '4800000000',
    '4882000000': '4800000000',
    '4884000000': '4800000000',
    '4885000000': '4800000000',
    '4886000000': '4800000000',
    '4887000000': '4800000000',
    '4888000000': '4800000000',
    '4889000000': '4800000000',
    '5011000000': '5000000000',
    '5013000000': '5000000000',
}

data_type_table = {
    'P': 'mois_population',
    'B': 'mois_birth',
    'D': 'mois_death',
    'H': 'mois_household',
}
data_type_desc = {
    'P': 'population',              # 연령별 주민등록 인구형황
    'B': 'birth',                   # birth: 주민등록기준 지역별 출생등록
    'D': 'death',                   # death: 주민등록기준 지역별 사망말소
    'H': 'household',               # households: 지역별 세대원수별 세대수
}
resident_type_dict = {
    '-': 'all',                     # 전체
    'R': 'resident',                # resident
    'U': 'unknown',                 # unknown domicile
    'O': 'overseas',                # overseas
}
data_name_list = [
    'birth',
    'death',
    'household_all',
    'household_resident',
    'population_all',
    'population_resident',
    'population_unknown',
    'population_overseas',
]

resident_type_to_slt_undef_type = {
    '-': '',                        # '': 전체
    'R': 'Y',                       # 'Y': 거주자
    'U': 'N',                       # 'N': 거주불명자
    'O': 'O',                       # 'O': 재외국민
}
resident_type_to_col_name = {
    'R': '거주자',                  # resident
    'U': '거주불명자',              # unknown domicile
    'O': '재외국민',                # overseas
}


# # ---------- generic ------------------------------------------------------------------------------------------------------------------------

def get_data_name(data_type: str, resident_type=None):
    if data_type not in data_type_desc.keys():
        error_msg = f"data_type '{data_type}' is invalid."
        raise ValueError(error_msg)
    data_name = data_type_desc[data_type]
    if resident_type is not None:
        if resident_type not in resident_type_dict.keys():
            error_msg = f"resident_type '{resident_type}' is invalid."
            raise ValueError(error_msg)
        data_name += f"_{resident_type_dict[resident_type]}"

    if data_name not in data_name_list:
        error_msg = f"data_type '{data_type}' and resident_type '{resident_type}' are not compatible."
        raise ValueError(error_msg)

    return data_name


def get_dir_path(admin_div_code: str, data_name: str, is_detail_data):
    if admin_div_code == '0000000000' and is_detail_data:
        dir_path = Path(data_dir, source_name, '[whole_country_detail]', data_name)
    else:
        dir_path = Path(data_dir, source_name, data_name, admin_div_code)

    return dir_path


# # ---------- download data ------------------------------------------------------------------------------------------------------------------------

def get_mois_data(admin_div_codes: list, data_type: str, resident_type=None, is_detail_data=False, from_year=2008, from_month=1, till_year=2021, till_month=12):
    data_name = get_data_name(data_type, resident_type=resident_type)

    for admin_div_code in admin_div_codes:
        dir_path = get_dir_path(admin_div_code, data_name, is_detail_data)
        dir_path.mkdir(parents=True, exist_ok=True)

        if admin_div_code == '0000000000':
            slt_org_type = '1'
            slt_org_lvl1 = 'A'
            slt_org_lvl2 = 'A'
        else:
            slt_org_type = '2'
            if admin_div_code[2:] == '0'*len(admin_div_code[2:]):
                slt_org_lvl1 = admin_div_code
                slt_org_lvl2 = 'A'
            else:
                if data_type != 'P' and is_detail_data is False:
                    print(f"  > admin_div_code '{admin_div_code}' is not available with data_type '{data_type}' when is_detail_data is False.")
                    continue
                slt_org_lvl1 = jr_admin_div_code_dict[admin_div_code]
                slt_org_lvl2 = admin_div_code
        if is_detail_data:
            xls_stats = '3'              # xlsStats -  3: 전체읍면동현황 / 2: 전체시군구현황
        else:
            xls_stats = '1'              # xlsStats -  1: 현재화면

        if data_type == 'P':
            url = f'https://jumin.mois.go.kr/downloadCsvAge.do?searchYearMonth=month&xlsStats={xls_stats}'
            referer = 'https://jumin.mois.go.kr/ageStatMonth.do'
            category = 'month'
        else:
            url = f'https://jumin.mois.go.kr/downloadCsvEtc.do?searchYearMonth=month&xlsStats={xls_stats}'
            if data_type == 'B':
                referer = 'https://jumin.mois.go.kr/etcStatBirth.do'
                category = data_type_desc[data_type]
            elif data_type == 'D':
                referer = 'https://jumin.mois.go.kr/etcStatDeath.do'
                category = data_type_desc[data_type]
            elif data_type == 'H':
                referer = 'https://jumin.mois.go.kr/etcStatHouseholds.do'
                category = 'households'
            else:
                error_msg = f"data_type '{data_type}' is invalid."
                raise ValueError(error_msg)
        headers = {
            'accept-language': 'ko,en;q=0.9,ko-KR;q=0.8',
            'host': 'jumin.mois.go.kr',
            'origin': 'https://jumin.mois.go.kr',
            'referer': referer,
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
                    'sltOrgType': slt_org_type,                   # 1: 전국  /  2: 시도
                    'sltOrgLvl1': slt_org_lvl1,                   # A: 모든 시도  /  시도 코드('1100000000'-'5000000000')
                    'sltOrgLvl2': slt_org_lvl2,                   # A: 모든 시군구)  /  시군구 코
                    'category': category,                       # 데이터 유형 - year, month, birth, death, households
                    'searchYearStart': str(year),
                    'searchMonthStart': f'{month:0>2}',
                    'searchYearEnd': str(year),
                    'searchMonthEnd': f'{month:0>2}',
                    'sltOrderType': '1',                        # 데이터 정렬 기준
                    'sltOrderValue': 'ASC',                     # 데이터 정렬 방식 - ASC
                }
                if data_type in ['P', 'H']:
                    data.update({'sltUndefType': resident_type_to_slt_undef_type[resident_type]})
                if data_type == 'P':
                    data.update({
                        'gender': 'gender',                     # '성별' 표시
                        'sum': 'sum',                           # '계' 표시
                        'sltArgTypes': '1',                     # 나이 단위: 1, 5, 10
                        'sltArgTypeA': '0',                     # 나이 시작: 0-100
                        'sltArgTypeB': '100',                   # 나이 끝: 0-100
                    })

                sess = requests.Session()
                retries = Retry(total=5, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])
                sess.mount('http://', HTTPAdapter(max_retries=retries))
                sess.mount('https://', HTTPAdapter(max_retries=retries))
                timeouts = (5, 60)

                resp = sess.post(url, data, headers=headers, timeout=timeouts)
                decoded_content = resp.content.decode(resp_encoding)

                fname = f"{admin_div_code}_{year:0>4}_{month:0>2}_{data_name}{'_detail' if is_detail_data else ''}.csv"
                file_path = dir_path / fname
                with codecs.open(file_path.as_posix(), mode='x', encoding=save_encoding) as file:
                    file.write(decoded_content)
                cnt_per_admin_div += 1
                print(f"  > Saved '{fname}' under '{dir_path}'.")
                if is_detail_data:
                    sleep(30)
                else:
                    sleep(3)

        print(f">>> Saved {cnt_per_admin_div} files for '{admin_div_code}'")
        print()


def get_mois_population_all(admin_div_codes: list, is_detail_data=False, from_year=2008, from_month=1, till_year=2021, till_month=12):
    get_mois_data(admin_div_codes, 'P', resident_type='-', is_detail_data=is_detail_data, from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_population_resident(admin_div_codes: list, is_detail_data=False, from_year=2010, from_month=10, till_year=2021, till_month=12):
    get_mois_data(admin_div_codes, 'P', resident_type='R', is_detail_data=is_detail_data, from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_population_unknown(admin_div_codes: list, is_detail_data=False, from_year=2010, from_month=10, till_year=2021, till_month=12):
    get_mois_data(admin_div_codes, 'P', resident_type='U', is_detail_data=is_detail_data, from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_population_overseas(admin_div_codes: list, is_detail_data=False, from_year=2015, from_month=1, till_year=2021, till_month=12):
    get_mois_data(admin_div_codes, 'P', resident_type='O', is_detail_data=is_detail_data, from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_birth(admin_div_codes: list, is_detail_data=False, from_year=2008, from_month=1, till_year=2021, till_month=12):
    get_mois_data(admin_div_codes, 'B', from_year=from_year, is_detail_data=is_detail_data, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_death(admin_div_codes: list, is_detail_data=False, from_year=2008, from_month=1, till_year=2021, till_month=12):
    get_mois_data(admin_div_codes, 'D', from_year=from_year, is_detail_data=is_detail_data, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_household_all(admin_div_codes: list, is_detail_data=False, from_year=2008, from_month=1, till_year=2021, till_month=12):
    get_mois_data(admin_div_codes, 'H', resident_type='-', is_detail_data=is_detail_data, from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


def get_mois_household_resident(admin_div_codes: list, is_detail_data=False, from_year=2010, from_month=1, till_year=2021, till_month=12):
    get_mois_data(admin_div_codes, 'H', resident_type='R', is_detail_data=is_detail_data, from_year=from_year, from_month=from_month, till_year=till_year, till_month=till_month)


# # ---------- upload data ------------------------------------------------------------------------------------------------------------------------

def mois_idx_to_admin_div(idx: str):
    admin_div_match = re.match(r'(.+)\((\d{10})\)', idx)
    if admin_div_match:
        admin_div_name = re.sub(r'\s+', ' ', admin_div_match.group(1).strip())
        admin_div_code = admin_div_match.group(2)
        if admin_div_code == '1000000000':
            admin_div_code = '0000000000'
    else:
        admin_div_name = None
        admin_div_code = None

    return admin_div_name, admin_div_code


def upload_mois_data(admin_div_codes: list, data_type: str, resident_type=None, is_detail_data=False, pop_conn=None, from_year=None, from_month=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    data_name = get_data_name(data_type, resident_type=resident_type)
    date_patt = re.compile(date_patt_str)
    if data_type == 'P':
        last_age_patt = re.compile(last_age_patt_str)
        age_patt = re.compile(age_patt_str)
    elif data_type == 'H':
        household_patt = re.compile(household_patt_str)

    for admin_div_code in admin_div_codes:
        dir_path = get_dir_path(admin_div_code, data_name, is_detail_data)
        if not dir_path.exists():
            print(f">>> Directory '{dir_path}' does not exist for data_name '{data_name}' and admin_div_code '{admin_div_code}'.")
            print()
            continue

        fname_patt_str = fr"{admin_div_code}_(?P<year>\d+)_(?P<month>\d+)_{data_name}{'_detail' if is_detail_data else ''}.csv"
        fname_patt = re.compile(fname_patt_str)
        file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and fname_patt.fullmatch(f_path.name)]
        file_paths = natsorted(file_paths)
        for file_path in file_paths:
            fname_match = fname_patt.fullmatch(file_path.name)
            fname_year = int(fname_match.group('year'))
            fname_month = int(fname_match.group('month'))
            if from_year is not None:
                if fname_year < from_year:
                    continue
                elif fname_year == from_year and from_month is not None:
                    if fname_month < from_month:
                        continue

            # read file
            df = pd.read_csv(file_path.as_posix(), index_col=0, dtype='string')

            # update index
            new_index = []
            for idx in df.index:
                (admin_div_name, admin_div_code) = mois_idx_to_admin_div(idx)
                if '출장소' in admin_div_name:
                    new_index.append(None)
                    continue
                admin_div_num = int(admin_div_code)
                admin_div_name_full = select_one_row_one_column(pop_conn, admin_div_table, {'admin_div_num': admin_div_num}, 'name_full')
                if admin_div_name_full is None:
                    # register a new admin_div_num
                    admin_div_name_split = admin_div_name.split(' ')
                    if admin_div_num == 0:
                        admin_div_level = 0
                        senior_admin_div_num = None
                    else:
                        if admin_div_name_split[0][-1] == '시' or admin_div_name_split[-1][-1] == '군':
                            admin_div_code_parts = [admin_div_code[0:2], admin_div_code[2:5], '', admin_div_code[5:8], admin_div_code[8:]]
                        else:
                            admin_div_code_parts = [admin_div_code[0:2], admin_div_code[2:4], admin_div_code[4:5], admin_div_code[5:8], admin_div_code[8:]]
                        admin_div_level = 5             # lowest possible level
                        for i in range(len(admin_div_code_parts) - 1, -1, -1):
                            if admin_div_code_parts[i]:
                                if not re.fullmatch('0+', admin_div_code_parts[i]):
                                    admin_div_level = i + 1
                                    break

                        admin_div_code_parts[admin_div_level - 1] = '0' * len(admin_div_code_parts[admin_div_level - 1])
                        senior_admin_div_num = int(''.join(admin_div_code_parts))
                        senior_admin_div_name_full = select_one_row_one_column(pop_conn, admin_div_table, {'admin_div_num': senior_admin_div_num}, 'name_full')
                        if senior_admin_div_name_full is None:
                            error_msg = f"Unable to identify senior_admin_div_num for a new admin_div '{admin_div_name}' with admin_div_num '{admin_div_num}'."
                            raise ValueError(error_msg)
                    insert_dict(pop_conn, admin_div_table, {'admin_div_num': admin_div_num, 'name_full': admin_div_name, 'name': admin_div_name_split[-1], 'admin_div_level': admin_div_level, 'senior_admin_div_num': senior_admin_div_num})
                    pop_conn.commit()
                new_index.append(admin_div_code)
            df.index = new_index

            # columns to keys
            col_keys = []
            for col_name in df.columns:
                # check resident_type of data
                col_resident_type = None
                for key, value in resident_type_to_col_name.items():
                    if value in col_name:
                        col_resident_type = key
                        break
                if data_type in ['P', 'H'] and col_resident_type is None:
                    col_resident_type = '-'
                if col_resident_type != resident_type:
                    error_msg = f"File '{file_path.name}' contains data column '{col_name}' with resident_type '{col_resident_type}'."
                    raise ValueError(error_msg)

                # split col_name for getting date and data types
                col_name_split = col_name.split('_')

                # determine date: year/month
                date_match = date_patt.search(col_name_split[0])
                col_year = int(date_match.group('year'))
                if col_year != fname_year:
                    error_msg = f"year '{fname_year}' in filename is different from year '{col_year}' in data column '{col_name}'."
                    raise ValueError(error_msg)
                col_month = int(date_match.group('month'))
                if col_month != fname_month:
                    error_msg = f"month '{fname_month}' in filename is different from month '{col_month}' in data column '{col_name}'."
                    raise ValueError(error_msg)

                # get column types
                if data_type == 'P':
                    # skip col_key that contains redundant data
                    if '연령구간' in col_name:
                        col_keys.append(None)
                        continue

                    # determine gender_type
                    if col_name_split[1] == '남':
                        gender_type = 'M'
                    elif col_name_split[1] == '여':
                        gender_type = 'F'
                    elif col_name_split[1] == '계' or col_name_split[1] in resident_type_to_col_name.values():
                        gender_type = 'T'
                    else:
                        error_msg = f"Unknown gender_type in data column '{col_name}'."
                        raise ValueError(error_msg)

                    # determine age type
                    if col_name_split[-1] == '총인구수':
                        age_type = 'age_total'
                    else:
                        last_age_match = last_age_patt.search(col_name_split[-1])
                        age_match = age_patt.search(col_name_split[-1])
                        if last_age_match:
                            age_type = f"age_{int(last_age_match.group('last_age'))}+"
                        elif age_match:
                            age_type = f"age_{int(age_match.group('age'))}"
                        else:
                            error_msg = f"Unknown age_type in data column '{col_name}'."
                            raise ValueError(error_msg)

                    col_keys.append({'age_type': age_type, 'gender_type': gender_type})

                elif data_type in ['B', 'D']:
                    # get gender_type
                    if '남자' in col_name_split[1]:
                        gender_type = 'M'
                    elif '여자' in col_name_split[1]:
                        gender_type = 'F'
                    elif '계' in col_name_split[1]:
                        gender_type = 'T'
                    else:
                        error_msg = f"Unknown gender_type in data column '{col_name}'."
                        raise ValueError(error_msg)

                    col_keys.append({'gender_type': gender_type})

                elif data_type == 'H':
                    if '세대' not in col_name_split[-1]:
                        error_msg = f"data column '{col_name}' does not indicate 'household' data."
                        raise ValueError(error_msg)

                    # get household_type
                    if '전체' in col_name_split[-1]:
                        household_type = 'size_total'
                    else:
                        household_match = household_patt.search(col_name_split[-1])
                        if not household_match:
                            error_msg = f"Unknown household_type in data column '{col_name}'."
                            raise ValueError(error_msg)
                        household_size = int(household_match.group('household_size'))
                        household_type = f"size_{household_size}"

                    col_keys.append({'household_type': household_type})

            # row by row, read and insert data
            for idx_code, row in df.iterrows():
                if idx_code is None:
                    continue
                admin_div_num = int(idx_code)

                # fill row_data
                row_data = {}
                for col_i, csv_value in enumerate(row):
                    if col_keys[col_i] is None:
                        continue

                    value = str_to_int(csv_value)
                    # insert data
                    if data_type == 'P':
                        gender_type = col_keys[col_i]['gender_type']
                        if not (gender_type in row_data.keys()):
                            row_data[gender_type] = {}

                        age_type = col_keys[col_i]['age_type']
                        if age_type not in row_data[gender_type].keys():
                            row_data[gender_type][age_type] = value
                        elif row_data[gender_type][age_type] != value:
                            error_msg = f"Data contains duplicate values for '{gender_type}', '{age_type}': '{row_data[gender_type][age_type]}' vs '{value}'."
                            raise ValueError(error_msg)

                    elif data_type in ['B', 'D']:
                        gender_type = col_keys[col_i]['gender_type']
                        if gender_type not in row_data.keys():
                            row_data[gender_type] = value
                        elif row_data[gender_type] != value:
                            error_msg = f"Data contains duplicate values for '{gender_type}': '{row_data[gender_type]}' vs '{value}'."
                            raise ValueError(error_msg)

                    elif data_type == 'H':
                        household_type = col_keys[col_i]['household_type']
                        if household_type not in row_data.keys():
                            row_data[household_type] = value
                        elif row_data[household_type] != value:
                            error_msg = f"Data contains duplicate values for '{household_type}': '{row_data[household_type]}' vs '{value}'."
                            raise ValueError(error_msg)

                    else:
                        error_msg = f"Unknown data_type '{data_type}'."
                        raise ValueError(error_msg)

                if data_type == 'P':
                    for gender_type, gender_pyramid in row_data.items():
                        new_data = {
                            'resident_type': resident_type,
                            'admin_div_num': admin_div_num,
                            'year': fname_year,
                            'month': fname_month,
                            'gender_type': gender_type,
                        }
                        new_data.update(gender_pyramid)
                        insert_success = insert_dict(pop_conn, data_type_table[data_type], new_data)
                        if insert_success:
                            print(f"  > Inserted into '{data_type_table[data_type]}': resident_type='{resident_type}', admin_div_num='{admin_div_num}', year='{fname_year}', month='{fname_month}', gender_type='{gender_type}'.")

                elif data_type in ['B', 'D']:
                    new_data = {
                        'admin_div_num': admin_div_num,
                        'year': fname_year,
                        'month': fname_month,
                        'total': row_data['T'],
                        'male': row_data['M'],
                        'female': row_data['F'],
                    }
                    insert_success = insert_dict(pop_conn, data_type_table[data_type], new_data)
                    if insert_success:
                        print(f"  > Inserted into '{data_type_table[data_type]}': admin_div_num='{admin_div_num}', year='{fname_year}', month='{fname_month}'.")

                elif data_type == 'H':
                    new_data = {
                        'resident_type': resident_type,
                        'admin_div_num': admin_div_num,
                        'year': fname_year,
                        'month': fname_month,
                    }
                    new_data.update(row_data)
                    insert_success = insert_dict(pop_conn, data_type_table[data_type], new_data)
                    if insert_success:
                        print(f"  > Inserted into '{data_type_table[data_type]}': resident_type='{resident_type}', admin_div_num='{admin_div_num}', year='{fname_year}', month='{fname_month}'.")

            pop_conn.commit()
            print(f">>> File '{file_path.name}' has been uploaded into '{data_type_table[data_type]}'.")
            print()
