import codecs
import re
import requests
from requests.adapters import Retry, HTTPAdapter
from pathlib import Path
from time import sleep

from data.data_loc import data_dir

resp_encoding = 'ansi'
save_encoding = 'utf-8'
source_name = 'mois'

patt_date = r'(?P<year>\d+)년\W*(?P<month>\d+)월'
patt_household = r'(?P<household_size>\d+)인세대'
patt_last_age = r'(?P<last_age>\d+)세\W*이상'
patt_age = r'(?P<age>\d+)세'

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

jr_admin_div_code_list = [
    '1111000000',
    '1114000000',
    '1117000000',
    '1120000000',
    '1121500000',
    '1123000000',
    '1126000000',
    '1129000000',
    '1130500000',
    '1132000000',
    '1135000000',
    '1138000000',
    '1141000000',
    '1144000000',
    '1147000000',
    '1150000000',
    '1153000000',
    '1154500000',
    '1156000000',
    '1159000000',
    '1162000000',
    '1165000000',
    '1168000000',
    '1171000000',
    '1174000000',
    '2611000000',
    '2614000000',
    '2617000000',
    '2620000000',
    '2623000000',
    '2626000000',
    '2629000000',
    '2632000000',
    '2635000000',
    '2638000000',
    '2641000000',
    '2644000000',
    '2647000000',
    '2650000000',
    '2653000000',
    '2671000000',
    '2711000000',
    '2714000000',
    '2717000000',
    '2720000000',
    '2723000000',
    '2726000000',
    '2729000000',
    '2771000000',
    '2811000000',
    '2814000000',
    '2817700000',
    '2818500000',
    '2820000000',
    '2823700000',
    '2824500000',
    '2826000000',
    '2871000000',
    '2872000000',
    '2911000000',
    '2914000000',
    '2915500000',
    '2917000000',
    '2920000000',
    '3011000000',
    '3014000000',
    '3017000000',
    '3020000000',
    '3023000000',
    '3111000000',
    '3114000000',
    '3117000000',
    '3120000000',
    '3171000000',
    '3611000000',
    '4111000000',
    '4113000000',
    '4115000000',
    '4117000000',
    '4119000000',
    '4121000000',
    '4122000000',
    '4125000000',
    '4127000000',
    '4128000000',
    '4129000000',
    '4131000000',
    '4136000000',
    '4137000000',
    '4139000000',
    '4141000000',
    '4143000000',
    '4145000000',
    '4146000000',
    '4148000000',
    '4150000000',
    '4155000000',
    '4157000000',
    '4159000000',
    '4161000000',
    '4163000000',
    '4165000000',
    '4167000000',
    '4180000000',
    '4182000000',
    '4183000000',
    '4211000000',
    '4213000000',
    '4215000000',
    '4217000000',
    '4219000000',
    '4221000000',
    '4223000000',
    '4272000000',
    '4273000000',
    '4275000000',
    '4276000000',
    '4277000000',
    '4278000000',
    '4279000000',
    '4280000000',
    '4281000000',
    '4282000000',
    '4283000000',
    '4311000000',
    '4313000000',
    '4315000000',
    '4372000000',
    '4373000000',
    '4374000000',
    '4374500000',
    '4375000000',
    '4376000000',
    '4377000000',
    '4380000000',
    '4413000000',
    '4415000000',
    '4418000000',
    '4420000000',
    '4421000000',
    '4423000000',
    '4425000000',
    '4427000000',
    '4471000000',
    '4476000000',
    '4477000000',
    '4479000000',
    '4480000000',
    '4481000000',
    '4482500000',
    '4511000000',
    '4513000000',
    '4514000000',
    '4518000000',
    '4519000000',
    '4521000000',
    '4571000000',
    '4572000000',
    '4573000000',
    '4574000000',
    '4575000000',
    '4577000000',
    '4579000000',
    '4580000000',
    '4611000000',
    '4613000000',
    '4615000000',
    '4617000000',
    '4623000000',
    '4671000000',
    '4672000000',
    '4673000000',
    '4677000000',
    '4678000000',
    '4679000000',
    '4680000000',
    '4681000000',
    '4682000000',
    '4683000000',
    '4684000000',
    '4686000000',
    '4687000000',
    '4688000000',
    '4689000000',
    '4690000000',
    '4691000000',
    '4711000000',
    '4713000000',
    '4715000000',
    '4717000000',
    '4719000000',
    '4721000000',
    '4723000000',
    '4725000000',
    '4728000000',
    '4729000000',
    '4772000000',
    '4773000000',
    '4775000000',
    '4776000000',
    '4777000000',
    '4782000000',
    '4783000000',
    '4784000000',
    '4785000000',
    '4790000000',
    '4792000000',
    '4793000000',
    '4794000000',
    '4812000000',
    '4817000000',
    '4822000000',
    '4824000000',
    '4825000000',
    '4827000000',
    '4831000000',
    '4833000000',
    '4872000000',
    '4873000000',
    '4874000000',
    '4882000000',
    '4884000000',
    '4885000000',
    '4886000000',
    '4887000000',
    '4888000000',
    '4889000000',
    '5011000000',
    '5013000000',
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

data_type_dict = {
    'B': 'birth',               # birth: 주민등록기준 지역별 출생등록
    'D': 'death',               # death: 주민등록기준 지역별 사망말소
    'H': 'household',           # households: 지역별 세대원수별 세대수
    'P': 'population',          # 연령별 주민등록 인구형황
}
resident_type_dict = {
    '-': 'all',                 # 전체
    'R': 'resident',            # resident
    'U': 'unknown',             # unknown domicile
    'O': 'overseas',            # overseas
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

resident_type_to_sltUndefType = {
    '-': '',                    # '': 전체
    'R': 'Y',                   # 'Y': 거주자
    'U': 'N',                   # 'N': 거주불명자
    'O': 'O',                   # 'O': 재외국민
}
resident_type_to_col_key = {
    'R': '거주자',              # resident
    'U': '거주불명자',          # unknown domicile
    'O': '재외국민',            # overseas
}


# # ---------- generic ------------------------------------------------------------------------------------------------------------------------

def get_data_name(data_type: str, resident_type=None):
    if data_type not in data_type_dict.keys():
        raise ValueError(f">>> Input data_type '{data_type}' is invalid.")
    data_name = data_type_dict[data_type]
    if resident_type is not None:
        if resident_type not in resident_type_dict.keys():
            raise ValueError(f">>> Input resident_type '{resident_type}' is invalid.")
        data_name += f"_{resident_type_dict[resident_type]}"
        if data_name not in data_name_list:
            raise ValueError(f">>> Invalid input combination: data_type '{data_type}' and resident_type '{resident_type}' are not compatible.")

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
            sltOrgType = '1'
            sltOrgLvl1 = 'A'
            sltOrgLvl2 = 'A'
        else:
            sltOrgType = '2'
            if admin_div_code[2:] == '0'*8:
                sltOrgLvl1 = admin_div_code
                sltOrgLvl2 = 'A'
            else:
                if data_type != 'P' and is_detail_data is False:
                    print(f">>> admin_div_code '{admin_div_code}' is not available with data_type '{data_type}' when is_detail_data is False.")
                    continue
                sltOrgLvl1 = jr_admin_div_code_dict[admin_div_code]
                sltOrgLvl2 = admin_div_code
        if is_detail_data:
            xlsStats = '3'          # xlsStats -  3: 전체읍면동현황 / 2: 전체시군구현황
        else:
            xlsStats = '1'          # xlsStats -  1: 현재화면

        if data_type == 'P':
            url = f'https://jumin.mois.go.kr/downloadCsvAge.do?searchYearMonth=month&xlsStats={xlsStats}'
            referer = 'https://jumin.mois.go.kr/ageStatMonth.do'
            category = 'month'
        else:
            url = f'https://jumin.mois.go.kr/downloadCsvEtc.do?searchYearMonth=month&xlsStats={xlsStats}'
            if data_type == 'B':
                referer = 'https://jumin.mois.go.kr/etcStatBirth.do'
                category = data_type_dict[data_type]
            elif data_type == 'D':
                referer = 'https://jumin.mois.go.kr/etcStatDeath.do'
                category = data_type_dict[data_type]
            elif data_type == 'H':
                referer = 'https://jumin.mois.go.kr/etcStatHouseholds.do'
                category = 'households'
            else:
                raise ValueError(f">>> Input data_type '{data_type}' is invalid.")
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
                    'sltOrgType': sltOrgType,                   # 1: 전국  /  2: 시도
                    'sltOrgLvl1': sltOrgLvl1,                   # A: 모든 시도  /  시도 코드('1100000000'-'5000000000')
                    'sltOrgLvl2': sltOrgLvl2,                   # A: 모든 시군구)  /  시군구 코
                    'category': category,                       # 데이터 유형 - year, month, birth, death, households
                    'searchYearStart': str(year),
                    'searchMonthStart': f'{month:02}',
                    'searchYearEnd': str(year),
                    'searchMonthEnd': f'{month:02}',
                    'sltOrderType': '1',                        # 데이터 정렬 기준
                    'sltOrderValue': 'ASC',                     # 데이터 정렬 방식 - ASC
                }
                if data_type in ['P', 'H']:
                    data.update({'sltUndefType': resident_type_to_sltUndefType[resident_type]})
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
                print(f">>> Saved '{fname}' under '{dir_path}'.")
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

def mois_idx_to_admin_div_num(idx: str):
    match_div_num = re.search(r'\d{10}', idx)
    if match_div_num:
        admin_div_code = match_div_num.group()
        if admin_div_code == '1000000000':
            admin_div_code = '0000000000'
        admin_div_num = int(admin_div_code)
    else:
        admin_div_num = None

    return admin_div_num


def mois_col_key_to_col_data_type(col_key: str):
    household_type = None
    gender_type = None
    age_type = None

    col_data_type = None

    data_keys = {'household_type': household_type,
                 'gender_type': gender_type,
                 'age_type': age_type}

    return col_data_type, data_keys


def upload_mois_data(admin_div_codes: list, data_type: str, resident_type=None, is_detail_data=False):
    data_name = get_data_name(data_type, resident_type=resident_type)
    for admin_div_code in admin_div_codes:
        dir_path = get_dir_path(admin_div_code, data_name, is_detail_data)
        if not dir_path.exists():
            print(f">>> ")
            continue





    parent_path = Path(data_dir, source_name, data_name)
    d_types = data_name.split('_')

    dir_paths = [p for p in parent_path.iterdir() if p.is_dir()]
    dir_paths = natsorted(dir_paths)
    for dir_path in dir_paths:
        file_paths = [f for f in dir_path.iterdir() if f.suffix == '.csv']
        for file_path in file_paths:
            df = pd.read_csv(file_path.as_posix(), index_col=0)
            new_index = []
            for idx in df.index:
                admin_div_num = mois_idx_to_admin_div_num(idx)
                if admin_div_num is None:
                    raise ValueError(f"Unknown admin_div type '{idx}'.")
                new_index.append(admin_div_num)
            df.index = new_index

            # check resident_type of data
            resident_type = '-'
            for key, value in resident_type_to_col_key.items():
                if re.search(value, df.columns[0]):
                    resident_type = key
                    break
            if d_types[0] in ['population', 'household']:
                resident_type_str = resident_type_dict[resident_type]
                if not d_types[1] == resident_type_str:
                    raise ValueError(f"Provided data has resident type '{resident_type_str}' (data_type='{data_name}').")

            for row_i, row in df.iterrows():
                if d_types[0] in ['birth', 'death']:
                    pop_data = {'T': None, 'M': None, 'F': None}
                elif d_types[0] == 'population':
                    age_data = {}
                elif d_types[0] == 'household':
                    household_data = {}
                else:
                    raise ValueError(f"Unknown data_type '{data_name}'.")

                for col_i, (col_key, csv_value) in enumerate(row.iteritems()):
                    # skip redundant data
                    if '연령구간' in col_key:
                        continue
                    # determine date: year/month
                    col_parts = col_key.split('_')
                    match_obj = re.search(patt_date, col_parts[0])
                    year = match_obj.group('year')
                    month = match_obj.group('month')
                    # determine data-type
                    col_data_type = None
                    if col_parts[-1][-2:] == '세대':
                        col_data_type = 'household'
                        # determine household_type
                        if re.search('전체', col_parts[-1]):
                            household_type = '--'
                        else:
                            match_obj = re.search(patt_household, col_parts[-1])
                            if not match_obj:
                                raise ValueError(f"Unknown household_type in column index '{col_key}'.")
                            household_size = match_obj.group('household_size')
                            household_type = household_size.zfill(2)
                    else:
                        # determine gender_type
                        offset = 0
                        if col_parts[1] == '남' or '남자' in col_parts[1]:
                            gender_type = 'M'
                        elif col_parts[1] == '여' or '여자' in col_parts[1]:
                            gender_type = 'F'
                        elif col_parts[1] == '계':
                            gender_type = 'T'
                        elif col_parts[1] in resident_type_to_col_key.values():
                            gender_type = 'T'
                            offset = 1
                        else:
                            raise ValueError(f"Unknown gender_type in column index '{col_key}'.")
                        if len(col_parts) <= (2 - offset):
                            col_data_type = 'birth/death'
                        if len(col_parts) > (2 - offset):
                            col_data_type = 'population'
                            # determine age type
                            if col_parts[-1] == '총인구수':
                                age_type = '-----'
                            else:
                                match_last_age = re.search(patt_last_age, col_parts[-1])
                                match_age = re.search(patt_age, col_parts[-1])
                                if match_last_age:
                                    age_type = '>=' + match_last_age.group('last_age').zfill(3)
                                elif match_age:
                                    age_type = '==' + match_age.group('age').zfill(3)
                                else:
                                    raise ValueError(f"Unknown age_type in column index '{col_key}'.")
                    # check if identified data_type matches
                    type_correct = False
                    if d_types[0] in ['birth', 'death']:
                        if col_data_type == 'birth/death':
                            type_correct = True
                    else:
                        if d_types[0] == col_data_type:
                            type_correct = True
                    if not type_correct:
                        raise ValueError(f"Provided data_type '{data_name}' and file do not match.")

                    int_value = str_to_int(csv_value)
                    if col_data_type == 'population':
                        if not (age_type in age_data.keys()):
                            age_data[age_type] = {}
                        if gender_type in age_data[age_type].keys():
                            if age_data[age_type][gender_type] != int_value:
                                raise ValueError(f"Data contains duplicate data for '{data_name}': '{age_data[age_type][gender_type]}' vs '{value}'.")
                        else:
                            age_data[age_type][gender_type] = int_value



                print(age_data)
                print()
