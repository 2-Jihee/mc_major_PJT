import codecs
import requests
from requests.adapters import Retry, HTTPAdapter
from pathlib import Path
from time import sleep

from data.data_loc import data_dir
from db.connector import pop_db, admin_div_table, db_connect, db_execute
from db.query_str import list_to_values

source_name = 'kosis'
resp_encoding = 'ansi'
save_encoding = 'utf-8'

log_seq = '130588136'                                   # 하루에 한번은 갱신된 값을 입력할 것을 권고

url_make = 'https://kosis.kr/statHtml/downGrid.do'
url_down = 'https://kosis.kr/statHtml/downNormal.do'

url_large_make = 'https://kosis.kr/statHtml/makeLarge.do'
url_large_down = 'https://kosis.kr/statHtml/downLarge.do?file='

data_default = {
    'orgId': '101',
    'language': 'ko',
    'logSeq': log_seq,                              # 조회할 때마다 숫자가 증가함
}
data_down_large = {
    'view': 'csv',                                  # [다운로드 파일명: 확장자]             'csv' / 'excel'
    'downLargeFileType': 'csv',                     # [파일형태]                            'csv' / 'excel'
    'exprYn': 'Y',                                  # [코드포함]                            (변수삭제): 미표시 / 'Y': 표시
    'downLargeExprType': '2',                       # [통계표구성]                          '1': 시점표두, 항목표측 / '2': 항목표두, 시점표측
    'downLargeSort': 'asc',                         # [시점정렬]                            'asc': 오름차순  /  'desc': 내림차순
}
data_down_small = {
    'view': 'csv',                                  # [다운로드 파일명: 확장자]             'csv' / 'xlsx' / 'xls'
    'downGridFileType': 'csv',                      # [파일형태]                            'csv' / 'xlsx' / 'xls'
    # 'downGridCellMerge': 'Y',                       # [엑셀 셀병합]                         (변수삭제): 미표시 / 'Y': 표시(xlsx 및 xls에만 해당)
    # 'expDash': 'Y',                                 # [빈셀부호(-)]                         (변수삭제): 미표시 / 'Y': 표시
    # 'smblYn': 'Y',                                  # [통계부호]                            (변수삭제): 미표시 / 'Y': 표시
    'codeYn': 'Y',                                  # [코드포함]                            (변수삭제): 미표시 / 'Y': 표시
    'periodCo': '99',                               # [표시소수점수]                        '': 조회화면과 동일 / '99': 수록자료형식과 동일(소수점 데이터가 있으면 무한대로 표시)
    'prdSort': 'asc',                               # [시점정렬]                            'asc': 오름차순  /  'desc': 내림차순
}

headers = {
    'accept-language': 'ko,en;q=0.9,ko-KR;q=0.8',
    'host': 'kosis.kr',
    'origin': 'https://kosis.kr',
    'referer': 'https://kosis.kr/statHtml/statHtmlContent.do',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36',
}


def generate_field_list_target(target_id: str, values: list):
    json_list = []
    for value in values:
        json_item = f'{{"targetId":"{target_id}","targetValue":"{value}","prdValue":""}}'
        json_list.append(json_item)

    return json_list


def get_and_save_kosis_large_data(request_data: dict, file_path: Path):
    sess = requests.Session()
    retries = Retry(total=5, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])
    sess.mount('http://', HTTPAdapter(max_retries=retries))
    sess.mount('https://', HTTPAdapter(max_retries=retries))
    timeouts = (5, 300)

    resp = sess.post(url_large_make, request_data, headers=headers, timeout=timeouts)
    file_key = resp.json()
    filename = file_key['file']
    url = url_large_down + filename

    resp = sess.post(url, request_data, headers=headers, timeout=timeouts)
    decoded_content = resp.content.decode(resp_encoding)

    file_path.parent.mkdir(parents=True, exist_ok=True)
    with codecs.open(file_path.as_posix(), mode='x', encoding=save_encoding) as file:
        file.write(decoded_content)

    return


# # ---------- population_move_by_age (annual) ------------------------------------------------------------------------------------------------------------------------

def download_population_move_by_age(admin_div_nums=None, from_year=2001, till_year=2021, pop_conn=None):
    data_name = 'population_move_by_age'

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        dir_path = Path(data_dir, source_name, data_name, str(year))

        if admin_div_nums:
            query = f"SELECT `kosis_id` FROM `{admin_div_table}` " \
                    f"WHERE `admin_div_num` in ({list_to_values(admin_div_nums)}) " \
                    f"and `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
        else:
            query = f"SELECT `kosis_id` FROM `{admin_div_table}` " \
                    f"WHERE `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
        cur = pop_conn.cursor()
        db_execute(cur, query)
        rows = cur.fetchall()
        if not rows:
            continue
        kosis_ids = [row[0] for row in rows]

        for kosis_id in kosis_ids:
            request_data = get_request_data_for_population_move_by_age(kosis_id, year, pop_conn=pop_conn)

            filename = f"{kosis_id}_{year:0>4}_{data_name}.csv"
            file_path = dir_path / filename

            get_and_save_kosis_large_data(request_data, file_path)
            sleep(20)

    return


def get_request_data_for_population_move_by_age(kosis_id: str, year: int, pop_conn=None):
    kosis_id_to_table_id = {
        '00000': 'DT_1B26B01',  # 전국
        '11000': 'DT_1B26B02',  # 서울특별시
        '26000': 'DT_1B26B03',  # 부산광역시
        '27000': 'DT_1B26B04',  # 대구광역시
        '28000': 'DT_1B26B05',  # 인천광역시
        '29000': 'DT_1B26B06',  # 광주광역시
        '30000': 'DT_1B26B07',  # 대전광역시
        '31000': 'DT_1B26B08',  # 울산광역시
        '36000': 'DT_1B26B18',  # 세종특별자치시
        '41000': 'DT_1B26B09',  # 경기도
        '42000': 'DT_1B26B10',  # 강원도
        '43000': 'DT_1B26B11',  # 충청북도
        '44000': 'DT_1B26B12',  # 충청남도
        '45000': 'DT_1B26B13',  # 전라북도
        '46000': 'DT_1B26B14',  # 전라남도
        '47000': 'DT_1B26B15',  # 경상북도
        '48000': 'DT_1B26B16',  # 경상남도
        '50000': 'DT_1B26B17',  # 제주특별자치도
    }
    items = [
        'T10',  # 총전입
        'T20',  # 총전출
        'T25',  # 순이동
        'T30',  # 시군구내
        'T31',  # 시군구간전입
        'T32',  # 시군구간전출
        'T40',  # 시도간전입
        'T50',  # 시도간전출
    ]
    ov_lv2 = [
        '00',  # 계
        '01',  # 0세
        '02',  # 1세
        '03',  # 2세
        '04',  # 3세
        '05',  # 4세
        '51',  # 5세
        '52',  # 6세
        '53',  # 7세
        '54',  # 8세
        '55',  # 9세
        '101',  # 10세
        '102',  # 11세
        '103',  # 12세
        '104',  # 13세
        '105',  # 14세
        '151',  # 15세
        '152',  # 16세
        '153',  # 17세
        '154',  # 18세
        '155',  # 19세
        '201',  # 20세
        '202',  # 21세
        '203',  # 22세
        '204',  # 23세
        '205',  # 24세
        '251',  # 25세
        '252',  # 26세
        '253',  # 27세
        '254',  # 28세
        '255',  # 29세
        '301',  # 30세
        '302',  # 31세
        '303',  # 32세
        '304',  # 33세
        '305',  # 34세
        '351',  # 35세
        '352',  # 36세
        '353',  # 37세
        '354',  # 38세
        '355',  # 39세
        '401',  # 40세
        '402',  # 41세
        '403',  # 42세
        '404',  # 43세
        '405',  # 44세
        '451',  # 45세
        '452',  # 46세
        '453',  # 47세
        '454',  # 48세
        '455',  # 49세
        '501',  # 50세
        '502',  # 51세
        '503',  # 52세
        '504',  # 53세
        '505',  # 54세
        '551',  # 55세
        '552',  # 56세
        '553',  # 57세
        '554',  # 58세
        '555',  # 59세
        '601',  # 60세
        '602',  # 61세
        '603',  # 62세
        '604',  # 63세
        '605',  # 64세
        '651',  # 65세
        '652',  # 66세
        '653',  # 67세
        '654',  # 68세
        '655',  # 69세
        '701',  # 70세
        '702',  # 71세
        '703',  # 72세
        '704',  # 73세
        '705',  # 74세
        '751',  # 75세
        '752',  # 76세
        '753',  # 77세
        '754',  # 78세
        '755',  # 79세
        '801',  # 80세
        '802',  # 81세
        '803',  # 82세
        '804',  # 83세
        '805',  # 84세
        '852',  # 85세
        '853',  # 86세
        '854',  # 87세
        '855',  # 88세
        '900',  # 89세
        '901',  # 90세
        '902',  # 91세
        '903',  # 92세
        '904',  # 93세
        '905',  # 94세
        '951',  # 95세
        '952',  # 96세
        '953',  # 97세
        '954',  # 98세
        '955',  # 99세
        '990',  # 100세이상
    ]

    if kosis_id not in kosis_id_to_table_id.keys():
        error_msg = f"kosis_id '{kosis_id}' is invalid."
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    query = f"SELECT jr.`kosis_id` " \
            f"FROM `{admin_div_table}` sr, `{admin_div_table}` jr " \
            f"WHERE sr.`kosis_id`='{kosis_id}' and sr.`admin_div_level`<=1 " \
            f"and jr.`senior_admin_div_num`=sr.`admin_div_num` " \
            f"and (jr.`first_date` IS NULL OR YEAR(jr.`first_date`)<={year}) and (jr.`last_date` IS NULL OR YEAR(jr.`last_date`)>={year})"
    cur = pop_conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if not rows:
        return
    if kosis_id == '00000':
        # use kosis_short_id
        kosis_ids = [kosis_id[:2]]
        kosis_ids += [row[0][:2] for row in rows]
    else:
        kosis_ids = [kosis_id]
        kosis_ids += [row[0] for row in rows]

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_ids)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': kosis_id_to_table_id[kosis_id],
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A,B',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- population_move_by_stack (monthly) ------------------------------------------------------------------------------------------------------------------------

def download_population_move_by_stack(admin_div_nums=None, from_year=2001, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'population_move_by_stack'

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        if admin_div_nums:
            query = f"SELECT `kosis_id` FROM `{admin_div_table}` " \
                    f"WHERE `admin_div_num` in ({list_to_values(admin_div_nums)}) " \
                    f"and `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
        else:
            query = f"SELECT `kosis_id` FROM `{admin_div_table}` " \
                    f"WHERE `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
        cur = pop_conn.cursor()
        db_execute(cur, query)
        rows = cur.fetchall()
        if not rows:
            continue
        kosis_ids = [row[0] for row in rows]

        if year == from_year:
            start_month = from_month
        else:
            start_month = 1
        if year == till_year:
            end_month = till_month
        else:
            end_month = 12

        for month in range(start_month, end_month + 1):
            dir_path = Path(data_dir, source_name, data_name, str(year), str(month))

            for kosis_id in kosis_ids:
                request_data = get_request_data_for_population_move_by_stack(kosis_id, year, month, pop_conn=pop_conn)

                filename = f"{kosis_id}_{year:0>4}_{month:0>2}_{data_name}.csv"
                file_path = dir_path / filename

                get_and_save_kosis_large_data(request_data, file_path)
                sleep(20)

    return


def get_request_data_for_population_move_by_stack(kosis_id: str, year: int, month: int, pop_conn=None):
    items = [
        'T10',  # 총전입
        'T20',  # 총전출
        'T25',  # 순이동
        'T30',  # 시도내이동-시군구내
        'T31',  # 시도내이동-시군구간전입
        'T32',  # 시도내이동-시군구간전출
        'T40',  # 시도간전입
        'T50',  # 시도간전출
    ]
    ov_lv2 = [
        '0',  # 계
        '1',  # 남자
        '2',  # 여자
    ]
    ov_lv3 = [
        '000',  # 계
        '020',  # 0 - 4세
        '050',  # 5 - 9세
        '070',  # 10 - 14세
        '100',  # 15 - 19세
        '120',  # 20 - 24세
        '130',  # 25 - 29세
        '150',  # 30 - 34세
        '160',  # 35 - 39세
        '180',  # 40 - 44세
        '190',  # 45 - 49세
        '210',  # 50 - 54세
        '230',  # 55 - 59세
        '260',  # 60 - 64세
        '280',  # 65 - 69세
        '310',  # 70 - 74세
        '330',  # 75 - 79세
        '340',  # 80세이상
    ]

    if not kosis_id:
        error_msg = 'kosis_id is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)
    if month is None:
        error_msg = 'month is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    query = f"SELECT jr.`kosis_id` " \
            f"FROM `{admin_div_table}` sr, `{admin_div_table}` jr " \
            f"WHERE sr.`kosis_id`='{kosis_id}' and sr.`admin_div_level`<=1 " \
            f"and jr.`senior_admin_div_num`=sr.`admin_div_num` " \
            f"and (jr.`first_date` IS NULL OR YEAR(jr.`first_date`)<={year}) and (jr.`last_date` IS NULL OR YEAR(jr.`last_date`)>={year})"
    cur = pop_conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if not rows:
        return

    kosis_ids = [kosis_id[:2]]  # use kosis_short_id since its admin_level is 1 or higher
    kosis_ids += [row[0][:2] if row[0][2:] == '0' * (len(row[0]) - 2) else row[0] for row in rows]  # use kosis_short_id if its admin_level is 1 or higher

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_ids)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)
    json_ov_lv3 = generate_field_list_target('OV_L3_ID', ov_lv3)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"M,{year:0>4}{month:0>2},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2 + json_ov_lv3
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B26001',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A,SBB,YRE',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- population_move_with_destination (monthly) ------------------------------------------------------------------------------------------------------------------------

def download_population_move_with_destination_by_stack(admin_div_nums=None, from_year=2001, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'population_move_with_destination_by_stack'

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        query = f"SELECT `kosis_id` FROM `{admin_div_table}` " \
                f"WHERE `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
        cur = pop_conn.cursor()
        db_execute(cur, query)
        rows = cur.fetchall()
        if not rows:
            continue
        kosis_short_ids_to = [row[0][:2] for row in rows]

        if not admin_div_nums:
            kosis_ids = [row[0] for row in rows]
        else:
            query = f"SELECT `kosis_id` FROM `{admin_div_table}` " \
                    f"WHERE `admin_div_num` in ({list_to_values(admin_div_nums)}) " \
                    f"and `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
            cur = pop_conn.cursor()
            db_execute(cur, query)
            rows = cur.fetchall()
            if not rows:
                continue
            kosis_ids = [row[0] for row in rows]

        if year == from_year:
            start_month = from_month
        else:
            start_month = 1
        if year == till_year:
            end_month = till_month
        else:
            end_month = 12

        for month in range(start_month, end_month + 1):
            dir_path = Path(data_dir, source_name, data_name, str(year), str(month))

            for kosis_id in kosis_ids:
                request_data = get_request_data_for_population_move_with_destination_by_stack(kosis_id[:2], kosis_short_ids_to, year, month)

                filename = f"{kosis_id}_{year:0>4}_{month:0>2}_{data_name}.csv"
                file_path = dir_path / filename

                get_and_save_kosis_large_data(request_data, file_path)
                sleep(20)

    return


def get_request_data_for_population_move_with_destination_by_stack(kosis_short_id: str, kosis_short_ids_to: list, year: int, month: int):
    items = [
        'T70',  # 이동자수
        'T80',  # 순이동자수
    ]
    ov_lv3 = [
        '0',  # 계
        '1',  # 남자
        '2',  # 여자
    ]
    ov_lv4 = [
        '000',  # 계
        '020',  # 0 - 4세
        '050',  # 5 - 9세
        '070',  # 10 - 14세
        '100',  # 15 - 19세
        '120',  # 20 - 24세
        '130',  # 25 - 29세
        '150',  # 30 - 34세
        '160',  # 35 - 39세
        '180',  # 40 - 44세
        '190',  # 45 - 49세
        '210',  # 50 - 54세
        '230',  # 55 - 59세
        '260',  # 60 - 64세
        '280',  # 65 - 69세
        '310',  # 70 - 74세
        '330',  # 75 - 79세
        '340',  # 80세이상
    ]

    if not kosis_short_id:
        error_msg = 'kosis_id is missing.'
        raise ValueError(error_msg)
    if not kosis_short_ids_to:
        error_msg = 'kosis_ids_to is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)
    if month is None:
        error_msg = 'month is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', [kosis_short_id])
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', kosis_short_ids_to)
    json_ov_lv3 = generate_field_list_target('OV_L3_ID', ov_lv3)
    json_ov_lv4 = generate_field_list_target('OV_L4_ID', ov_lv4)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"M,{year:0>4}{month:0>2},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2 + json_ov_lv3 + json_ov_lv4
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B26003',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'B,C,SBB,YRE',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data
