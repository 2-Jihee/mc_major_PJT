import codecs
import re
import pandas as pd
import requests
from requests.adapters import Retry, HTTPAdapter
from pathlib import Path
from natsort import natsorted
from time import sleep

from data.data_loc import data_dir
from db.connector import pop_db, admin_div_table, db_connect, db_execute
from db.query import select_one_row_pack_into_dict, update_dict, insert_dict
from db.query_str import list_to_values, dict_to_set
from pop.model import str_to_int

source_name = 'kosis'
resp_encoding = 'ansi'
save_encoding = 'utf-8'

log_seq = '130739896'  # 신규 다운로드 시 갱신된 값을 입력할 것
# 130628071
# 130628129
# 130637926
# 130640473
# 130642570
# 130642872
# 130646172
# 130646772
# 130648398
# 130652195
# 130652665
# 130652646
# 130655512
# 130656745
# 130657497
# 130658757
# 130658925
# 130664732
# 130665840
# 130667465
# 130672606
# 130685249
# 130710482
# 130712013

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

csv_sex_to_sex_type = {
    '0': '-',  # 계
    '1': 'M',  # 남자
    '2': 'F',  # 여자
}
csv_sex_to_sex_col = {
    '0': 'total',  # 계
    '1': 'male',  # 남자
    '2': 'female',  # 여자
}
csv_pop_move_age_to_age_type = {
    '00': '-----',  # 계
    '01': '000==',  # 0세
    '02': '001==',  # 1세
    '03': '002==',  # 2세
    '04': '003==',  # 3세
    '05': '004==',  # 4세
    '51': '005==',  # 5세
    '52': '006==',  # 6세
    '53': '007==',  # 7세
    '54': '008==',  # 8세
    '55': '009==',  # 9세
    '101': '010==',  # 10세
    '102': '011==',  # 11세
    '103': '012==',  # 12세
    '104': '013==',  # 13세
    '105': '014==',  # 14세
    '151': '015==',  # 15세
    '152': '016==',  # 16세
    '153': '017==',  # 17세
    '154': '018==',  # 18세
    '155': '019==',  # 19세
    '201': '020==',  # 20세
    '202': '021==',  # 21세
    '203': '022==',  # 22세
    '204': '023==',  # 23세
    '205': '024==',  # 24세
    '251': '025==',  # 25세
    '252': '026==',  # 26세
    '253': '027==',  # 27세
    '254': '028==',  # 28세
    '255': '029==',  # 29세
    '301': '030==',  # 30세
    '302': '031==',  # 31세
    '303': '032==',  # 32세
    '304': '033==',  # 33세
    '305': '034==',  # 34세
    '351': '035==',  # 35세
    '352': '036==',  # 36세
    '353': '037==',  # 37세
    '354': '038==',  # 38세
    '355': '039==',  # 39세
    '401': '040==',  # 40세
    '402': '041==',  # 41세
    '403': '042==',  # 42세
    '404': '043==',  # 43세
    '405': '044==',  # 44세
    '451': '045==',  # 45세
    '452': '046==',  # 46세
    '453': '047==',  # 47세
    '454': '048==',  # 48세
    '455': '049==',  # 49세
    '501': '050==',  # 50세
    '502': '051==',  # 51세
    '503': '052==',  # 52세
    '504': '053==',  # 53세
    '505': '054==',  # 54세
    '551': '055==',  # 55세
    '552': '056==',  # 56세
    '553': '057==',  # 57세
    '554': '058==',  # 58세
    '555': '059==',  # 59세
    '601': '060==',  # 60세
    '602': '061==',  # 61세
    '603': '062==',  # 62세
    '604': '063==',  # 63세
    '605': '064==',  # 64세
    '651': '065==',  # 65세
    '652': '066==',  # 66세
    '653': '067==',  # 67세
    '654': '068==',  # 68세
    '655': '069==',  # 69세
    '701': '070==',  # 70세
    '702': '071==',  # 71세
    '703': '072==',  # 72세
    '704': '073==',  # 73세
    '705': '074==',  # 74세
    '751': '075==',  # 75세
    '752': '076==',  # 76세
    '753': '077==',  # 77세
    '754': '078==',  # 78세
    '755': '079==',  # 79세
    '801': '080==',  # 80세
    '802': '081==',  # 81세
    '803': '082==',  # 82세
    '804': '083==',  # 83세
    '805': '084==',  # 84세
    '852': '085==',  # 85세
    '853': '086==',  # 86세
    '854': '087==',  # 87세
    '855': '088==',  # 88세
    '900': '089==',  # 89세
    '901': '090==',  # 90세
    '902': '091==',  # 91세
    '903': '092==',  # 92세
    '904': '093==',  # 93세
    '905': '094==',  # 94세
    '951': '095==',  # 95세
    '952': '096==',  # 96세
    '953': '097==',  # 97세
    '954': '098==',  # 98세
    '955': '099==',  # 99세
    '990': '100=+',  # 100세이상
}
csv_pop_move_stack_to_age_type = {
    '000': '-----',  # 계
    '020': '00004',  # 0 - 4세
    '050': '00509',  # 5 - 9세
    '070': '01014',  # 10 - 14세
    '100': '01519',  # 15 - 19세
    '120': '02024',  # 20 - 24세
    '130': '02529',  # 25 - 29세
    '150': '03034',  # 30 - 34세
    '160': '03539',  # 35 - 39세
    '180': '04044',  # 40 - 44세
    '190': '04549',  # 45 - 49세
    '210': '05054',  # 50 - 54세
    '230': '05559',  # 55 - 59세
    '260': '06064',  # 60 - 64세
    '280': '06569',  # 65 - 69세
    '310': '07074',  # 70 - 74세
    '330': '07579',  # 75 - 79세
    '340': '080=+',  # 80세이상
}
csv_birth_order_to_birth_order_type = {
    '00': '--',  # 총계
    '01': '1=',  # 1아
    '02': '2=',  # 2아
    '03': '3=',  # 3아
    '04': '4=',  # 4아
    '05': '5=',  # 5아
    '06': '6=',  # 6아
    '07': '7=',  # 7아
    '08': '8+',  # 8아이상
    '13': '3+',  # 3아 이상
    '99': 'XX',  # 미상
}
csv_mother_age_to_mother_age_type = {
    '00': '-----',  # 계
    '16': '00014',  # 15세 미만
    '20': '01519',  # 15 - 19세
    '201': '015==',  # 15세
    '202': '016==',  # 16세
    '203': '017==',  # 17세
    '204': '018==',  # 18세
    '205': '019==',  # 19세
    '25': '02024',  # 20 - 24세
    '251': '020==',  # 20세
    '252': '021==',  # 21세
    '253': '022==',  # 22세
    '254': '023==',  # 23세
    '255': '024==',  # 24세
    '30': '02529',  # 25 - 29세
    '301': '025==',  # 25세
    '302': '026==',  # 26세
    '303': '027==',  # 27세
    '304': '028==',  # 28세
    '305': '029==',  # 29세
    '35': '03034',  # 30 - 34세
    '351': '030==',  # 30세
    '352': '031==',  # 31세
    '353': '032==',  # 32세
    '354': '033==',  # 33세
    '355': '034==',  # 34세
    '40': '03539',  # 35 - 39세
    '401': '035==',  # 35세
    '402': '036==',  # 36세
    '403': '037==',  # 37세
    '404': '038==',  # 38세
    '405': '039==',  # 39세
    '45': '04044',  # 40 - 44세
    '451': '040==',  # 40세
    '452': '041==',  # 41세
    '453': '042==',  # 42세
    '454': '043==',  # 43세
    '455': '044==',  # 44세
    '50': '04549',  # 45 - 49세
    '501': '045==',  # 45세
    '502': '046==',  # 46세
    '503': '047==',  # 47세
    '504': '048==',  # 48세
    '505': '049==',  # 49세
    '56': '050=+',  # 50세 이상
    '95': 'XXXXX',  # 연령미상
}
csv_death_stack_to_age_type = {
    '000': '-----',  # 계
    '520': '000==',  # 0세
    '040': '00104',  # 1 - 4세
    '050': '00509',  # 5 - 9세
    '070': '01014',  # 10 - 14세
    '100': '01519',  # 15 - 19세
    '120': '02024',  # 20 - 24세
    '130': '02529',  # 25 - 29세
    '150': '03034',  # 30 - 34세
    '160': '03539',  # 35 - 39세
    '180': '04044',  # 40 - 44세
    '190': '04549',  # 45 - 49세
    '210': '05054',  # 50 - 54세
    '230': '05559',  # 55 - 59세
    '260': '06064',  # 60 - 64세
    '280': '06569',  # 65 - 69세
    '310': '07074',  # 70 - 74세
    '330': '07579',  # 75 - 79세
    '340': '080=+',  # 80세 이상
    '360': '08084',  # 80 - 84세
    '380': '08589',  # 85 - 89세
    '390': '090=+',  # 90세 이상
    '990': 'XXXXX',  # 연령미상
}
csv_death_age_to_age_type = {
    '1000': '-----',  # 계
    '100100': '000==',  # 0세
    '100101': '001==',  # 1세
    '100102': '002==',  # 2세
    '100103': '003==',  # 3세
    '100104': '004==',  # 4세
    '100500': '005==',  # 5세
    '100501': '006==',  # 6세
    '100502': '007==',  # 7세
    '100503': '008==',  # 8세
    '100504': '009==',  # 9세
    '101000': '010==',  # 10세
    '101001': '011==',  # 11세
    '101002': '012==',  # 12세
    '101003': '013==',  # 13세
    '101004': '014==',  # 14세
    '101500': '015==',  # 15세
    '101501': '016==',  # 16세
    '101502': '017==',  # 17세
    '101503': '018==',  # 18세
    '101504': '019==',  # 19세
    '102000': '020==',  # 20세
    '102001': '021==',  # 21세
    '102002': '022==',  # 22세
    '102003': '023==',  # 23세
    '102004': '024==',  # 24세
    '102500': '025==',  # 25세
    '102501': '026==',  # 26세
    '102502': '027==',  # 27세
    '102503': '028==',  # 28세
    '102504': '029==',  # 29세
    '103000': '030==',  # 30세
    '103001': '031==',  # 31세
    '103002': '032==',  # 32세
    '103003': '033==',  # 33세
    '103004': '034==',  # 34세
    '103500': '035==',  # 35세
    '103501': '036==',  # 36세
    '103502': '037==',  # 37세
    '103503': '038==',  # 38세
    '103504': '039==',  # 39세
    '104000': '040==',  # 40세
    '104001': '041==',  # 41세
    '104002': '042==',  # 42세
    '104003': '043==',  # 43세
    '104004': '044==',  # 44세
    '104500': '045==',  # 45세
    '104501': '046==',  # 46세
    '104502': '047==',  # 47세
    '104503': '048==',  # 48세
    '104504': '049==',  # 49세
    '105000': '050==',  # 50세
    '105001': '051==',  # 51세
    '105002': '052==',  # 52세
    '105003': '053==',  # 53세
    '105004': '054==',  # 54세
    '105500': '055==',  # 55세
    '105501': '056==',  # 56세
    '105502': '057==',  # 57세
    '105503': '058==',  # 58세
    '105504': '059==',  # 59세
    '106000': '060==',  # 60세
    '106001': '061==',  # 61세
    '106002': '062==',  # 62세
    '106003': '063==',  # 63세
    '106004': '064==',  # 64세
    '106500': '065==',  # 65세
    '106501': '066==',  # 66세
    '106502': '067==',  # 67세
    '106503': '068==',  # 68세
    '106504': '069==',  # 69세
    '107000': '070==',  # 70세
    '107001': '071==',  # 71세
    '107002': '072==',  # 72세
    '107003': '073==',  # 73세
    '107004': '074==',  # 74세
    '107500': '075==',  # 75세
    '107501': '076==',  # 76세
    '107502': '077==',  # 77세
    '107503': '078==',  # 78세
    '107504': '079==',  # 79세
    '108000': '080==',  # 80세
    '108001': '081==',  # 81세
    '108002': '082==',  # 82세
    '108003': '083==',  # 83세
    '108004': '084==',  # 84세
    '108500': '085==',  # 85세
    '108501': '086==',  # 86세
    '108502': '087==',  # 87세
    '108503': '088==',  # 88세
    '108504': '089==',  # 89세
    '109000': '090==',  # 90세
    '109001': '091==',  # 91세
    '109002': '092==',  # 92세
    '109003': '093==',  # 93세
    '109004': '094==',  # 94세
    '109500': '095==',  # 95세
    '109501': '096==',  # 96세
    '109502': '097==',  # 97세
    '109503': '098==',  # 98세
    '109504': '099==',  # 99세
    '1100': '100=+',  # 100세이상
    '1999': 'XXXXX',  # 연령미상
}
csv_marriage_age_to_age_type = {
    '000': '-----',  # 계
    '160': '00014',  # 15세미만
    '200': '01519',  # 15 - 19세
    '201': '015==',  # 15세
    '202': '016==',  # 16세
    '203': '017==',  # 17세
    '204': '018==',  # 18세
    '205': '019==',  # 19세
    '250': '02024',  # 20 - 24세
    '251': '020==',  # 20세
    '252': '021==',  # 21세
    '253': '022==',  # 22세
    '254': '023==',  # 23세
    '255': '024==',  # 24세
    '300': '02529',  # 25 - 29세
    '301': '025==',  # 25세
    '302': '026==',  # 26세
    '303': '027==',  # 27세
    '304': '028==',  # 28세
    '305': '029==',  # 29세
    '350': '03034',  # 30 - 34세
    '351': '030==',  # 30세
    '352': '031==',  # 31세
    '353': '032==',  # 32세
    '354': '033==',  # 33세
    '355': '034==',  # 34세
    '401': '035==',  # 35세
    '402': '036==',  # 36세
    '403': '037==',  # 37세
    '404': '038==',  # 38세
    '405': '039==',  # 39세
    '450': '04044',  # 40 - 44세
    '451': '040==',  # 40세
    '452': '041==',  # 41세
    '453': '042==',  # 42세
    '454': '043==',  # 43세
    '455': '044==',  # 44세
    '500': '04549',  # 45 - 49세
    '501': '045==',  # 45세
    '502': '046==',  # 46세
    '503': '047==',  # 47세
    '504': '048==',  # 48세
    '505': '049==',  # 49세
    '550': '05054',  # 50 - 54세
    '551': '050==',  # 50세
    '552': '051==',  # 51세
    '553': '052==',  # 52세
    '554': '053==',  # 53세
    '555': '054==',  # 54세
    '600': '05559',  # 55 - 59세
    '601': '055==',  # 55세
    '602': '056==',  # 56세
    '603': '057==',  # 57세
    '604': '058==',  # 58세
    '605': '059==',  # 59세
    '650': '06064',  # 60 - 64세
    '651': '060==',  # 60세
    '652': '061==',  # 61세
    '653': '062==',  # 62세
    '654': '063==',  # 63세
    '655': '064==',  # 64세
    '700': '06569',  # 65 - 69세
    '701': '065==',  # 65세
    '702': '066==',  # 66세
    '703': '067==',  # 67세
    '704': '068==',  # 68세
    '705': '069==',  # 69세
    '750': '07074',  # 70 - 74세
    '751': '070==',  # 70세
    '752': '071==',  # 71세
    '753': '072==',  # 72세
    '754': '073==',  # 73세
    '755': '074==',  # 74세
    '770': '075=+',  # 75세이상
    '800': '075=+',  # 75세이상
    '801': '075==',  # 75세
    '806': '076=+',  # 76세이상
    '950': 'XXXXX',  # 미상
}
csv_marriage_stack_to_age_type = {
    '00': '-----',  # 계
    '16': '00014',  # 15세 미만
    '20': '01519',  # 15 - 19세
    '25': '02024',  # 20 - 24세
    '30': '02529',  # 25 - 29세
    '35': '03034',  # 30 - 34세
    '40': '03539',  # 35 - 39세
    '45': '04044',  # 40 - 44세
    '50': '04549',  # 45 - 49세
    '55': '05054',  # 50 - 54세
    '60': '05559',  # 55 - 59세
    '65': '06064',  # 60 - 64세
    '70': '06569',  # 65 - 69세
    '75': '07074',  # 70 - 74세
    '80': '075=+',  # 75세 이상
    '95': 'XXXXX',  # 미상
}
csv_pop_age_to_age_type = {
    '000': '015=+',  # 합계
    '020001': '015==',  # 15세
    '020002': '016==',  # 16세
    '020003': '017==',  # 17세
    '020004': '018==',  # 18세
    '020005': '019==',  # 19세
    '025001': '020==',  # 20세
    '025002': '021==',  # 21세
    '025003': '022==',  # 22세
    '025004': '023==',  # 23세
    '025005': '024==',  # 24세
    '030001': '025==',  # 25세
    '030002': '026==',  # 26세
    '030003': '027==',  # 27세
    '030004': '028==',  # 28세
    '030005': '029==',  # 29세
    '035001': '030==',  # 30세
    '035002': '031==',  # 31세
    '035003': '032==',  # 32세
    '035004': '033==',  # 33세
    '035005': '034==',  # 34세
    '040001': '035==',  # 35세
    '040002': '036==',  # 36세
    '040003': '037==',  # 37세
    '040004': '038==',  # 38세
    '040005': '039==',  # 39세
    '045001': '040==',  # 40세
    '045002': '041==',  # 41세
    '045003': '042==',  # 42세
    '045004': '043==',  # 43세
    '045005': '044==',  # 44세
    '050001': '045==',  # 45세
    '050002': '046==',  # 46세
    '050003': '047==',  # 47세
    '050004': '048==',  # 48세
    '050005': '049==',  # 49세
    '055001': '050==',  # 50세
    '055002': '051==',  # 51세
    '055003': '052==',  # 52세
    '055004': '053==',  # 53세
    '055005': '054==',  # 54세
    '060001': '055==',  # 55세
    '060002': '056==',  # 56세
    '060003': '057==',  # 57세
    '060004': '058==',  # 58세
    '060005': '059==',  # 59세
    '065001': '060==',  # 60세
    '065002': '061==',  # 61세
    '065003': '062==',  # 62세
    '065004': '063==',  # 63세
    '065005': '064==',  # 64세
    '070001': '065==',  # 65세
    '070002': '066==',  # 66세
    '070003': '067==',  # 67세
    '070004': '068==',  # 68세
    '070005': '069==',  # 69세
    '075001': '070==',  # 70세
    '075002': '071==',  # 71세
    '075003': '072==',  # 72세
    '075004': '073==',  # 73세
    '075005': '074==',  # 74세
    '080001': '075==',  # 75세
    '080002': '076==',  # 76세
    '080003': '077==',  # 77세
    '080004': '078==',  # 78세
    '080005': '079==',  # 79세
    '085001': '080==',  # 80세
    '085002': '081==',  # 81세
    '085003': '082==',  # 82세
    '085004': '083==',  # 83세
    '085005': '084==',  # 84세
    '086': '085=+',  # 85세이상
}


def get_admin_div_n2_codes_level_0_and_1(admin_div_nums: list, year: int, pop_conn=None, exclude_overseas=True):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    where_excl_overseas = 'and `admin_div_num`<>9000000000 ' if exclude_overseas else ''

    if admin_div_nums:
        query = f"SELECT SUBSTRING(CONVERT(`admin_div_num`, CHAR), 1, 2) " \
                f"FROM `{admin_div_table}` " \
                f"WHERE `admin_div_num` in ({list_to_values(admin_div_nums)}) " \
                f"and `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
    else:
        query = f"SELECT SUBSTRING(CONVERT(`admin_div_num`, CHAR), 1, 2) " \
                f"FROM `{admin_div_table}` " \
                f"WHERE `admin_div_level`<=1 {where_excl_overseas}" \
                f"and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
    cur = pop_conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if rows:
        admin_div_n2_codes = [row[0] for row in rows]
    else:
        admin_div_n2_codes = None

    return admin_div_n2_codes


def get_admin_div_n5_codes_level_0_and_1(admin_div_nums: list, year: int, pop_conn=None, exclude_overseas=True):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    where_excl_overseas = 'and `admin_div_num`<>9000000000 ' if exclude_overseas else ''

    if admin_div_nums:
        query = f"SELECT SUBSTRING(CONVERT(`admin_div_num`, CHAR), 1, 5) " \
                f"FROM `{admin_div_table}` " \
                f"WHERE `admin_div_num` in ({list_to_values(admin_div_nums)}) " \
                f"and `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
    else:
        query = f"SELECT SUBSTRING(CONVERT(`admin_div_num`, CHAR), 1, 5) " \
                f"FROM `{admin_div_table}` " \
                f"WHERE `admin_div_level`<=1 {where_excl_overseas}" \
                f"and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
    cur = pop_conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if rows:
        admin_div_n5_codes = [row[0] for row in rows]
    else:
        admin_div_n5_codes = None

    return admin_div_n5_codes


def get_kosis_admin_div_code_to_admin_div_num(year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    query = f"SELECT `admin_div_num`, `kosis_admin_div_code`, `kosis_admin_div_code_2` " \
            f"FROM `{admin_div_table}` " \
            f"WHERE (`kosis_admin_div_code` IS NOT NULL OR `kosis_admin_div_code_2` IS NOT NULL) " \
            f"and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
    cur = pop_conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if rows:
        kosis_admin_div_code_to_admin_div_num = {row[1]: row[0] for row in rows if row[1] is not None}
        kosis_admin_div_code_to_admin_div_num.update({row[2]: row[0] for row in rows if row[2] is not None})
    else:
        kosis_admin_div_code_to_admin_div_num = None

    return kosis_admin_div_code_to_admin_div_num


def get_kosis_admin_div_codes_level_0_and_1(admin_div_nums: list, year: int, pop_conn=None, exclude_overseas=True):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    where_excl_overseas = 'and `admin_div_num`<>9000000000 ' if exclude_overseas else ''

    if admin_div_nums:
        query = f"SELECT `kosis_admin_div_code` " \
                f"FROM `{admin_div_table}` " \
                f"WHERE `admin_div_num` in ({list_to_values(admin_div_nums)}) " \
                f"and `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year}) " \
                f"ORDER BY `admin_div_num`"
    else:
        query = f"SELECT `kosis_admin_div_code` " \
                f"FROM `{admin_div_table}` " \
                f"WHERE `admin_div_level`<=1 {where_excl_overseas}" \
                f"and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year}) " \
                f"ORDER BY `admin_div_num`"
    cur = pop_conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if rows:
        kosis_admin_div_codes = [row[0] for row in rows]
    else:
        kosis_admin_div_codes = None

    return kosis_admin_div_codes


def get_admin_div_nums_and_kosis_codes_level_0_and_1(admin_div_nums: list, year: int, pop_conn=None, exclude_overseas=True):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    where_excl_overseas = 'and `admin_div_num`<>9000000000 ' if exclude_overseas else ''

    if admin_div_nums:
        query = f"SELECT `admin_div_num`, `kosis_admin_div_code` " \
                f"FROM `{admin_div_table}` " \
                f"WHERE `admin_div_num` in ({list_to_values(admin_div_nums)}) " \
                f"and `admin_div_level`<=1 and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year}) " \
                f"ORDER BY `admin_div_num`"
    else:
        query = f"SELECT `admin_div_num`, `kosis_admin_div_code` " \
                f"FROM `{admin_div_table}` " \
                f"WHERE `admin_div_level`<=1 {where_excl_overseas}" \
                f"and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year}) " \
                f"ORDER BY `admin_div_num`"
    cur = pop_conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if rows:
        admin_div_nums_and_codes = rows
    else:
        admin_div_nums_and_codes = None

    return admin_div_nums_and_codes


def convert_admin_div_num_to_admin_div_n5_code(admin_div_num: int):
    if admin_div_num % 100000 == 0:
        admin_div_n5_code = str(int(admin_div_num / 100000)).rjust(5, '0')
    else:
        admin_div_n5_code = None

    return admin_div_n5_code


def convert_admin_div_code_to_admin_div_num(admin_div_code: str):
    admin_div_n10_code = admin_div_code.ljust(10, '0')
    admin_div_num = int(admin_div_n10_code)

    return admin_div_num


def convert_admin_div_n5_code_to_admin_div_code(admin_div_n5_code: str):
    admin_div_code = admin_div_n5_code[:2] if admin_div_n5_code[2:] == '0' * len(admin_div_n5_code[2:]) else admin_div_n5_code

    return admin_div_code


def get_jr_admin_div_n5_codes(admin_div_n5_code: str, year: int, pop_conn=None, exclude_overseas=True):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    where_excl_overseas = 'and jr.`admin_div_num`<>9000000000 ' if exclude_overseas else ''

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_n5_code)
    query = f"SELECT SUBSTRING(CONVERT(jr.`admin_div_num`, CHAR), 1, 5) " \
            f"FROM `{admin_div_table}` sr, `{admin_div_table}` jr " \
            f"WHERE sr.`admin_div_num`={admin_div_num} and sr.`admin_div_level`<=1 " \
            f"and jr.`senior_admin_div_num`=sr.`admin_div_num` {where_excl_overseas}" \
            f"and (jr.`first_date` IS NULL OR YEAR(jr.`first_date`)<={year}) and (jr.`last_date` IS NULL OR YEAR(jr.`last_date`)>={year})"
    cur = pop_conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if rows:
        jr_admin_div_n5_codes = [row[0] for row in rows]
    else:
        jr_admin_div_n5_codes = None

    return jr_admin_div_n5_codes


def get_jr_kosis_admin_div_codes(kosis_admin_div_code: str, year: int, pop_conn=None, exclude_overseas=True):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    where_excl_overseas = 'and jr.`admin_div_num`<>9000000000 ' if exclude_overseas else ''

    if kosis_admin_div_code[0] != '0':
        # senior가 전국이 아닌 경우, 2자리에 3글자가 더해진 경우를 조회
        query = f"SELECT `kosis_admin_div_code`, `kosis_admin_div_code_2` " \
                f"FROM `{admin_div_table}` " \
                f"WHERE `kosis_admin_div_code`<>'{kosis_admin_div_code}' " \
                f"and (`kosis_admin_div_code` LIKE '{kosis_admin_div_code}%' OR `kosis_admin_div_code_2` LIKE '{kosis_admin_div_code}%') " \
                f"and (`first_date` IS NULL OR YEAR(`first_date`)<={year}) and (`last_date` IS NULL OR YEAR(`last_date`)>={year})"
    else:
        # senior가 전국인 경우, child를 조회
        query = f"SELECT jr.`kosis_admin_div_code`, jr.`kosis_admin_div_code_2` " \
                f"FROM `{admin_div_table}` sr, `{admin_div_table}` jr " \
                f"WHERE sr.`kosis_admin_div_code`={kosis_admin_div_code} and sr.`admin_div_level`<=1 " \
                f"and jr.`senior_admin_div_num`=sr.`admin_div_num` {where_excl_overseas}" \
                f"and (jr.`first_date` IS NULL OR YEAR(jr.`first_date`)<={year}) and (jr.`last_date` IS NULL OR YEAR(jr.`last_date`)>={year})"
    cur = pop_conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if rows:
        len_sr_code = len(kosis_admin_div_code)
        jr_kosis_admin_div_codes = []
        jr_kosis_admin_div_codes += [row[0] for row in rows if row[0] is not None]
        jr_kosis_admin_div_codes += [row[1] for row in rows if row[1] is not None]
        jr_kosis_admin_div_codes = list(dict.fromkeys(jr_kosis_admin_div_codes))
        if kosis_admin_div_code[0] != '0':
            jr_kosis_admin_div_codes = [code for code in jr_kosis_admin_div_codes if code[:len_sr_code] == kosis_admin_div_code]
        jr_kosis_admin_div_codes.sort()
    else:
        jr_kosis_admin_div_codes = None

    return jr_kosis_admin_div_codes


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

def upload_population_move_by_age(from_year=2001, till_year=2021, pop_conn=None):
    data_name = 'population_move_by_age'
    db_table = 'kosis_population_move'
    csv_col_to_db_col = {
        '총전입': 'total_move_in',
        '총전출': 'total_move_out',
        '순이동': 'net_move',
        '시군구내': 'intra_level_1_intra_level_2_flow',
        '시군구간전입': 'intra_level_1_inter_level_2_inflow',
        '시군구간전출': 'intra_level_1_inter_level_2_outflow',
        '시도간전입': 'inter_level_1_inflow',
        '시도간전출': 'inter_level_1_outflow',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        file_paths = [f_path for f_path in year_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
        for file_path in file_paths:
            # read file
            df = pd.read_csv(file_path.as_posix(), dtype='string')
            for idx, row in df.iterrows():
                year = int(integer_patt.search(row['시점']).group())

                admin_div_num = convert_admin_div_code_to_admin_div_num(row['[A]행정구역별'])
                age_type = csv_pop_move_age_to_age_type[row['[B]각세별']]

                unique_keys = {
                    'admin_div_num': admin_div_num,
                    'year': year,
                    'month': 0,
                    'sex_type': '-',
                    'age_type': age_type,
                }

                row_data = {}
                for key, value in csv_col_to_db_col.items():
                    row_data.update({value: str_to_int(row[key])})
                if all(value is None for value in row_data.values()):
                    continue

                curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                if curr_row:
                    for value_key in row_data.keys():
                        if curr_row[value_key] != row_data[value_key]:
                            if curr_row[value_key] is None:
                                update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                if update_success:
                                    print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                            elif row_data[value_key] is not None:
                                error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                raise ValueError(error_msg)
                else:
                    new_row = unique_keys.copy()
                    new_row.update(row_data)
                    insert_success = insert_dict(pop_conn, db_table, new_row)
                    if insert_success:
                        print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                    else:
                        error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                        raise ValueError(error_msg)

            pop_conn.commit()
            print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
            print()

    return


def download_population_move_by_age(admin_div_nums=None, from_year=2001, till_year=2021, pop_conn=None):
    data_name = 'population_move_by_age'
    # 연령(각세별) 이동자수: 시도/각세별 이동자수, 서울특별시 시군구 각세별 이동자수 등

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        dir_path = Path(data_dir, source_name, data_name, str(year))

        admin_div_n5_codes = get_admin_div_n5_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_n5_codes:
            continue

        for admin_div_n5_code in admin_div_n5_codes:
            request_data = get_request_data_for_population_move_by_age(admin_div_n5_code, year, pop_conn=pop_conn)

            filename = f"{admin_div_n5_code}_{year:0>4}_{data_name}.csv"
            file_path = dir_path / filename

            get_and_save_kosis_large_data(request_data, file_path)
            sleep(20)

    return


def get_request_data_for_population_move_by_age(admin_div_n5_code: str, year: int, pop_conn=None):
    admin_div_n5_code_to_table_id = {
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

    if admin_div_n5_code not in admin_div_n5_code_to_table_id.keys():
        error_msg = f"admin_div_n5_code '{admin_div_n5_code}' is invalid."
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_admin_div_n5_codes = get_jr_admin_div_n5_codes(admin_div_n5_code, year, pop_conn=pop_conn)
    if not jr_admin_div_n5_codes:
        return
    if admin_div_n5_code == '00000':
        # use admin_div_n2_codes
        admin_div_n2_codes = [admin_div_n5_code[:2]]
        admin_div_n2_codes += [jr_admin_div_n5_code[:2] for jr_admin_div_n5_code in jr_admin_div_n5_codes]
        json_ov_lv1 = generate_field_list_target('OV_L1_ID', admin_div_n2_codes)
    else:
        admin_div_n5_codes = [admin_div_n5_code]
        admin_div_n5_codes += jr_admin_div_n5_codes
        json_ov_lv1 = generate_field_list_target('OV_L1_ID', admin_div_n5_codes)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': admin_div_n5_code_to_table_id[admin_div_n5_code],
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A,B',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- population_move_by_stack (monthly) ------------------------------------------------------------------------------------------------------------------------

def upload_population_move_by_stack(from_year=1970, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'population_move_by_stack'
    db_table = 'kosis_population_move'
    csv_col_to_db_col = {
        '총전입[명]': 'total_move_in',
        '총전출[명]': 'total_move_out',
        '순이동[명]': 'net_move',
        '시도내이동-시군구내[명]': 'intra_level_1_intra_level_2_flow',
        '시도내이동-시군구간 전입[명]': 'intra_level_1_inter_level_2_inflow',
        '시도내이동-시군구간 전출[명]': 'intra_level_1_inter_level_2_outflow',
        '시도간전입[명]': 'inter_level_1_inflow',
        '시도간전출[명]': 'inter_level_1_outflow',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    year_month_patt = re.compile(r'(?P<year>\d+)\.(?P<month>\d+)')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        if year_folder == from_year:
            start_month = from_month
        else:
            start_month = 1
        if year_folder == till_year:
            end_month = till_month
        else:
            end_month = 12

        month_path_names = [m_path.name for m_path in year_path.iterdir() if m_path.is_dir() and start_month <= int(m_path.name) <= end_month]
        month_path_names = natsorted(month_path_names)
        month_paths = [year_path / month_path_name for month_path_name in month_path_names]
        for month_path in month_paths:
            file_paths = [f_path for f_path in month_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
            for file_path in file_paths:
                # read file
                df = pd.read_csv(file_path.as_posix(), dtype='string')
                for idx, row in df.iterrows():
                    year_month_match = year_month_patt.search(row['시점'])
                    year = int(year_month_match.group('year'))
                    month = int(year_month_match.group('month'))

                    admin_div_num = convert_admin_div_code_to_admin_div_num(row['[A]행정구역(시군구)별'])
                    sex_type = csv_sex_to_sex_type[row['[SBB]성별']]
                    age_type = csv_pop_move_stack_to_age_type[row['[YRE]연령별']]

                    unique_keys = {
                        'admin_div_num': admin_div_num,
                        'year': year,
                        'month': month,
                        'sex_type': sex_type,
                        'age_type': age_type,
                    }

                    row_data = {}
                    for key, value in csv_col_to_db_col.items():
                        row_data.update({value: str_to_int(row[key])})
                    if all(value is None for value in row_data.values()):
                        continue

                    curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                    if curr_row:
                        for value_key in row_data.keys():
                            if curr_row[value_key] != row_data[value_key]:
                                if curr_row[value_key] is None:
                                    update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                    if update_success:
                                        print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                                elif row_data[value_key] is not None:
                                    error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                    raise ValueError(error_msg)
                    else:
                        new_row = unique_keys.copy()
                        new_row.update(row_data)
                        insert_success = insert_dict(pop_conn, db_table, new_row)
                        if insert_success:
                            print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                        else:
                            error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                            raise ValueError(error_msg)

                pop_conn.commit()
                print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
                print()

    return


def download_population_move_by_stack(admin_div_nums=None, from_year=1970, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'population_move_by_stack'
    # 시군구/성/연령(5세)별 이동률

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        admin_div_n5_codes = get_admin_div_n5_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_n5_codes:
            continue

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

            for admin_div_n5_code in admin_div_n5_codes:
                request_data = get_request_data_for_population_move_by_stack(admin_div_n5_code, year, month, pop_conn=pop_conn)

                filename = f"{admin_div_n5_code}_{year:0>4}_{month:0>2}_{data_name}.csv"
                file_path = dir_path / filename

                get_and_save_kosis_large_data(request_data, file_path)
                sleep(20)

    return


def get_request_data_for_population_move_by_stack(admin_div_n5_code: str, year: int, month: int, pop_conn=None):
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

    if not admin_div_n5_code:
        error_msg = 'admin_div_n5_code is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)
    if month is None:
        error_msg = 'month is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_admin_div_n5_codes = get_jr_admin_div_n5_codes(admin_div_n5_code, year, pop_conn=pop_conn)
    if not jr_admin_div_n5_codes:
        return

    admin_div_codes = [convert_admin_div_n5_code_to_admin_div_code(admin_div_n5_code)]
    admin_div_codes += [convert_admin_div_n5_code_to_admin_div_code(jr_admin_div_n5_code) for jr_admin_div_n5_code in jr_admin_div_n5_codes]

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', admin_div_codes)
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

def upload_population_move_with_destination_by_stack(from_year=1970, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'population_move_with_destination_by_stack'
    db_table = 'kosis_population_move_matrix'
    csv_col_to_db_col = {
        '이동자수[명]': 'total_move',
        '순이동자수[명]': 'net_move',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    year_month_patt = re.compile(r'(?P<year>\d+)\.(?P<month>\d+)')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        if year_folder == from_year:
            start_month = from_month
        else:
            start_month = 1
        if year_folder == till_year:
            end_month = till_month
        else:
            end_month = 12

        month_path_names = [m_path.name for m_path in year_path.iterdir() if m_path.is_dir() and start_month <= int(m_path.name) <= end_month]
        month_path_names = natsorted(month_path_names)
        month_paths = [year_path / month_path_name for month_path_name in month_path_names]
        for month_path in month_paths:
            file_paths = [f_path for f_path in month_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
            for file_path in file_paths:
                # read file
                df = pd.read_csv(file_path.as_posix(), dtype='string')
                for idx, row in df.iterrows():
                    year_month_match = year_month_patt.search(row['시점'])
                    year = int(year_month_match.group('year'))
                    month = int(year_month_match.group('month'))

                    from_admin_div_num = convert_admin_div_code_to_admin_div_num(row['[B]전출지별'])
                    into_admin_div_num = convert_admin_div_code_to_admin_div_num(row['[C]전입지별'])
                    sex_type = csv_sex_to_sex_type[row['[SBB]성별']]
                    age_type = csv_pop_move_stack_to_age_type[row['[YRE]연령별']]

                    unique_keys = {
                        'from_admin_div_num': from_admin_div_num,
                        'to_admin_div_num': into_admin_div_num,
                        'year': year,
                        'month': month,
                        'sex_type': sex_type,
                        'age_type': age_type,
                    }

                    row_data = {}
                    for key, value in csv_col_to_db_col.items():
                        row_data.update({value: str_to_int(row[key])})
                    if all(value is None for value in row_data.values()):
                        continue

                    curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                    if curr_row:
                        for value_key in row_data.keys():
                            if curr_row[value_key] != row_data[value_key]:
                                if curr_row[value_key] is None:
                                    update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                    if update_success:
                                        print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                                elif row_data[value_key] is not None:
                                    error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                    raise ValueError(error_msg)
                    else:
                        new_row = unique_keys.copy()
                        new_row.update(row_data)
                        insert_success = insert_dict(pop_conn, db_table, new_row)
                        if insert_success:
                            print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                        else:
                            error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                            raise ValueError(error_msg)

                pop_conn.commit()
                print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
                print()

    return


def download_population_move_with_destination_by_stack(admin_div_nums=None, from_year=1970, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'population_move_with_destination_by_stack'
    # 전출지/전입지(시도)/성/연령(5세)별 이동자수

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        admin_div_n5_codes_0 = get_admin_div_n5_codes_level_0_and_1([], year, pop_conn=pop_conn)
        admin_div_n2_codes_destination = [admin_div_n5_code[:2] for admin_div_n5_code in admin_div_n5_codes_0]

        if not admin_div_nums:
            admin_div_n5_codes = admin_div_n5_codes_0
        else:
            admin_div_n5_codes = get_admin_div_n5_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_n5_codes:
            continue

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

            for admin_div_n5_code in admin_div_n5_codes:
                admin_div_n2_code = admin_div_n5_code[:2]
                request_data = get_request_data_for_population_move_with_destination_by_stack(admin_div_n2_code, admin_div_n2_codes_destination, year, month)

                filename = f"{admin_div_n5_code}_{year:0>4}_{month:0>2}_{data_name}.csv"
                file_path = dir_path / filename

                get_and_save_kosis_large_data(request_data, file_path)
                sleep(20)

    return


def get_request_data_for_population_move_with_destination_by_stack(admin_div_n2_code: str, admin_div_n2_codes_destination: list, year: int, month: int):
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

    if not admin_div_n2_code:
        error_msg = 'admin_div_n2_code is missing.'
        raise ValueError(error_msg)
    if not admin_div_n2_codes_destination:
        error_msg = 'admin_div_n2_codes_destination is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)
    if month is None:
        error_msg = 'month is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', [admin_div_n2_code])
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', admin_div_n2_codes_destination)
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


# # ---------- birth (monthly) ------------------------------------------------------------------------------------------------------------------------

def upload_birth(from_year=1997, from_month=1, till_year=2020, till_month=12, pop_conn=None):
    data_name = 'birth'
    db_table = 'kosis_birth'
    csv_col_to_db_col = {
        '계[명]': 'total',
        '남자[명]': 'male',
        '여자[명]': 'female',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    year_month_patt = re.compile(r'(?P<year>\d+)\.(?P<month>\d+)')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_folder, pop_conn=pop_conn)

        if year_folder == from_year:
            start_month = from_month
        else:
            start_month = 1
        if year_folder == till_year:
            end_month = till_month
        else:
            end_month = 12

        month_path_names = [m_path.name for m_path in year_path.iterdir() if m_path.is_dir() and start_month <= int(m_path.name) <= end_month]
        month_path_names = natsorted(month_path_names)
        month_paths = [year_path / month_path_name for month_path_name in month_path_names]
        for month_path in month_paths:
            file_paths = [f_path for f_path in month_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
            for file_path in file_paths:
                # read file
                df = pd.read_csv(file_path.as_posix(), dtype='string')
                for idx, row in df.iterrows():
                    year_month_match = year_month_patt.search(row['시점'])
                    year = int(year_month_match.group('year'))
                    month = int(year_month_match.group('month'))

                    kosis_admin_div_code = row['[A]시군구별']
                    if year < 2014:
                        if kosis_admin_div_code in ['33040', '33041', '33042', '33043', '33044']:
                            continue
                    else:
                        if kosis_admin_div_code in ['33010', '33011', '33012']:
                            continue
                    admin_div_num = kosis_admin_div_code_to_admin_div_num[kosis_admin_div_code]

                    unique_keys = {
                        'admin_div_num': admin_div_num,
                        'year': year,
                        'month': month,
                        'mother_age_type': '----',
                        'birth_order_type': '--',
                    }

                    row_data = {}
                    for key, value in csv_col_to_db_col.items():
                        row_data.update({value: str_to_int(row[key])})
                    if all(value is None for value in row_data.values()):
                        continue

                    curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                    if curr_row:
                        for value_key in row_data.keys():
                            if curr_row[value_key] != row_data[value_key]:
                                if curr_row[value_key] is None:
                                    update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                    if update_success:
                                        print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                                elif row_data[value_key] is not None:
                                    error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                    raise ValueError(error_msg)
                    else:
                        new_row = unique_keys.copy()
                        new_row.update(row_data)
                        insert_success = insert_dict(pop_conn, db_table, new_row)
                        if insert_success:
                            print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                        else:
                            error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                            raise ValueError(error_msg)

                pop_conn.commit()
                print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
                print()

    return


def download_birth(admin_div_nums=None, from_year=1997, from_month=1, till_year=2020, till_month=12, pop_conn=None):
    data_name = 'birth'
    # 시군구/성/월별 출생

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        admin_div_nums_and_codes = get_admin_div_nums_and_kosis_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_nums_and_codes:
            continue

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

            for admin_div_num_and_code in admin_div_nums_and_codes:
                request_data = get_request_data_for_birth(admin_div_num_and_code[1], year, month, pop_conn=pop_conn)

                filename = f"{convert_admin_div_num_to_admin_div_n5_code(admin_div_num_and_code[0])}_{year:0>4}_{month:0>2}_{data_name}.csv"
                file_path = dir_path / filename

                get_and_save_kosis_large_data(request_data, file_path)
                sleep(15)

    return


def get_request_data_for_birth(kosis_admin_div_code: str, year: int, month: int, pop_conn=None):
    items = [
        'T1',  # 계
        'T2',  # 남자
        'T3',  # 여자
    ]

    if not kosis_admin_div_code:
        error_msg = 'kosis_admin_div_code is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)
    if month is None:
        error_msg = 'month is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_kosis_admin_div_codes = get_jr_kosis_admin_div_codes(kosis_admin_div_code, year, pop_conn=pop_conn)
    if not jr_kosis_admin_div_codes:
        return

    kosis_admin_div_codes = [kosis_admin_div_code]
    kosis_admin_div_codes += jr_kosis_admin_div_codes

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"M,{year:0>4}{month:0>2},@"}}'] + json_items + json_ov_lv1
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B81A01',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- birth_by_order (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_birth_by_order(from_year=2000, till_year=2020, pop_conn=None):
    data_name = 'birth_by_order'
    db_table = 'kosis_birth'
    csv_col_to_db_col = {
        '계[명]': 'total',
        '남자[명]': 'male',
        '여자[명]': 'female',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_folder, pop_conn=pop_conn)

        file_paths = [f_path for f_path in year_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
        for file_path in file_paths:
            # read file
            df = pd.read_csv(file_path.as_posix(), dtype='string')
            for idx, row in df.iterrows():
                year = int(integer_patt.search(row['시점']).group())

                kosis_admin_div_code = row['[A]시군구별']
                if year < 2014:
                    if kosis_admin_div_code in ['33040', '33041', '33042', '33043', '33044']:
                        continue
                else:
                    if kosis_admin_div_code in ['33010', '33011', '33012']:
                        continue
                admin_div_num = kosis_admin_div_code_to_admin_div_num[kosis_admin_div_code]
                birth_order_type = csv_birth_order_to_birth_order_type[row['[J]출산순위별']]

                unique_keys = {
                    'admin_div_num': admin_div_num,
                    'year': year,
                    'month': 0,
                    'mother_age_type': '----',
                    'birth_order_type': birth_order_type,
                }

                row_data = {}
                for key, value in csv_col_to_db_col.items():
                    row_data.update({value: str_to_int(row[key])})
                if all(value is None for value in row_data.values()):
                    continue

                curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                if curr_row:
                    for value_key in row_data.keys():
                        if curr_row[value_key] != row_data[value_key]:
                            if curr_row[value_key] is None:
                                update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                if update_success:
                                    print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                            elif row_data[value_key] is not None:
                                error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                raise ValueError(error_msg)
                else:
                    new_row = unique_keys.copy()
                    new_row.update(row_data)
                    insert_success = insert_dict(pop_conn, db_table, new_row)
                    if insert_success:
                        print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                    else:
                        error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                        raise ValueError(error_msg)

            pop_conn.commit()
            print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
            print()

    return


def download_birth_by_order(admin_div_nums=None, from_year=2000, till_year=2020, pop_conn=None):
    data_name = 'birth_by_order'
    # 시군구/성/출산순위별 출생

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        dir_path = Path(data_dir, source_name, data_name, str(year))

        admin_div_nums_and_codes = get_admin_div_nums_and_kosis_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_nums_and_codes:
            continue

        for admin_div_num_and_code in admin_div_nums_and_codes:
            request_data = get_request_data_for_birth_by_order(admin_div_num_and_code[1], year, pop_conn=pop_conn)

            filename = f"{convert_admin_div_num_to_admin_div_n5_code(admin_div_num_and_code[0])}_{year:0>4}_{data_name}.csv"
            file_path = dir_path / filename

            get_and_save_kosis_large_data(request_data, file_path)
            sleep(20)

    return


def get_request_data_for_birth_by_order(kosis_admin_div_code: str, year: int, pop_conn=None):
    items = [
        'T1',  # 계
        'T2',  # 남자
        'T3',  # 여자
    ]
    ov_lv2 = [
        '00',  # 총계
        '01',  # 1아
        '02',  # 2아
        '13',  # 3아 이상
        '99',  # 미상
    ]

    if not kosis_admin_div_code:
        error_msg = 'kosis_admin_div_code is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_kosis_admin_div_codes = get_jr_kosis_admin_div_codes(kosis_admin_div_code, year, pop_conn=pop_conn)
    if not jr_kosis_admin_div_codes:
        return

    kosis_admin_div_codes = [kosis_admin_div_code]
    kosis_admin_div_codes += jr_kosis_admin_div_codes

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B81A03',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A,J',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- birth_by_stack (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_birth_by_stack(from_year=2000, till_year=2020, pop_conn=None):
    data_name = 'birth_by_stack'
    db_table = 'kosis_birth'
    value_col_to_mother_age_type = {
        '출생아수(명)': '----',
        '모의 연령별 출생아수(명):15-19세': '1519',
        '20-24세(명)': '2024',
        '25-29세(명)': '2529',
        '30-34세(명)': '3034',
        '35-39세(명)': '3539',
        '40-44세(명)': '4044',
        '45-49세(명)': '4549',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_folder, pop_conn=pop_conn)

        file_paths = [f_path for f_path in year_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
        for file_path in file_paths:
            # read file
            df = pd.read_csv(file_path.as_posix(), dtype='string')
            for idx, row in df.iterrows():
                year = int(integer_patt.search(row['시점']).group())

                kosis_admin_div_code = row['[A]시군구별']
                if year < 2014:
                    if kosis_admin_div_code in ['33040', '33041', '33042', '33043', '33044']:
                        continue
                else:
                    if kosis_admin_div_code in ['33010', '33011', '33012']:
                        continue
                admin_div_num = kosis_admin_div_code_to_admin_div_num[kosis_admin_div_code]

                for key, value in value_col_to_mother_age_type.items():
                    unique_keys = {
                        'admin_div_num': admin_div_num,
                        'year': year,
                        'month': 0,
                        'mother_age_type': value,
                        'birth_order_type': '--',
                    }

                    row_data = {'total': str_to_int(row[key])}
                    if all(value is None for value in row_data.values()):
                        continue

                    curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                    if curr_row:
                        for value_key in row_data.keys():
                            if curr_row[value_key] != row_data[value_key]:
                                if curr_row[value_key] is None:
                                    update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                    if update_success:
                                        print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                                elif row_data[value_key] is not None:
                                    error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                    raise ValueError(error_msg)
                    else:
                        new_row = unique_keys.copy()
                        new_row.update(row_data)
                        insert_success = insert_dict(pop_conn, db_table, new_row)
                        if insert_success:
                            print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                        else:
                            error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                            raise ValueError(error_msg)

            pop_conn.commit()
            print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
            print()

    return


def download_birth_by_stack(admin_div_nums=None, from_year=2000, till_year=2020, pop_conn=None):
    data_name = 'birth_by_stack'
    # 시군구/ 모의 평균 출산연령, 모의 연령별(5세간격) 출생

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        dir_path = Path(data_dir, source_name, data_name, str(year))

        admin_div_nums_and_codes = get_admin_div_nums_and_kosis_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_nums_and_codes:
            continue

        for admin_div_num_and_code in admin_div_nums_and_codes:
            request_data = get_request_data_for_birth_by_stack(admin_div_num_and_code[1], year, pop_conn=pop_conn)

            filename = f"{convert_admin_div_num_to_admin_div_n5_code(admin_div_num_and_code[0])}_{year:0>4}_{data_name}.csv"
            file_path = dir_path / filename

            get_and_save_kosis_large_data(request_data, file_path)
            sleep(20)

    return


def get_request_data_for_birth_by_stack(kosis_admin_div_code: str, year: int, pop_conn=None):
    items = [
        'T0',  # 모의 평균 출산 연령(세)
        'T1',  # 출생아수(명)
        'T2',  # 모의 연령별 출생아수(명):15-19세
        'T3',  # 20-24세(명)
        'T4',  # 25-29세(명)
        'T5',  # 30-34세(명)
        'T6',  # 35-39세(명)
        'T7',  # 40-44세(명)
        'T8',  # 45-49세(명)
    ]

    if not kosis_admin_div_code:
        error_msg = 'kosis_admin_div_code is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_kosis_admin_div_codes = get_jr_kosis_admin_div_codes(kosis_admin_div_code, year, pop_conn=pop_conn)
    if not jr_kosis_admin_div_codes:
        return

    kosis_admin_div_codes = [kosis_admin_div_code]
    kosis_admin_div_codes += jr_kosis_admin_div_codes

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B81A28',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- birth_by_stack_and_order (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_birth_by_stack_and_order(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'birth_by_stack_and_order'
    db_table = 'kosis_birth'
    csv_col_to_db_col = {
        '계[명]': 'total',
        '남자[명]': 'male',
        '여자[명]': 'female',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        year_fname = int(file_path.stem[:4])
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_fname, pop_conn=pop_conn)

        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())

            admin_div_num = kosis_admin_div_code_to_admin_div_num[row['[A]시도별']]
            mother_age_type = csv_mother_age_to_mother_age_type[row['[F]모의 연령(5세계급)별']]
            birth_order_type = csv_birth_order_to_birth_order_type[row['[J]출산순위별']]

            unique_keys = {
                'admin_div_num': admin_div_num,
                'year': year,
                'month': 0,
                'mother_age_type': mother_age_type,
                'birth_order_type': birth_order_type,
            }

            row_data = {}
            for key, value in csv_col_to_db_col.items():
                row_data.update({value: str_to_int(row[key])})
            if all(value is None for value in row_data.values()):
                continue

            curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
            if curr_row:
                for value_key in row_data.keys():
                    if curr_row[value_key] != row_data[value_key]:
                        if curr_row[value_key] is None:
                            update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                            if update_success:
                                print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                        elif row_data[value_key] is not None:
                            error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                            raise ValueError(error_msg)
            else:
                new_row = unique_keys.copy()
                new_row.update(row_data)
                insert_success = insert_dict(pop_conn, db_table, new_row)
                if insert_success:
                    print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                else:
                    error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                    raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_birth_by_stack_and_order(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'birth_by_stack_and_order'
    # 시도/성/모의 연령(5세계급)/출산순위별 출생

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        kosis_admin_div_codes = get_kosis_admin_div_codes_level_0_and_1([], year, pop_conn=pop_conn)
        request_data = get_request_data_for_birth_by_stack_and_order(kosis_admin_div_codes, year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(15)

    return


def get_request_data_for_birth_by_stack_and_order(kosis_admin_div_codes: list, year: int):
    items = [
        'T1',  # 계
        'T2',  # 남자
        'T3',  # 여자
    ]
    ov_lv2 = [
        '00',  # 계
        '16',  # 15세 미만
        '20',  # 15 - 19세
        '25',  # 20 - 24세
        '30',  # 25 - 29세
        '35',  # 30 - 34세
        '40',  # 35 - 39세
        '45',  # 40 - 44세
        '50',  # 45 - 49세
        '56',  # 50세 이상
        '95',  # 연령미상
    ]
    ov_lv3 = [
        '00',  # 총계
        '01',  # 1아
        '02',  # 2아
        '03',  # 3아
        '04',  # 4아
        '05',  # 5아
        '06',  # 6아
        '07',  # 7아
        '08',  # 8아이상
        '99',  # 미상
    ]

    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)
    json_ov_lv3 = generate_field_list_target('OV_L3_ID', ov_lv3)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2 + json_ov_lv3
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B81A12',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A,F,J',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- birth_by_age_and_order (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_birth_by_age_and_order(from_year=1981, till_year=2020, pop_conn=None):
    data_name = 'birth_by_age_and_order'
    db_table = 'kosis_birth'

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())
            mother_age_type = csv_mother_age_to_mother_age_type[row['[F]모의 연령(5세계급)별']]
            birth_order_type = csv_birth_order_to_birth_order_type[row['[J]출산순위별']]

            unique_keys = {
                'admin_div_num': 0,
                'year': year,
                'month': 0,
                'mother_age_type': mother_age_type,
                'birth_order_type': birth_order_type,
            }

            row_data = {}
            csv_col_to_db_col = {
                '출생[명]': csv_sex_to_sex_col[row['[SBB]성별']],
            }
            for key, value in csv_col_to_db_col.items():
                row_data.update({value: str_to_int(row[key])})
            if all(value is None for value in row_data.values()):
                continue

            curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
            if curr_row:
                for value_key in row_data.keys():
                    if curr_row[value_key] != row_data[value_key]:
                        if curr_row[value_key] is None:
                            update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                            if update_success:
                                print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                        elif row_data[value_key] is not None:
                            error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                            raise ValueError(error_msg)
            else:
                new_row = unique_keys.copy()
                new_row.update(row_data)
                insert_success = insert_dict(pop_conn, db_table, new_row)
                if insert_success:
                    print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                else:
                    error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                    raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_birth_by_age_and_order(from_year=1981, till_year=2020):
    data_name = 'birth_by_age_and_order'
    # 성/모의 연령(각세)/출산순위별 출생

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        request_data = get_request_data_for_birth_by_age_and_order(year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(15)

    return


def get_request_data_for_birth_by_age_and_order(year: int):
    items = [
        'T1',  # 출생
    ]
    ov_lv1 = [
        '0',  # 계
        '1',  # 남자
        '2',  # 여자
    ]
    ov_lv2 = [
        '00',   # 계
        '16',   # 15세미만
        '201',  # 15세
        '202',  # 16세
        '203',  # 17세
        '204',  # 18세
        '205',  # 19세
        '251',  # 20세
        '252',  # 21세
        '253',  # 22세
        '254',  # 23세
        '255',  # 24세
        '301',  # 25세
        '302',  # 26세
        '303',  # 27세
        '304',  # 28세
        '305',  # 29세
        '351',  # 30세
        '352',  # 31세
        '353',  # 32세
        '354',  # 33세
        '355',  # 34세
        '401',  # 35세
        '402',  # 36세
        '403',  # 37세
        '404',  # 38세
        '405',  # 39세
        '451',  # 40세
        '452',  # 41세
        '453',  # 42세
        '454',  # 43세
        '455',  # 44세
        '501',  # 45세
        '502',  # 46세
        '503',  # 47세
        '504',  # 48세
        '505',  # 49세
        '56',   # 50세이상
        '95',   # 연령미상
    ]
    ov_lv3 = [
        '00',  # 총계
        '01',  # 1아
        '02',  # 2아
        '03',  # 3아
        '04',  # 4아
        '05',  # 5아
        '06',  # 6아
        '07',  # 7아
        '08',  # 8아 이상
        '99',  # 미상
    ]

    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', ov_lv1)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)
    json_ov_lv3 = generate_field_list_target('OV_L3_ID', ov_lv3)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2 + json_ov_lv3
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B80A01',
        'fieldList': field_list,
        'colAxis': 'TIME',
        'rowAxis': 'SBB,F,J',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- birth by cohabitation period (annual) ------------------------------------------------------------------------------------------------------------------------

def download_birth_by_cohabitation_period(from_year=1993, till_year=2020, pop_conn=None):
    data_name = 'birth_by_cohabitation_period'
    # 시도/모의 연령/동거기간별 출생

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        kosis_admin_div_codes = get_kosis_admin_div_codes_level_0_and_1([], year, pop_conn=pop_conn)
        request_data = get_request_data_for_birth_by_cohabitation_period(kosis_admin_div_codes, year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(15)

    return


def get_request_data_for_birth_by_cohabitation_period(kosis_admin_div_codes: list, year: int):
    items = [
        'T1',  # 출생
    ]
    ov_lv2 = [
        '00',  # 계
        '16',  # 15세 미만
        '20',  # 15 - 19세
        '25',  # 20 - 24세
        '30',  # 25 - 29세
        '35',  # 30 - 34세
        '40',  # 35 - 39세
        '45',  # 40 - 44세
        '50',  # 45 - 49세
        '56',  # 50세 이상
        '95',  # 연령미상
    ]
    ov_lv3 = [
        '00',  # 계
        '01',  # 1년미만
        '03',  # 1년
        '06',  # 2년
        '09',  # 3년
        '12',  # 4년
        '15',  # 5년
        '18',  # 6년
        '21',  # 7년
        '24',  # 8년
        '27',  # 9년
        '30',  # 10~14년
        '40',  # 15~19년
        '50',  # 20년이상
        '95',  # 미상
    ]

    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)
    json_ov_lv3 = generate_field_list_target('OV_L3_ID', ov_lv3)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2 + json_ov_lv3
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B81A08',
        'fieldList': field_list,
        'colAxis': 'TIME',
        'rowAxis': 'A,G,K',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- birth by marital status (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_birth_by_marital_status(from_year=1981, till_year=2020, pop_conn=None):
    data_name = 'birth_by_marital_status'
    db_table = 'kosis_birth'
    csv_col_to_db_col = {
        '총계[명]': 'total',
        '혼인중의 자[명]': 'total_married_parents',
        '혼인외의 자[명]': 'total_unmarried_parents',
        '미상[명]': 'total_unknown_parents',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        year_fname = int(file_path.stem[:4])
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_fname, pop_conn=pop_conn)

        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())

            admin_div_num = kosis_admin_div_code_to_admin_div_num[row['[A]시도별']]

            unique_keys = {
                'admin_div_num': admin_div_num,
                'year': year,
                'month': 0,
                'mother_age_type': '----',
                'birth_order_type': '--',
            }

            row_data = {}
            for key, value in csv_col_to_db_col.items():
                row_data.update({value: str_to_int(row[key])})
            if all(value is None for value in row_data.values()):
                continue

            curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
            if curr_row:
                for value_key in row_data.keys():
                    if curr_row[value_key] != row_data[value_key]:
                        if curr_row[value_key] is None:
                            update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                            if update_success:
                                print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                        elif row_data[value_key] is not None:
                            error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                            raise ValueError(error_msg)
            else:
                new_row = unique_keys.copy()
                new_row.update(row_data)
                insert_success = insert_dict(pop_conn, db_table, new_row)
                if insert_success:
                    print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                else:
                    error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                    raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_birth_by_marital_status(from_year=1981, till_year=2020, pop_conn=None):
    data_name = 'birth_by_marital_status'
    # 시도/법적혼인상태별 출생

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        kosis_admin_div_codes = get_kosis_admin_div_codes_level_0_and_1([], year, pop_conn=pop_conn)
        request_data = get_request_data_for_birth_by_marital_status(kosis_admin_div_codes, year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(15)

    return


def get_request_data_for_birth_by_marital_status(kosis_admin_div_codes: list, year: int):
    items = [
        'T1',  # 총계
        'T2',  # 혼인중의
        'T3',  # 혼인외의
        'T4',  # 미상

    ]
    ov_lv2 = [
        '00',  # 계
        '16',  # 15세 미만
        '20',  # 15 - 19세
        '25',  # 20 - 24세
        '30',  # 25 - 29세
        '35',  # 30 - 34세
        '40',  # 35 - 39세
        '45',  # 40 - 44세
        '50',  # 45 - 49세
        '56',  # 50세 이상
        '95',  # 연령미상
    ]
    ov_lv3 = [
        '00',  # 계
        '01',  # 1년미만
        '03',  # 1년
        '06',  # 2년
        '09',  # 3년
        '12',  # 4년
        '15',  # 5년
        '18',  # 6년
        '21',  # 7년
        '24',  # 8년
        '27',  # 9년
        '30',  # 10~14년
        '40',  # 15~19년
        '50',  # 20년이상
        '95',  # 미상
    ]

    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)
    json_ov_lv3 = generate_field_list_target('OV_L3_ID', ov_lv3)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2 + json_ov_lv3
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B81A16',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- death (monthly) ------------------------------------------------------------------------------------------------------------------------

def upload_death(from_year=1997, from_month=1, till_year=2020, till_month=12, pop_conn=None):
    data_name = 'death'
    db_table = 'kosis_death'

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    year_month_patt = re.compile(r'(?P<year>\d+)\.(?P<month>\d+)')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_folder, pop_conn=pop_conn)

        if year_folder == from_year:
            start_month = from_month
        else:
            start_month = 1
        if year_folder == till_year:
            end_month = till_month
        else:
            end_month = 12

        month_path_names = [m_path.name for m_path in year_path.iterdir() if m_path.is_dir() and start_month <= int(m_path.name) <= end_month]
        month_path_names = natsorted(month_path_names)
        month_paths = [year_path / month_path_name for month_path_name in month_path_names]
        for month_path in month_paths:
            file_paths = [f_path for f_path in month_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
            for file_path in file_paths:
                # read file
                df = pd.read_csv(file_path.as_posix(), dtype='string')
                for idx, row in df.iterrows():
                    year_month_match = year_month_patt.search(row['시점'])
                    year = int(year_month_match.group('year'))
                    month = int(year_month_match.group('month'))

                    kosis_admin_div_code = row['[A]시군구별']
                    if year < 2014:
                        if kosis_admin_div_code in ['33040', '33041', '33042', '33043', '33044']:
                            continue
                    else:
                        if kosis_admin_div_code in ['33010', '33011', '33012']:
                            continue
                    admin_div_num = kosis_admin_div_code_to_admin_div_num[kosis_admin_div_code]

                    unique_keys = {
                        'admin_div_num': admin_div_num,
                        'year': year,
                        'month': month,
                        'age_type': '----',
                    }

                    row_data = {}
                    csv_col_to_db_col = {
                        '사망자수': csv_sex_to_sex_col[row['[SBB]성별']],
                    }
                    for key, value in csv_col_to_db_col.items():
                        row_data.update({value: str_to_int(row[key])})
                    if all(value is None for value in row_data.values()):
                        continue

                    curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                    if curr_row:
                        for value_key in row_data.keys():
                            if curr_row[value_key] != row_data[value_key]:
                                if curr_row[value_key] is None:
                                    update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                    if update_success:
                                        print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                                elif row_data[value_key] is not None:
                                    error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                    raise ValueError(error_msg)
                    else:
                        new_row = unique_keys.copy()
                        new_row.update(row_data)
                        insert_success = insert_dict(pop_conn, db_table, new_row)
                        if insert_success:
                            print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                        else:
                            error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                            raise ValueError(error_msg)

                pop_conn.commit()
                print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
                print()

    return


def download_death(admin_div_nums=None, from_year=1997, from_month=1, till_year=2020, till_month=12, pop_conn=None):
    data_name = 'death'
    # 시군구/월별 사망자수(1997~)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        admin_div_nums_and_codes = get_admin_div_nums_and_kosis_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_nums_and_codes:
            continue

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

            for admin_div_num_and_code in admin_div_nums_and_codes:
                request_data = get_request_data_for_death(admin_div_num_and_code[1], year, month, pop_conn=pop_conn)

                filename = f"{convert_admin_div_num_to_admin_div_n5_code(admin_div_num_and_code[0])}_{year:0>4}_{month:0>2}_{data_name}.csv"
                file_path = dir_path / filename

                get_and_save_kosis_large_data(request_data, file_path)
                sleep(15)

    return


def get_request_data_for_death(kosis_admin_div_code: str, year: int, month: int, pop_conn=None):
    items = [
        'T1',  # 사망자수
    ]
    ov_lv2 = [
        '0',  # 계
        '1',  # 남자
        '2',  # 여자
    ]

    if not kosis_admin_div_code:
        error_msg = 'kosis_admin_div_code is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)
    if month is None:
        error_msg = 'month is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_kosis_admin_div_codes = get_jr_kosis_admin_div_codes(kosis_admin_div_code, year, pop_conn=pop_conn)
    if not jr_kosis_admin_div_codes:
        return

    kosis_admin_div_codes = [kosis_admin_div_code]
    kosis_admin_div_codes += jr_kosis_admin_div_codes

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"M,{year:0>4}{month:0>2},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B82A01',
        'fieldList': field_list,
        'colAxis': 'TIME',
        'rowAxis': 'A,SBB',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- death_by_stack (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_death_by_stack(from_year=2000, till_year=2020, pop_conn=None):
    data_name = 'death_by_stack'
    db_table = 'kosis_death'

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_folder, pop_conn=pop_conn)

        file_paths = [f_path for f_path in year_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
        for file_path in file_paths:
            # read file
            df = pd.read_csv(file_path.as_posix(), dtype='string')
            for idx, row in df.iterrows():
                year = int(integer_patt.search(row['시점']).group())

                kosis_admin_div_code = row['[S]시군구별']
                if year < 2014:
                    if kosis_admin_div_code in ['33040', '33041', '33042', '33043', '33044']:
                        continue
                else:
                    if kosis_admin_div_code in ['33010', '33011', '33012']:
                        continue
                admin_div_num = kosis_admin_div_code_to_admin_div_num[kosis_admin_div_code]
                age_type = csv_death_stack_to_age_type[row['[YRE]연령(5세)별']]

                unique_keys = {
                    'admin_div_num': admin_div_num,
                    'year': year,
                    'month': 0,
                    'age_type': age_type,
                }

                row_data = {}
                csv_col_to_db_col = {
                    '사망[명]': csv_sex_to_sex_col[row['[SBB]성별']],
                }
                for key, value in csv_col_to_db_col.items():
                    row_data.update({value: str_to_int(row[key])})
                if all(value is None for value in row_data.values()):
                    continue

                curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                if curr_row:
                    for value_key in row_data.keys():
                        if curr_row[value_key] != row_data[value_key]:
                            if curr_row[value_key] is None:
                                update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                if update_success:
                                    print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                            elif row_data[value_key] is not None:
                                error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                raise ValueError(error_msg)
                else:
                    new_row = unique_keys.copy()
                    new_row.update(row_data)
                    insert_success = insert_dict(pop_conn, db_table, new_row)
                    if insert_success:
                        print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                    else:
                        error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                        raise ValueError(error_msg)

            pop_conn.commit()
            print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
            print()

    return


def download_death_by_stack(admin_div_nums=None, from_year=1997, till_year=2020, pop_conn=None):
    data_name = 'death_by_stack'
    # 시군구/성/연령(5세)별 사망자수(1997~), 사망률(1998~)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        dir_path = Path(data_dir, source_name, data_name, str(year))

        admin_div_nums_and_codes = get_admin_div_nums_and_kosis_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_nums_and_codes:
            continue

        for admin_div_num_and_code in admin_div_nums_and_codes:
            request_data = get_request_data_for_death_by_stack(admin_div_num_and_code[1], year, pop_conn=pop_conn)

            filename = f"{convert_admin_div_num_to_admin_div_n5_code(admin_div_num_and_code[0])}_{year:0>4}_{data_name}.csv"
            file_path = dir_path / filename

            get_and_save_kosis_large_data(request_data, file_path)
            sleep(20)

    return


def get_request_data_for_death_by_stack(kosis_admin_div_code: str, year: int, pop_conn=None):
    items = [
        'T2',  # 사망 (명)
        'T4',  # 사망률 (십만명당)
    ]
    ov_lv1 = [
        '0',  # 계
        '1',  # 남자
        '2',  # 여자
    ]
    ov_lv2 = [
        '000',  # 계
        '520',  # 0세
        '040',  # 1 - 4세
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
        '340',  # 80세 이상
        '360',  # 80 - 84세
        '380',  # 85 - 89세
        '390',  # 90세 이상
        '990',  # 연령미상
    ]

    if not kosis_admin_div_code:
        error_msg = 'kosis_admin_div_code is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_kosis_admin_div_codes = get_jr_kosis_admin_div_codes(kosis_admin_div_code, year, pop_conn=pop_conn)
    if not jr_kosis_admin_div_codes:
        return

    kosis_admin_div_codes = [kosis_admin_div_code]
    kosis_admin_div_codes += jr_kosis_admin_div_codes

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', ov_lv1)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)
    json_ov_lv3 = generate_field_list_target('OV_L3_ID', kosis_admin_div_codes)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2 + json_ov_lv3
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B80A18',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'SBB,YRE,S',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- death_by_age (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_death_by_age(from_year=1983, till_year=2020, pop_conn=None):
    data_name = 'death_by_age'
    db_table = 'kosis_death'

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        year_fname = int(file_path.stem[:4])
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_fname, pop_conn=pop_conn)

        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())

            admin_div_num = kosis_admin_div_code_to_admin_div_num[row['[B]시도별']]
            age_type = csv_death_age_to_age_type[row['[D]연령(5세,각세)별']]

            unique_keys = {
                'admin_div_num': admin_div_num,
                'year': year,
                'month': 0,
                'age_type': age_type,
            }

            row_data = {}
            csv_col_to_db_col = {
                '사망[명]': csv_sex_to_sex_col[row['[SBB]성별']],
            }
            for key, value in csv_col_to_db_col.items():
                row_data.update({value: str_to_int(row[key])})
            if all(value is None for value in row_data.values()):
                continue

            curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
            if curr_row:
                for value_key in row_data.keys():
                    if curr_row[value_key] != row_data[value_key]:
                        if curr_row[value_key] is None:
                            update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                            if update_success:
                                print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                        elif row_data[value_key] is not None:
                            error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                            raise ValueError(error_msg)
            else:
                new_row = unique_keys.copy()
                new_row.update(row_data)
                insert_success = insert_dict(pop_conn, db_table, new_row)
                if insert_success:
                    print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                else:
                    error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                    raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_death_by_age(from_year=1983, till_year=2020, pop_conn=None):
    data_name = 'death_by_age'
    # 시도/성/연령(각세)별 사망자수

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):

        kosis_admin_div_codes = get_kosis_admin_div_codes_level_0_and_1([], year, pop_conn=pop_conn)
        request_data = get_request_data_for_death_by_age(kosis_admin_div_codes, year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(20)

    return


def get_request_data_for_death_by_age(kosis_admin_div_codes: list, year: int):
    items = [
        'T2',  # 사망 (명)
    ]
    ov_lv2 = [
        '0',  # 계
        '1',  # 남자
        '2',  # 여자
    ]
    ov_lv3 = [
        '1000',    # 계
        '100100',  # 0세
        '100101',  # 1세
        '100102',  # 2세
        '100103',  # 3세
        '100104',  # 4세
        '100500',  # 5세
        '100501',  # 6세
        '100502',  # 7세
        '100503',  # 8세
        '100504',  # 9세
        '101000',  # 10세
        '101001',  # 11세
        '101002',  # 12세
        '101003',  # 13세
        '101004',  # 14세
        '101500',  # 15세
        '101501',  # 16세
        '101502',  # 17세
        '101503',  # 18세
        '101504',  # 19세
        '102000',  # 20세
        '102001',  # 21세
        '102002',  # 22세
        '102003',  # 23세
        '102004',  # 24세
        '102500',  # 25세
        '102501',  # 26세
        '102502',  # 27세
        '102503',  # 28세
        '102504',  # 29세
        '103000',  # 30세
        '103001',  # 31세
        '103002',  # 32세
        '103003',  # 33세
        '103004',  # 34세
        '103500',  # 35세
        '103501',  # 36세
        '103502',  # 37세
        '103503',  # 38세
        '103504',  # 39세
        '104000',  # 40세
        '104001',  # 41세
        '104002',  # 42세
        '104003',  # 43세
        '104004',  # 44세
        '104500',  # 45세
        '104501',  # 46세
        '104502',  # 47세
        '104503',  # 48세
        '104504',  # 49세
        '105000',  # 50세
        '105001',  # 51세
        '105002',  # 52세
        '105003',  # 53세
        '105004',  # 54세
        '105500',  # 55세
        '105501',  # 56세
        '105502',  # 57세
        '105503',  # 58세
        '105504',  # 59세
        '106000',  # 60세
        '106001',  # 61세
        '106002',  # 62세
        '106003',  # 63세
        '106004',  # 64세
        '106500',  # 65세
        '106501',  # 66세
        '106502',  # 67세
        '106503',  # 68세
        '106504',  # 69세
        '107000',  # 70세
        '107001',  # 71세
        '107002',  # 72세
        '107003',  # 73세
        '107004',  # 74세
        '107500',  # 75세
        '107501',  # 76세
        '107502',  # 77세
        '107503',  # 78세
        '107504',  # 79세
        '108000',  # 80세
        '108001',  # 81세
        '108002',  # 82세
        '108003',  # 83세
        '108004',  # 84세
        '108500',  # 85세
        '108501',  # 86세
        '108502',  # 87세
        '108503',  # 88세
        '108504',  # 89세
        '109000',  # 90세
        '109001',  # 91세
        '109002',  # 92세
        '109003',  # 93세
        '109004',  # 94세
        '109500',  # 95세
        '109501',  # 96세
        '109502',  # 97세
        '109503',  # 98세
        '109504',  # 99세
        '1100',    # 100세이상
        '1999',    # 연령미상
    ]

    if not kosis_admin_div_codes:
        error_msg = 'kosis_admin_div_codes is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)
    json_ov_lv3 = generate_field_list_target('OV_L3_ID', ov_lv3)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2 + json_ov_lv3
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B80A11',
        'fieldList': field_list,
        'colAxis': 'TIME',
        'rowAxis': 'B,SBB,D',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- death by marital status (annual) ------------------------------------------------------------------------------------------------------------------------

def download_death_by_marital_status(from_year=1983, till_year=2020):
    data_name = 'death_by_marital_status'
    # 성/연령/혼인상태별 사망자수

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        request_data = get_request_data_for_death_by_marital_status(year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(15)

    return


def get_request_data_for_death_by_marital_status(year: int):
    items = [
        'T1',  # 계
        'T2',  # 미혼
        'T3',  # 유배우
        'T4',  # 이혼
        'T5',  # 사별
        'T6',  # 미상
    ]
    ov_lv1 = [
        '0',  # 계
        '1',  # 남자
        '2',  # 여자
    ]
    ov_lv2 = [
        '000',  # 계
        '081',  # 14세 이하
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
        '340',  # 80세 이상
        '360',  # 80 - 84세
        '380',  # 85 - 89세
        '390',  # 90세 이상
        '410',  # 90 - 94세
        '430',  # 95 - 99세
        '440',  # 100세 이상
        '990',  # 연령미상
    ]

    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', ov_lv1)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B80A14',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'SBB,YRE',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- marriage (monthly) ------------------------------------------------------------------------------------------------------------------------

def upload_marriage(from_year=1997, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'marriage'
    db_table = 'kosis_marriage'
    csv_col_to_db_col = {
        '혼인': 'marriage',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    year_month_patt = re.compile(r'(?P<year>\d+)\.(?P<month>\d+)')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_folder, pop_conn=pop_conn)

        if year_folder == from_year:
            start_month = from_month
        else:
            start_month = 1
        if year_folder == till_year:
            end_month = till_month
        else:
            end_month = 12

        month_path_names = [m_path.name for m_path in year_path.iterdir() if m_path.is_dir() and start_month <= int(m_path.name) <= end_month]
        month_path_names = natsorted(month_path_names)
        month_paths = [year_path / month_path_name for month_path_name in month_path_names]
        for month_path in month_paths:
            file_paths = [f_path for f_path in month_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
            for file_path in file_paths:
                # read file
                df = pd.read_csv(file_path.as_posix(), dtype='string')
                for idx, row in df.iterrows():
                    year_month_match = year_month_patt.search(row['시점'])
                    year = int(year_month_match.group('year'))
                    month = int(year_month_match.group('month'))

                    kosis_admin_div_code = row['[A]시군구별']
                    if year < 2014:
                        if kosis_admin_div_code in ['33040', '33041', '33042', '33043', '33044']:
                            continue
                    else:
                        if kosis_admin_div_code in ['33010', '33011', '33012']:
                            continue
                    admin_div_num = kosis_admin_div_code_to_admin_div_num[kosis_admin_div_code]

                    unique_keys = {
                        'admin_div_num': admin_div_num,
                        'year': year,
                        'month': month,
                        'husband_age_type': '----',
                        'wife_age_type': '----',
                    }

                    row_data = {}
                    for key, value in csv_col_to_db_col.items():
                        row_data.update({value: str_to_int(row[key])})
                    if all(value is None for value in row_data.values()):
                        continue

                    curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                    if curr_row:
                        for value_key in row_data.keys():
                            if curr_row[value_key] != row_data[value_key]:
                                if curr_row[value_key] is None:
                                    update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                    if update_success:
                                        print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                                elif row_data[value_key] is not None:
                                    error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                    raise ValueError(error_msg)
                    else:
                        new_row = unique_keys.copy()
                        new_row.update(row_data)
                        insert_success = insert_dict(pop_conn, db_table, new_row)
                        if insert_success:
                            print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                        else:
                            error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                            raise ValueError(error_msg)

                pop_conn.commit()
                print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
                print()

    return


def download_marriage(admin_div_nums=None, from_year=1997, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'marriage'
    # 시도/시군구/월별 혼인

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        admin_div_nums_and_codes = get_admin_div_nums_and_kosis_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_nums_and_codes:
            continue

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

            for admin_div_num_and_code in admin_div_nums_and_codes:
                request_data = get_request_data_for_marriage(admin_div_num_and_code[1], year, month, pop_conn=pop_conn)

                filename = f"{convert_admin_div_num_to_admin_div_n5_code(admin_div_num_and_code[0])}_{year:0>4}_{month:0>2}_{data_name}.csv"
                file_path = dir_path / filename

                get_and_save_kosis_large_data(request_data, file_path)
                sleep(15)

    return


def get_request_data_for_marriage(kosis_admin_div_code: str, year: int, month: int, pop_conn=None):
    items = [
        'T3',  # 혼인
    ]

    if not kosis_admin_div_code:
        error_msg = 'kosis_admin_div_code is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)
    if month is None:
        error_msg = 'month is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_kosis_admin_div_codes = get_jr_kosis_admin_div_codes(kosis_admin_div_code, year, pop_conn=pop_conn, exclude_overseas=False)
    if not jr_kosis_admin_div_codes:
        return

    kosis_admin_div_codes = [kosis_admin_div_code]
    kosis_admin_div_codes += jr_kosis_admin_div_codes

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"M,{year:0>4}{month:0>2},@"}}'] + json_items + json_ov_lv1
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B83A35',
        'fieldList': field_list,
        'colAxis': 'TIME',
        'rowAxis': 'A',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- marriage_matrix_by_age (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_marriage_matrix_by_age(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'marriage_matrix_by_age'
    db_table = 'kosis_marriage'
    csv_col_to_db_col = {
        '혼인[건]': 'marriage',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())
            husband_age_type = csv_marriage_age_to_age_type[row['[H]남편의 연령별']]
            wife_age_type = csv_marriage_age_to_age_type[row['[I]아내의 연령별']]

            unique_keys = {
                'admin_div_num': 0,
                'year': year,
                'month': 0,
                'husband_age_type': husband_age_type,
                'wife_age_type': wife_age_type,
            }

            row_data = {}
            for key, value in csv_col_to_db_col.items():
                row_data.update({value: str_to_int(row[key])})
            if all(value is None for value in row_data.values()):
                continue

            curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
            if curr_row:
                for value_key in row_data.keys():
                    if curr_row[value_key] != row_data[value_key]:
                        if curr_row[value_key] is None:
                            update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                            if update_success:
                                print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                        elif row_data[value_key] is not None:
                            error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                            raise ValueError(error_msg)
            else:
                new_row = unique_keys.copy()
                new_row.update(row_data)
                insert_success = insert_dict(pop_conn, db_table, new_row)
                if insert_success:
                    print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                else:
                    error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                    raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_marriage_matrix_by_age(from_year=1990, till_year=2020):
    data_name = 'marriage_matrix_by_age'
    # 혼인부부의 연령별 혼인

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        request_data = get_request_data_for_marriage_matrix_by_age(year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(20)

    return


def get_request_data_for_marriage_matrix_by_age(year: int):
    items = [
        'T3',  # 혼인
    ]
    ov_lv1 = [
        '000',  # 계
        '160',  # 15세미만
        '200',  # 15 - 19세
        '251',  # 20세
        '252',  # 21세
        '253',  # 22세
        '254',  # 23세
        '255',  # 24세
        '301',  # 25세
        '302',  # 26세
        '303',  # 27세
        '304',  # 28세
        '305',  # 29세
        '351',  # 30세
        '352',  # 31세
        '353',  # 32세
        '354',  # 33세
        '355',  # 34세
        '401',  # 35세
        '402',  # 36세
        '403',  # 37세
        '404',  # 38세
        '405',  # 39세
        '451',  # 40세
        '452',  # 41세
        '453',  # 42세
        '454',  # 43세
        '455',  # 44세
        '500',  # 45 - 49세
        '550',  # 50 - 54세
        '600',  # 55 - 59세
        '650',  # 60 - 64세
        '700',  # 65 - 69세
        '750',  # 70 - 74세
        '800',  # 75세이상
        '950',  # 미상
    ]
    ov_lv2 = [
        '000',  # 계
        '160',  # 15세미만
        '201',  # 15세
        '202',  # 16세
        '203',  # 17세
        '204',  # 18세
        '205',  # 19세
        '251',  # 20세
        '252',  # 21세
        '253',  # 22세
        '254',  # 23세
        '255',  # 24세
        '301',  # 25세
        '302',  # 26세
        '303',  # 27세
        '304',  # 28세
        '305',  # 29세
        '351',  # 30세
        '352',  # 31세
        '353',  # 32세
        '354',  # 33세
        '355',  # 34세
        '401',  # 35세
        '402',  # 36세
        '403',  # 37세
        '404',  # 38세
        '405',  # 39세
        '450',  # 40 - 44세
        '500',  # 45 - 49세
        '550',  # 50 - 54세
        '600',  # 55 - 59세
        '650',  # 60 - 64세
        '700',  # 65 - 69세
        '750',  # 70 - 74세
        '800',  # 75세이상
        '950',  # 미상
    ]

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', ov_lv1)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B83A02',
        'fieldList': field_list,
        'colAxis': 'TIME',
        'rowAxis': 'H,I',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- marriage_matrix_by_stack (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_marriage_matrix_by_stack(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'marriage_matrix_by_stack'
    db_table = 'kosis_marriage'
    value_col_to_wife_age_type = {
        '아내의 연령별: 계': '----',
        '15세미만': '<015',
        '15 - 19세': '1519',
        '20 - 24세': '2024',
        '25 - 29세': '2529',
        '30 - 34세': '3034',
        '35 - 39세': '3539',
        '40 - 44세': '4044',
        '45 - 49세': '4549',
        '50 - 54세': '5054',
        '55 - 59세': '5559',
        '60 - 64세': '6064',
        '65 - 69세': '6569',
        '70 - 74세': '7074',
        '75세이상': '+075',
        '미상': 'XXXX',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        year_fname = int(file_path.stem[:4])
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_fname, pop_conn=pop_conn)

        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())

            admin_div_num = kosis_admin_div_code_to_admin_div_num[row['[A]시도별']]
            husband_age_type = csv_marriage_stack_to_age_type[row['[C]남편의 연령별']]

            for key, value in value_col_to_wife_age_type.items():
                unique_keys = {
                    'admin_div_num': admin_div_num,
                    'year': year,
                    'month': 0,
                    'husband_age_type': husband_age_type,
                    'wife_age_type': value,
                }

                row_data = {'marriage': str_to_int(row[key])}
                if all(value is None for value in row_data.values()):
                    continue

                curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                if curr_row:
                    for value_key in row_data.keys():
                        if curr_row[value_key] != row_data[value_key]:
                            if curr_row[value_key] is None:
                                update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                if update_success:
                                    print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                            elif row_data[value_key] is not None:
                                error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                raise ValueError(error_msg)
                else:
                    new_row = unique_keys.copy()
                    new_row.update(row_data)
                    insert_success = insert_dict(pop_conn, db_table, new_row)
                    if insert_success:
                        print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                    else:
                        error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                        raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_marriage_matrix_by_stack(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'marriage_matrix_by_stack'
    # 시도/연령(5세)별 혼인

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        kosis_admin_div_codes = get_kosis_admin_div_codes_level_0_and_1([], year, pop_conn=pop_conn, exclude_overseas=False)
        request_data = get_request_data_for_marriage_matrix_by_stack(kosis_admin_div_codes, year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(20)

    return


def get_request_data_for_marriage_matrix_by_stack(kosis_admin_div_codes: list, year: int):
    items = [
        'T05',  # 아내의 연령별: 계
        'T10',  # 15세미만
        'T15',  # 15 - 19세
        'T20',  # 20 - 24세
        'T25',  # 25 - 29세
        'T30',  # 30 - 34세
        'T35',  # 35 - 39세
        'T40',  # 40 - 44세
        'T45',  # 45 - 49세
        'T50',  # 50 - 54세
        'T55',  # 55 - 59세
        'T60',  # 60 - 64세
        'T65',  # 65 - 69세
        'T70',  # 70 - 74세
        'T75',  # 75세이상
        'T80',  # 미상
    ]
    ov_lv2 = [
        '00',  # 계
        '16',  # 15세 미만
        '20',  # 15 - 19세
        '25',  # 20 - 24세
        '30',  # 25 - 29세
        '35',  # 30 - 34세
        '40',  # 35 - 39세
        '45',  # 40 - 44세
        '50',  # 45 - 49세
        '55',  # 50 - 54세
        '60',  # 55 - 59세
        '65',  # 60 - 64세
        '70',  # 65 - 69세
        '75',  # 70 - 74세
        '80',  # 75세 이상
        '95',  # 미상
    ]

    if not kosis_admin_div_codes:
        error_msg = 'kosis_admin_div_codes is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B83A33',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A,C',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- marriage_by_age (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_marriage_by_age(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'marriage_by_age'
    db_table = 'kosis_marriage_age'
    csv_col_to_db_col = {
        '남편': 'husband',
        '아내': 'wife',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        year_fname = int(file_path.stem[:4])
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_fname, pop_conn=pop_conn)

        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())

            admin_div_num = kosis_admin_div_code_to_admin_div_num[row['[B]시도별']]
            age_type = csv_marriage_age_to_age_type[row['[E]연령별']]

            unique_keys = {
                'admin_div_num': admin_div_num,
                'year': year,
                'age_type': age_type,
            }

            row_data = {}
            for key, value in csv_col_to_db_col.items():
                row_data.update({value: str_to_int(row[key])})
            if all(value is None for value in row_data.values()):
                continue

            curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
            if curr_row:
                for value_key in row_data.keys():
                    if curr_row[value_key] != row_data[value_key]:
                        if curr_row[value_key] is None:
                            update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                            if update_success:
                                print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                        elif row_data[value_key] is not None:
                            error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                            raise ValueError(error_msg)
            else:
                new_row = unique_keys.copy()
                new_row.update(row_data)
                insert_success = insert_dict(pop_conn, db_table, new_row)
                if insert_success:
                    print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                else:
                    error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                    raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_marriage_by_age(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'marriage_by_age'
    # 시도/연령(각세)별 혼인

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        kosis_admin_div_codes = get_kosis_admin_div_codes_level_0_and_1([], year, pop_conn=pop_conn, exclude_overseas=False)
        request_data = get_request_data_for_marriage_by_age(kosis_admin_div_codes, year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(20)

    return


def get_request_data_for_marriage_by_age(kosis_admin_div_codes: list, year: int):
    items = [
        'T10',  # 남편
        'T20',  # 아내
    ]
    ov_lv2 = [
        '000',  # 계
        '160',  # 15세미만
        '201',  # 15세
        '202',  # 16세
        '203',  # 17세
        '204',  # 18세
        '205',  # 19세
        '251',  # 20세
        '252',  # 21세
        '253',  # 22세
        '254',  # 23세
        '255',  # 24세
        '301',  # 25세
        '302',  # 26세
        '303',  # 27세
        '304',  # 28세
        '305',  # 29세
        '351',  # 30세
        '352',  # 31세
        '353',  # 32세
        '354',  # 33세
        '355',  # 34세
        '401',  # 35세
        '402',  # 36세
        '403',  # 37세
        '404',  # 38세
        '405',  # 39세
        '451',  # 40세
        '452',  # 41세
        '453',  # 42세
        '454',  # 43세
        '455',  # 44세
        '501',  # 45세
        '502',  # 46세
        '503',  # 47세
        '504',  # 48세
        '505',  # 49세
        '551',  # 50세
        '552',  # 51세
        '553',  # 52세
        '554',  # 53세
        '555',  # 54세
        '601',  # 55세
        '602',  # 56세
        '603',  # 57세
        '604',  # 58세
        '605',  # 59세
        '651',  # 60세
        '652',  # 61세
        '653',  # 62세
        '654',  # 63세
        '655',  # 64세
        '701',  # 65세
        '702',  # 66세
        '703',  # 67세
        '704',  # 68세
        '705',  # 69세
        '751',  # 70세
        '752',  # 71세
        '753',  # 72세
        '754',  # 73세
        '755',  # 74세
        '801',  # 75세
        '806',  # 76세이상
        '950',  # 미상
    ]

    if not kosis_admin_div_codes:
        error_msg = 'kosis_admin_div_codes is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B83A03',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'B,E',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- divorce (monthly) ------------------------------------------------------------------------------------------------------------------------

def upload_divorce(from_year=1997, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'divorce'
    db_table = 'kosis_divorce'
    csv_col_to_db_col = {
        '이혼': 'divorce',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    year_month_patt = re.compile(r'(?P<year>\d+)\.(?P<month>\d+)')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_folder, pop_conn=pop_conn)

        if year_folder == from_year:
            start_month = from_month
        else:
            start_month = 1
        if year_folder == till_year:
            end_month = till_month
        else:
            end_month = 12

        month_path_names = [m_path.name for m_path in year_path.iterdir() if m_path.is_dir() and start_month <= int(m_path.name) <= end_month]
        month_path_names = natsorted(month_path_names)
        month_paths = [year_path / month_path_name for month_path_name in month_path_names]
        for month_path in month_paths:
            file_paths = [f_path for f_path in month_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
            for file_path in file_paths:
                # read file
                df = pd.read_csv(file_path.as_posix(), dtype='string')
                for idx, row in df.iterrows():
                    year_month_match = year_month_patt.search(row['시점'])
                    year = int(year_month_match.group('year'))
                    month = int(year_month_match.group('month'))

                    kosis_admin_div_code = row['[A]시군구별']
                    if year < 2014:
                        if kosis_admin_div_code in ['33040', '33041', '33042', '33043', '33044']:
                            continue
                    else:
                        if kosis_admin_div_code in ['33010', '33011', '33012']:
                            continue
                    admin_div_num = kosis_admin_div_code_to_admin_div_num[kosis_admin_div_code]

                    unique_keys = {
                        'admin_div_num': admin_div_num,
                        'year': year,
                        'month': month,
                        'husband_age_type': '----',
                        'wife_age_type': '----',
                    }

                    row_data = {}
                    for key, value in csv_col_to_db_col.items():
                        row_data.update({value: str_to_int(row[key])})
                    if all(value is None for value in row_data.values()):
                        continue

                    curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                    if curr_row:
                        for value_key in row_data.keys():
                            if curr_row[value_key] != row_data[value_key]:
                                if curr_row[value_key] is None:
                                    update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                    if update_success:
                                        print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                                elif row_data[value_key] is not None:
                                    error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                    raise ValueError(error_msg)
                    else:
                        new_row = unique_keys.copy()
                        new_row.update(row_data)
                        insert_success = insert_dict(pop_conn, db_table, new_row)
                        if insert_success:
                            print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                        else:
                            error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                            raise ValueError(error_msg)

                pop_conn.commit()
                print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
                print()

    return


def download_divorce(admin_div_nums=None, from_year=1997, from_month=1, till_year=2021, till_month=12, pop_conn=None):
    data_name = 'divorce'
    # 시도/시군구/월별 이혼

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        admin_div_nums_and_codes = get_admin_div_nums_and_kosis_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_nums_and_codes:
            continue

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

            for admin_div_num_and_code in admin_div_nums_and_codes:
                request_data = get_request_data_for_divorce(admin_div_num_and_code[1], year, month, pop_conn=pop_conn)

                filename = f"{convert_admin_div_num_to_admin_div_n5_code(admin_div_num_and_code[0])}_{year:0>4}_{month:0>2}_{data_name}.csv"
                file_path = dir_path / filename

                get_and_save_kosis_large_data(request_data, file_path)
                sleep(15)

    return


def get_request_data_for_divorce(kosis_admin_div_code: str, year: int, month: int, pop_conn=None):
    items = [
        'T4',  # 이혼
    ]

    if not kosis_admin_div_code:
        error_msg = 'kosis_admin_div_code is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)
    if month is None:
        error_msg = 'month is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_kosis_admin_div_codes = get_jr_kosis_admin_div_codes(kosis_admin_div_code, year, pop_conn=pop_conn, exclude_overseas=False)
    if not jr_kosis_admin_div_codes:
        return

    kosis_admin_div_codes = [kosis_admin_div_code]
    kosis_admin_div_codes += jr_kosis_admin_div_codes

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"M,{year:0>4}{month:0>2},@"}}'] + json_items + json_ov_lv1
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B85033',
        'fieldList': field_list,
        'colAxis': 'TIME',
        'rowAxis': 'A',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- divorce_matrix_by_age (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_divorce_matrix_by_age(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'divorce_matrix_by_age'
    db_table = 'kosis_divorce'
    csv_col_to_db_col = {
        '이혼': 'divorce',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())
            husband_age_type = csv_marriage_age_to_age_type[row['[H]남편의 연령별']]
            wife_age_type = csv_marriage_age_to_age_type[row['[I]아내의 연령별']]

            unique_keys = {
                'admin_div_num': 0,
                'year': year,
                'month': 0,
                'husband_age_type': husband_age_type,
                'wife_age_type': wife_age_type,
            }

            row_data = {}
            for key, value in csv_col_to_db_col.items():
                row_data.update({value: str_to_int(row[key])})
            if all(value is None for value in row_data.values()):
                continue

            curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
            if curr_row:
                for value_key in row_data.keys():
                    if curr_row[value_key] != row_data[value_key]:
                        if curr_row[value_key] is None:
                            update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                            if update_success:
                                print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                        elif row_data[value_key] is not None:
                            error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                            raise ValueError(error_msg)
            else:
                new_row = unique_keys.copy()
                new_row.update(row_data)
                insert_success = insert_dict(pop_conn, db_table, new_row)
                if insert_success:
                    print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                else:
                    error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                    raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_divorce_matrix_by_age(from_year=1990, till_year=2020):
    data_name = 'divorce_matrix_by_age'
    # 이혼부부의 연령별 이혼

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        request_data = get_request_data_for_divorce_matrix_by_age(year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(20)

    return


def get_request_data_for_divorce_matrix_by_age(year: int):
    items = [
        'T4',  # 이혼
    ]
    ov_lv1 = [
        '000',  # 계
        '160',  # 15세미만
        '200',  # 15 - 19세
        '250',  # 20 - 24세
        '300',  # 25 - 29세
        '350',  # 30 - 34세
        '401',  # 35세
        '402',  # 36세
        '403',  # 37세
        '404',  # 38세
        '405',  # 39세
        '451',  # 40세
        '452',  # 41세
        '453',  # 42세
        '454',  # 43세
        '455',  # 44세
        '501',  # 45세
        '502',  # 46세
        '503',  # 47세
        '504',  # 48세
        '505',  # 49세
        '551',  # 50세
        '552',  # 51세
        '553',  # 52세
        '554',  # 53세
        '555',  # 54세
        '600',  # 55 - 59세
        '650',  # 60 - 64세
        '700',  # 65 - 69세
        '750',  # 70 - 74세
        '770',  # 75세이상
        '950',  # 미상
    ]
    ov_lv2 = [
        '000',  # 계
        '160',  # 15세미만
        '200',  # 15 - 19세
        '251',  # 20세
        '252',  # 21세
        '253',  # 22세
        '254',  # 23세
        '255',  # 24세
        '301',  # 25세
        '302',  # 26세
        '303',  # 27세
        '304',  # 28세
        '305',  # 29세
        '351',  # 30세
        '352',  # 31세
        '353',  # 32세
        '354',  # 33세
        '355',  # 34세
        '401',  # 35세
        '402',  # 36세
        '403',  # 37세
        '404',  # 38세
        '405',  # 39세
        '451',  # 40세
        '452',  # 41세
        '453',  # 42세
        '454',  # 43세
        '455',  # 44세
        '501',  # 45세
        '502',  # 46세
        '503',  # 47세
        '504',  # 48세
        '505',  # 49세
        '550',  # 50 - 54세
        '600',  # 55 - 59세
        '650',  # 60 - 64세
        '700',  # 65 - 69세
        '750',  # 70 - 74세
        '770',  # 75세이상
        '950',  # 미상
    ]

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', ov_lv1)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B85002',
        'fieldList': field_list,
        'colAxis': 'TIME',
        'rowAxis': 'H,I',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- divorce_matrix_by_stack (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_divorce_matrix_by_stack(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'divorce_matrix_by_stack'
    db_table = 'kosis_divorce'
    value_col_to_wife_age_type = {
        '아내의 연령별: 계': '----',
        '15세미만': '<015',
        '15 - 19세': '1519',
        '20 - 24세': '2024',
        '25 - 29세': '2529',
        '30 - 34세': '3034',
        '35 - 39세': '3539',
        '40 - 44세': '4044',
        '45 - 49세': '4549',
        '50 - 54세': '5054',
        '55 - 59세': '5559',
        '60 - 64세': '6064',
        '65 - 69세': '6569',
        '70 - 74세': '7074',
        '75세이상': '+075',
        '미상': 'XXXX',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        year_fname = int(file_path.stem[:4])
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_fname, pop_conn=pop_conn)

        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())

            admin_div_num = kosis_admin_div_code_to_admin_div_num[row['[B]시도별']]
            husband_age_type = csv_marriage_stack_to_age_type[row['[C]남편의 연령별']]

            for key, value in value_col_to_wife_age_type.items():
                unique_keys = {
                    'admin_div_num': admin_div_num,
                    'year': year,
                    'month': 0,
                    'husband_age_type': husband_age_type,
                    'wife_age_type': value,
                }

                row_data = {'divorce': str_to_int(row[key])}
                if all(value is None for value in row_data.values()):
                    continue

                curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                if curr_row:
                    for value_key in row_data.keys():
                        if curr_row[value_key] != row_data[value_key]:
                            if curr_row[value_key] is None:
                                update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                if update_success:
                                    print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                            elif row_data[value_key] is not None:
                                error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                raise ValueError(error_msg)
                else:
                    new_row = unique_keys.copy()
                    new_row.update(row_data)
                    insert_success = insert_dict(pop_conn, db_table, new_row)
                    if insert_success:
                        print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                    else:
                        error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                        raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_divorce_matrix_by_stack(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'divorce_matrix_by_stack'
    # 시도/연령(5세)별 이혼

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        kosis_admin_div_codes = get_kosis_admin_div_codes_level_0_and_1([], year, pop_conn=pop_conn, exclude_overseas=False)
        request_data = get_request_data_for_divorce_matrix_by_stack(kosis_admin_div_codes, year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(20)

    return


def get_request_data_for_divorce_matrix_by_stack(kosis_admin_div_codes: list, year: int):
    items = [
        'T05',  # 아내의 연령별: 계
        'T10',  # 15세미만
        'T15',  # 15 - 19세
        'T20',  # 20 - 24세
        'T25',  # 25 - 29세
        'T30',  # 30 - 34세
        'T35',  # 35 - 39세
        'T40',  # 40 - 44세
        'T45',  # 45 - 49세
        'T50',  # 50 - 54세
        'T55',  # 55 - 59세
        'T60',  # 60 - 64세
        'T65',  # 65 - 69세
        'T70',  # 70 - 74세
        'T75',  # 75세이상
        'T80',  # 미상
    ]
    ov_lv2 = [
        '00',  # 계
        '16',  # 15세 미만
        '20',  # 15 - 19세
        '25',  # 20 - 24세
        '30',  # 25 - 29세
        '35',  # 30 - 34세
        '40',  # 35 - 39세
        '45',  # 40 - 44세
        '50',  # 45 - 49세
        '55',  # 50 - 54세
        '60',  # 55 - 59세
        '65',  # 60 - 64세
        '70',  # 65 - 69세
        '75',  # 70 - 74세
        '80',  # 75세 이상
        '95',  # 미상
    ]

    if not kosis_admin_div_codes:
        error_msg = 'kosis_admin_div_codes is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B85027',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'B,C',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- divorce_by_age (annual) ------------------------------------------------------------------------------------------------------------------------

def upload_divorce_by_age(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'divorce_by_age'
    db_table = 'kosis_divorce_age'
    csv_col_to_db_col = {
        '남편': 'husband',
        '아내': 'wife',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    file_paths = [f_path for f_path in dir_path.iterdir() if f_path.is_file() and from_year <= int(f_path.stem[:4]) <= till_year and data_name in f_path.stem and f_path.suffix == '.csv']
    for file_path in file_paths:
        year_fname = int(file_path.stem[:4])
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_fname, pop_conn=pop_conn)

        # read file
        df = pd.read_csv(file_path.as_posix(), dtype='string')
        for idx, row in df.iterrows():
            year = int(integer_patt.search(row['시점']).group())

            admin_div_num = kosis_admin_div_code_to_admin_div_num[row['[B]시도별']]
            age_type = csv_marriage_age_to_age_type[row['[E]연령별']]

            unique_keys = {
                'admin_div_num': admin_div_num,
                'year': year,
                'age_type': age_type,
            }

            row_data = {}
            for key, value in csv_col_to_db_col.items():
                row_data.update({value: str_to_int(row[key])})
            if all(value is None for value in row_data.values()):
                continue

            curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
            if curr_row:
                for value_key in row_data.keys():
                    if curr_row[value_key] != row_data[value_key]:
                        if curr_row[value_key] is None:
                            update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                            if update_success:
                                print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                        elif row_data[value_key] is not None:
                            error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                            raise ValueError(error_msg)
            else:
                new_row = unique_keys.copy()
                new_row.update(row_data)
                insert_success = insert_dict(pop_conn, db_table, new_row)
                if insert_success:
                    print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                else:
                    error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                    raise ValueError(error_msg)

        pop_conn.commit()
        print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
        print()

    return


def download_divorce_by_age(from_year=1990, till_year=2020, pop_conn=None):
    data_name = 'divorce_by_age'
    # 시도/연령(각세)별 이혼

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    dir_path = Path(data_dir, source_name, data_name)

    for year in range(from_year, till_year + 1):
        kosis_admin_div_codes = get_kosis_admin_div_codes_level_0_and_1([], year, pop_conn=pop_conn, exclude_overseas=False)
        request_data = get_request_data_for_divorce_by_age(kosis_admin_div_codes, year)

        filename = f"{year:0>4}_{data_name}.csv"
        file_path = dir_path / filename

        get_and_save_kosis_large_data(request_data, file_path)
        sleep(20)

    return


def get_request_data_for_divorce_by_age(kosis_admin_div_codes: list, year: int):
    items = [
        'T10',  # 남편
        'T20',  # 아내
    ]
    ov_lv2 = [
        '000',  # 계
        '160',  # 15세미만
        '201',  # 15세
        '202',  # 16세
        '203',  # 17세
        '204',  # 18세
        '205',  # 19세
        '251',  # 20세
        '252',  # 21세
        '253',  # 22세
        '254',  # 23세
        '255',  # 24세
        '301',  # 25세
        '302',  # 26세
        '303',  # 27세
        '304',  # 28세
        '305',  # 29세
        '351',  # 30세
        '352',  # 31세
        '353',  # 32세
        '354',  # 33세
        '355',  # 34세
        '401',  # 35세
        '402',  # 36세
        '403',  # 37세
        '404',  # 38세
        '405',  # 39세
        '451',  # 40세
        '452',  # 41세
        '453',  # 42세
        '454',  # 43세
        '455',  # 44세
        '501',  # 45세
        '502',  # 46세
        '503',  # 47세
        '504',  # 48세
        '505',  # 49세
        '551',  # 50세
        '552',  # 51세
        '553',  # 52세
        '554',  # 53세
        '555',  # 54세
        '601',  # 55세
        '602',  # 56세
        '603',  # 57세
        '604',  # 58세
        '605',  # 59세
        '651',  # 60세
        '652',  # 61세
        '653',  # 62세
        '654',  # 63세
        '655',  # 64세
        '701',  # 65세
        '702',  # 66세
        '703',  # 67세
        '704',  # 68세
        '705',  # 69세
        '751',  # 70세
        '752',  # 71세
        '753',  # 72세
        '754',  # 73세
        '755',  # 74세
        '801',  # 75세
        '806',  # 76세이상
        '950',  # 미상
    ]

    if not kosis_admin_div_codes:
        error_msg = 'kosis_admin_div_codes is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"Y,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': 'DT_1B85003',
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'B,E',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


# # ---------- population by marital status ------------------------------------------------------------------------------------------------------------------------

def upload_population_by_marital_status(from_year=2015, till_year=2020, pop_conn=None):
    data_name = 'population_by_marital_status'
    db_table = 'kosis_population'
    csv_col_to_db_col = {
        '내국인(15세이상)-계': 'total',
        '내국인-미혼': 'never_married',
        '내국인-배우자있음': 'married',
        '내국인-사별': 'bereaved',
        '내국인-이혼': 'divorced',
        '남자(15세이상)-계': 'male_total',
        '남자-미혼': 'male_never_married',
        '남자-배우자있음': 'male_married',
        '남자-사별': 'male_bereaved',
        '남자-이혼': 'male_divorced',
        '여자(15세이상)-계': 'female_total',
        '여자-미혼': 'female_never_married',
        '여자-배우자있음': 'female_married',
        '여자-사별': 'female_bereaved',
        '여자-이혼': 'female_divorced',
    }

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    integer_patt = re.compile(r'\d+')

    dir_path = Path(data_dir, source_name, data_name)
    year_paths = [y_path for y_path in dir_path.iterdir() if y_path.is_dir() and from_year <= int(y_path.name) <= till_year]
    for year_path in year_paths:
        year_folder = int(year_path.name)
        kosis_admin_div_code_to_admin_div_num = get_kosis_admin_div_code_to_admin_div_num(year_folder, pop_conn=pop_conn)

        file_paths = [f_path for f_path in year_path.iterdir() if f_path.is_file() and data_name in f_path.stem and f_path.suffix == '.csv']
        for file_path in file_paths:
            # read file
            df = pd.read_csv(file_path.as_posix(), dtype='string')
            for idx, row in df.iterrows():
                year = int(integer_patt.search(row['시점']).group())

                admin_div_code = row['[A]행정구역별(시군구)']
                if admin_div_code[0] == '0' and admin_div_code != '00':
                    continue
                elif admin_div_code[2:] in ['003', '004', '005']:
                    continue
                admin_div_num = kosis_admin_div_code_to_admin_div_num[admin_div_code]
                age_type = csv_pop_age_to_age_type[row['[B]연령별']]

                unique_keys = {
                    'admin_div_num': admin_div_num,
                    'year': year,
                    'age_type': age_type,
                }

                row_data = {}
                for key, value in csv_col_to_db_col.items():
                    row_data.update({value: str_to_int(row[key])})
                if all(value is None for value in row_data.values()):
                    continue

                curr_row = select_one_row_pack_into_dict(pop_conn, db_table, unique_keys, [])
                if curr_row:
                    for value_key in row_data.keys():
                        if curr_row[value_key] != row_data[value_key]:
                            if curr_row[value_key] is None:
                                update_success = update_dict(pop_conn, db_table, unique_keys, {value_key: row_data[value_key]})
                                if update_success:
                                    print(f"  > Updated '{value_key}' in '{db_table}': {dict_to_set(unique_keys)}.")
                            elif row_data[value_key] is not None:
                                error_msg = f"Conflict with column '{value_key}' in {dict_to_set(unique_keys)}: current '{curr_row[value_key]}' vs new '{row_data[value_key]}'."
                                raise ValueError(error_msg)
                else:
                    new_row = unique_keys.copy()
                    new_row.update(row_data)
                    insert_success = insert_dict(pop_conn, db_table, new_row)
                    if insert_success:
                        print(f"  > Inserted into '{db_table}': {dict_to_set(unique_keys)}.")
                    else:
                        error_msg = f"DB insert failure with file '{file_path}': {dict_to_set(unique_keys)}."
                        raise ValueError(error_msg)

            pop_conn.commit()
            print(f">>> File '{file_path.name}' has been uploaded into '{db_table}'.")
            print()

    return


def download_population_by_marital_status(admin_div_nums=None, from_year=2015, till_year=2020, pop_conn=None):
    data_name = 'population_by_marital_status'
    # 연령별/성별/혼인상태별 인구(15세이상,내국인)-시군구
    years = [2015, 2020]

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    for year in range(from_year, till_year + 1):
        if year not in years:
            continue
        dir_path = Path(data_dir, source_name, data_name, str(year))

        admin_div_nums_and_codes = get_admin_div_nums_and_kosis_codes_level_0_and_1(admin_div_nums, year, pop_conn=pop_conn)
        if not admin_div_nums_and_codes:
            return

        for admin_div_num_and_code in admin_div_nums_and_codes:
            request_data = get_request_data_for_population_by_marital_status(admin_div_num_and_code[1], year, pop_conn=pop_conn)

            filename = f"{convert_admin_div_num_to_admin_div_n5_code(admin_div_num_and_code[0])}_{year:0>4}_{data_name}.csv"
            file_path = dir_path / filename

            get_and_save_kosis_large_data(request_data, file_path)
            sleep(20)

    return


def get_request_data_for_population_by_marital_status(kosis_admin_div_code: str, year: int, pop_conn=None):
    year_to_table_id = {
        2015: 'DT_1PM1503',  # 2015년 인구총조사
        2020: 'DT_1PM2002',  # 2020년 인구총조사
    }

    items = [
        'T10',  # 내국인(15세이상)-계
        'T11',  # 내국인-미혼
        'T12',  # 내국인-배우자있음
        'T13',  # 내국인-사별
        'T14',  # 내국인-이혼
        'T20',  # 남자(15세이상)-계
        'T21',  # 남자-미혼
        'T22',  # 남자-배우자있음
        'T23',  # 남자-사별
        'T24',  # 남자-이혼
        'T30',  # 여자(15세이상)-계
        'T31',  # 여자-미혼
        'T32',  # 여자-배우자있음
        'T33',  # 여자-사별
        'T34',  # 여자-이혼
    ]
    ov_lv2 = [
        '000',  # 합계
        '020001',  # 15세
        '020002',  # 16세
        '020003',  # 17세
        '020004',  # 18세
        '020005',  # 19세
        '025001',  # 20세
        '025002',  # 21세
        '025003',  # 22세
        '025004',  # 23세
        '025005',  # 24세
        '030001',  # 25세
        '030002',  # 26세
        '030003',  # 27세
        '030004',  # 28세
        '030005',  # 29세
        '035001',  # 30세
        '035002',  # 31세
        '035003',  # 32세
        '035004',  # 33세
        '035005',  # 34세
        '040001',  # 35세
        '040002',  # 36세
        '040003',  # 37세
        '040004',  # 38세
        '040005',  # 39세
        '045001',  # 40세
        '045002',  # 41세
        '045003',  # 42세
        '045004',  # 43세
        '045005',  # 44세
        '050001',  # 45세
        '050002',  # 46세
        '050003',  # 47세
        '050004',  # 48세
        '050005',  # 49세
        '055001',  # 50세
        '055002',  # 51세
        '055003',  # 52세
        '055004',  # 53세
        '055005',  # 54세
        '060001',  # 55세
        '060002',  # 56세
        '060003',  # 57세
        '060004',  # 58세
        '060005',  # 59세
        '065001',  # 60세
        '065002',  # 61세
        '065003',  # 62세
        '065004',  # 63세
        '065005',  # 64세
        '070001',  # 65세
        '070002',  # 66세
        '070003',  # 67세
        '070004',  # 68세
        '070005',  # 69세
        '075001',  # 70세
        '075002',  # 71세
        '075003',  # 72세
        '075004',  # 73세
        '075005',  # 74세
        '080001',  # 75세
        '080002',  # 76세
        '080003',  # 77세
        '080004',  # 78세
        '080005',  # 79세
        '085001',  # 80세
        '085002',  # 81세
        '085003',  # 82세
        '085004',  # 83세
        '085005',  # 84세
        '086',  # 85세이상
    ]

    if not kosis_admin_div_code:
        error_msg = 'kosis_admin_div_code is missing.'
        raise ValueError(error_msg)
    if year is None:
        error_msg = 'year is missing.'
        raise ValueError(error_msg)

    if not pop_conn:
        pop_conn = db_connect(pop_db)

    jr_kosis_admin_div_codes = get_jr_kosis_admin_div_codes(kosis_admin_div_code, year, pop_conn=pop_conn)
    if not jr_kosis_admin_div_codes:
        return

    kosis_admin_div_codes = [kosis_admin_div_code]
    if kosis_admin_div_code == '00':
        kosis_admin_div_codes += ['03', '04', '05']
    elif kosis_admin_div_code in ['21', '22', '23', '26', '29', '31', '32', '33', '34', '35', '36', '37', '38', '39']:
        kosis_admin_div_codes += [kosis_admin_div_code + suffix for suffix in ['003', '004', '005']]
    kosis_admin_div_codes += jr_kosis_admin_div_codes

    json_items = generate_field_list_target('ITM_ID', items)
    json_ov_lv1 = generate_field_list_target('OV_L1_ID', kosis_admin_div_codes)
    json_ov_lv2 = generate_field_list_target('OV_L2_ID', ov_lv2)

    json_list = [f'{{"targetId":"PRD","targetValue":"","prdValue":"F,{year:0>4},@"}}'] + json_items + json_ov_lv1 + json_ov_lv2
    field_list = '[' + ','.join(json_list) + ']'
    data_table_info = {
        'tblId': year_to_table_id[year],
        'fieldList': field_list,
        'colAxis': 'TIME,ITEM',
        'rowAxis': 'A,B',
    }

    request_data = data_default.copy()
    request_data.update(data_table_info)
    request_data.update(data_down_large)

    return request_data


