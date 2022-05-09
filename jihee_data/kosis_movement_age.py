import os

import requests
import re

from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve
from urllib.error import HTTPError
from urllib.error import URLError

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoAlertPresentException

import time

from datetime import datetime
from dateutil.relativedelta import relativedelta

##### 시도/각세별 이동자수 #####

path = './driver/chromedriver.exe'
driver = webdriver.Chrome(path)
driver.implicitly_wait(3)
driver.get(
    'https://kosis.kr/statHtml/statHtml.do?orgId=101&tblId=DT_1B26B01&lang_mode=ko&vw_cd=MT_ZTITLE&list_id=A34_1&conn_path=I4')
time.sleep(2)

# iframe으로 전환
# driver.switch_to.frame('iframe id 입력')
driver.switch_to.frame('iframe_rightMenu')
driver.switch_to.frame('iframe_centerMenu')

# city_name = ['전국', '서울특별시', '부산광역시', '대구광역시', '인천광역시', '광주광역시', '대전광역시', '울산광역시', '세종특별자치시', '경기도', '강원도', '충청북도', '충청남도', '전라북도', '전라남도', '경상북도', '경상남도', '제주특별자치도']

chkBox_lst1 = driver.find_elements(By.CSS_SELECTOR, '#ft-id-2 > li > span > span.fancytree-checkbox')
chkBox_lst2 = driver.find_elements(By.CSS_SELECTOR, '#ft-id-5 > li > span > span.fancytree-checkbox')

city_check = 0
count = 0

# (시군구 중 시 체크박스 클릭)
for i in chkBox_lst1:
    # 조회 설정 클릭
    driver.find_element(By.XPATH,'''//*[@id="ico_querySetting"]''').click()
    time.sleep(1)

    if city_check == 0:
        # 행정구역(시군구별) 탭 클릭
        driver.find_element(By.XPATH,'''//*[@id="tabClassText_1"]''').click()
        city_check = city_check + 1
        time.sleep(1)

    # 전체 선택
    driver.find_element(By.XPATH,'''//*[@id="treeCheckAll1"]''').click()
    time.sleep(1)
    try:
        WebDriverWait(driver, 3).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        alert.accept()
        time.sleep(1)
    except TimeoutException:
        pass

        # 전체 해제
    driver.find_element(By.XPATH,'''//*[@id="treeCheckAll1"]''').click()
    time.sleep(1)

    i.click()
    time.sleep(1)

    # 조회버튼 클릭
    driver.find_element(By.XPATH,'''//*[@id="searchImg1"]/span/button''').click()
    time.sleep(2)
    try:
        WebDriverWait(driver, 3).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        alert.accept()
    except TimeoutException:
        pass

    # 시점 클릭
    driver.find_element(By.XPATH,'''//*[@id="btnShow"]/div[1]/span[1]/button[1]''').click()
    time.sleep(2)

    # 전체 해제
    driver.find_element(By.XPATH,'''//*[@id="timePopListYBtn"]''').click()
    time.sleep(1)

    for j in chkBox_lst2:
        # 체크박스 클릭
        j.click()
        time.sleep(1)

        # 적용 클릭
        driver.find_element(By.XPATH,'''//*[@id="searchPopBtn"]/button[1]''').click()
        time.sleep(3)

        try:
            WebDriverWait(driver, 3).until(EC.alert_is_present())
            alert = driver.switch_to.alert
            alert.accept()
            flag = True
            break

        except TimeoutException:
            # 다운로드
            driver.find_element(By.XPATH, '''//*[@id="ico_download"]''').click()
            time.sleep(1)
            # csv 선택
            driver.find_element(By.XPATH, '''//*[@id="csvradio"]''').click()
            time.sleep(1)
            #  다운로드 창 > 다운로드
            driver.find_element(By.XPATH, '''//*[@id="pop_downgrid2"]/div[2]/div[3]/span[1]/a''').click()
            time.sleep(2)
            # 닫기
            driver.find_element(By.XPATH, '''//*[@id="pop_downgrid2"]/div[1]/span/a''').click()
            time.sleep(1)
            # 시점
            driver.find_element(By.XPATH, '''//*[@id="btnShow"]/div[1]/span[1]/button[1]''').click()
            time.sleep(2)
            # 전체해제
            driver.find_element(By.XPATH, '''//*[@id="timePopListYBtn"]''').click()
            time.sleep(1)

print('다운로드가 완료되었습니다')

# 브라우저 닫기
driver.quit()