from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from urllib import parse, request, response
import json
import time
import shutil, os
from threading import Thread
import functools

chrome_options = Options()
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_experimental_option("useAutomationExtension", False)


def timeout(timeout):
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            res = [Exception('function [%s] timeout [%s seconds] exceeded!' % (func.__name__, timeout))]
            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e
            t = Thread(target=newFunc)
            t.daemon = True
            try:
                t.start()
                t.join(timeout)
            except Exception as je:
                print('error starting thread')
                raise je
            ret = res[0]
            if isinstance(ret, BaseException):
                raise ret
            return ret
        return wrapper
    return deco


def get_province_list():
    url_base = 'http://vdb3.soil.csdb.cn'
    url_suffix = '/front/list-整合数据库$integ_location_name'
    url_params = '?orderParam=&orderType=&searchType=simple&rnum=0&fieldName_0=&comparator_0=&fieldValue_0=&relation_0=and&pageSize=30'

    root_url = parse.quote(url_base + url_suffix + url_params, safe='/:?=$&')

    bs_root = BeautifulSoup(request.urlopen(root_url).read(), 'html.parser')

    province_list = []
    province_table = bs_root.find_all('table')
    for link in province_table[0].find_all('a'):
        province_list.append(parse.quote(url_base + link.get('href'), safe='/:?=$&'))

    return province_list


def get_county_list(province_url):

    if not os.path.exists('./temp/'):
        os.makedirs('./temp/')

    url_soil_dict = {}

    time_s_province = time.time()

    province_dict = {}
    url_base = 'http://vdb3.soil.csdb.cn'

    browser = webdriver.Chrome('./chromedriver.exe', chrome_options=chrome_options)
    browser.get(province_url)
    time.sleep(3.5)
    bs_province = BeautifulSoup(browser.page_source, 'html.parser')

    info = bs_province.find(class_='datatypetable')
    rows = info.find_all(class_='row')
    province_dict['省份代码'] = rows[1].find(class_='datacol').getText().strip()
    province_dict['省份名称'] = rows[2].find(class_='datacol').getText().strip()
    province_dict['省份缩写'] = rows[3].find(class_='datacol').getText().strip()
    province_dict['县市列表'] = {}

    print('正在获取 {:s} (省) 的土壤数据...'.format(province_dict['省份名称']))
    county_tables = info.find('table').find_all('tr')[1:]

    while len(county_tables) > 0:
        for county_table in county_tables:
            cells = county_table.find_all('td')
            county = {}
            county['县市名称'] = cells[0].getText().strip()
            county['经度'] = cells[1].getText().strip()
            county['纬度'] = cells[2].getText().strip()

            temp_file_path = 'temp/{:s}_{:s}.json'.format(province_dict['省份名称'], county['县市名称'])
            county_url = parse.quote(url_base + cells[3].find('a').get('href'), safe='/:?=$&')

            print('\t正在获取 {:s} (市/县) 的土壤数据...'.format(county['县市名称']))
            time_s_county = time.time()

            if os.path.exists(temp_file_path):
                with open(temp_file_path, 'r', encoding='utf-8') as fr:
                    county = json.load(fr)
            else:
                county['土壤列表'] = get_soil_list(county_url, url_soil_dict)
                with open(temp_file_path, 'w', encoding='utf-8') as fp:
                    json.dump(county, fp, ensure_ascii=False, indent=4)

            province_dict['县市列表'][county['县市名称']] = county

            print('\t耗时{:.2f}分钟'.format((time.time() - time_s_county) / 60.0))

        nextpage_key = browser.find_element_by_xpath("//a[contains(text(),'下一页')]")
        nextpage_key.send_keys('\n')
        time.sleep(3.5)
        bs_province = BeautifulSoup(browser.page_source, 'html.parser')

        info = bs_province.find(class_='datatypetable')
        county_tables = info.find('table').find_all('tr')[1:]

    print('{:s}(省)共耗时{:.2f}分钟'.format(province_dict['省份名称'], (time.time() - time_s_province) / 60.0))

    return province_dict


def get_soil_list(county_url, url_soil_dict):
    soil_list = []
    url_base = 'http://vdb3.soil.csdb.cn'

    browser = webdriver.Chrome('./chromedriver.exe', chrome_options=chrome_options)
    browser.get(county_url)
    time.sleep(3.5)
    bs_county = BeautifulSoup(browser.page_source, 'html.parser')

    info = bs_county.find(class_='datatypetable')
    soil_tables = info.find('table').find_all('tr')[1:]
    while len(soil_tables) > 0:
        for soil_table in soil_tables:
            cells = soil_table.find_all('td')
            soil_url = parse.quote(url_base + cells[4].find('a').get('href'), safe='/:?=$&')

            time_out = 0
            success = False
            while not success and time_out < 5:
                try:
                    print('\t\t正在从 {:s} 获取土壤数据...'.format(soil_url))
                    if soil_url in url_soil_dict.keys():
                        soil_details = url_soil_dict[soil_url]
                    else:
                        soil_details = get_soil_details(soil_url)
                    soil_list.append(soil_details)
                    url_soil_dict[soil_url] = soil_details
                    success = True
                except:
                    time_out += 1
                    if time_out == 5:
                        print("\t\t无法从{:s}获取数据!".format(soil_url))
                        soil_list.append({})

        nextpage_key = browser.find_element_by_xpath("//a[contains(text(),'下一页')]")
        nextpage_key.send_keys('\n')
        time.sleep(3.5)
        bs_county = BeautifulSoup(browser.page_source, 'html.parser')

        info = bs_county.find(class_='datatypetable')
        soil_tables = info.find('table').find_all('tr')[1:]
    return soil_list


@timeout(240)
def get_soil_details(soil_url):
    soil_dict = {}
    url_base = 'http://vdb3.soil.csdb.cn'
    browser = webdriver.Chrome('./chromedriver.exe', chrome_options=chrome_options)
    browser.get(soil_url)
    time.sleep(3.5)

    bs_soil = BeautifulSoup(browser.page_source, 'html.parser')
    soil_rows = bs_soil.find(class_='datatypetable').find_all('div', class_='row')[1:]

    # 土壤信息
    for row in soil_rows[:-5]:
        title = row.find('div', class_='titlecol').getText().strip()
        data = row.find('div', class_='datacol').getText().strip()
        soil_dict[title] = data

    # 剖面景观
    prof_landspace_list = []
    prof_landspace_rows = soil_rows[-5].find('table').find_all('tr')[1:]
    while len(prof_landspace_rows) > 0:
        for row in prof_landspace_rows:
            prof_landspace_url = row.find('a').get('href')
            prof_landspace_url = parse.quote(url_base + prof_landspace_url, safe='/:?=$&')
            prof_landspace_list.append(get_prof_landspace_detail(prof_landspace_url))
        nextpage_key = browser.find_element_by_xpath("//a[@id='整合数据库integ_soil_proflandspacenext']")
        nextpage_key.send_keys('\n')
        time.sleep(3.5)
        bs_soil = BeautifulSoup(browser.page_source, 'html.parser')
        soil_rows = bs_soil.find(class_='datatypetable').find_all('div', class_='row')[1:]
        prof_landspace_rows = soil_rows[-5].find('table').find_all('tr')[1:]
    soil_dict['剖面景观'] = prof_landspace_list

    # 剖面发生层, 物理性质, 化学性质, 养分
    ind_dict = {-1: '整合数据库integ_soil_profnutrnext',
                -2: '整合数据库integ_soil_profchemnext',
                -3: '整合数据库integ_soil_profphysnext',
                -4: '整合数据库integ_profile_horizonnext'}

    prof_url_dict = {}

    for ind in ind_dict.keys():
        prof_url_dict[ind] = []
        soil_row = soil_rows[ind]
        prof_rows = soil_row.find(class_='datacol').find_all('tr')[1:]
        while len(prof_rows) > 0:
            for prof_row in prof_rows:
                prof_url = prof_row.find('a').get('href')
                prof_url_dict[ind].append(prof_url)

            nextpage_key = browser.find_element_by_xpath("//a[@id='{:s}']".format(ind_dict[ind]))
            nextpage_key.send_keys('\n')
            time.sleep(3.5)

            bs_soil = BeautifulSoup(browser.page_source, 'html.parser')
            soil_rows = bs_soil.find(class_='datatypetable').find_all('div', class_='row')[1:]
            prof_rows = soil_rows[ind].find(class_='datacol').find_all('tr')[1:]

    soil_dict['典型剖面数据'] = get_prof_detail(prof_url_dict)

    return soil_dict


def get_prof_landspace_detail(prof_landspace_url):
    prof_landspace_detail_dict = {}
    prof_landspace_sp = BeautifulSoup(request.urlopen(prof_landspace_url).read(), "html.parser")
    table = prof_landspace_sp.find(class_='datatypetable')
    rows = table.find_all(class_='row')[1:]
    for row in rows:
        title = row.find(class_='titlecol').getText().strip()
        data = row.find(class_='datacol').getText().strip()
        prof_landspace_detail_dict[title] = data

    return prof_landspace_detail_dict


def get_prof_detail(prof_url_dict):
    url_base = 'http://vdb3.soil.csdb.cn'
    prof_detail_dict = {}
    for ind in [-1, -2, -3, -4]:
        for prof_url in prof_url_dict[ind]:
            single_dict = {}
            url = parse.quote(url_base + prof_url, safe='/:?=$&')
            bs = BeautifulSoup(request.urlopen(url).read(), 'html.parser')
            table = bs.find(class_='datatypetable')
            rows = table.find_all(class_='row')[1:]
            for row in rows:
                title = row.find(class_='titlecol').getText().strip()
                data = row.find(class_='datacol').getText().strip()
                if title not in single_dict.keys():
                    single_dict[title] = data

            if '发生层名称' in single_dict.keys():
                prof_id = single_dict['发生层名称']
            else:
                prof_id = single_dict['发生层次名称']


            if prof_id not in prof_detail_dict:
                prof_detail_dict[prof_id] = dict()

            prof_detail_dict[prof_id] = {**prof_detail_dict[prof_id], **single_dict}

    return prof_detail_dict


if __name__ == '__main__':
    total_dict = {}
    province_list = get_province_list()
    province_list = province_list[15:20]
    for province_url in province_list:
        province_dict = get_county_list(province_url)
        with open('data_raw/{:s}.json'.format(province_dict['省份名称']), 'w', encoding='utf-8') as fp:
            json.dump(province_dict, fp, ensure_ascii=False, indent=4)

    shutil.rmtree('./temp/')

#province_list = get_province_list()
#get_county_list(province_list[0])
#get_soil_list('http://vdb3.soil.csdb.cn/front/detail-%E6%95%B4%E5%90%88%E6%95%B0%E6%8D%AE%E5%BA%93$integ_sublocation?id=2')
#result = get_soil_details("http://vdb3.soil.csdb.cn/front/detail-%E6%95%B4%E5%90%88%E6%95%B0%E6%8D%AE%E5%BA%93$integ_cou_soiltype?id=40037")
#print(result)
