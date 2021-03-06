import xlsxwriter
import json
import os
import shutil
import re

def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as fr:
        data = json.load(fr)
    return data


def parse_province(root_dict, output_dir):

    province_name = root_dict['省份名称']
    output_dir = output_dir + '{:s}/'.format(province_name)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)

    # Create total workbook
    col_names = ["省", "县/市", "土种名称", "土类名称", "亚类名称",
                 "土地利用", "描述", "生产性能"]

    workbook_total = xlsxwriter.Workbook(output_dir + province_name + '.xlsx')
    worksheet_total = workbook_total.add_worksheet('土种类型')

    format_total_left = workbook_total.add_format()
    format_total_left.set_text_wrap()
    format_total_left.set_align('left')
    format_total_left.set_align('vcenter')

    format_total_center = workbook_total.add_format()
    format_total_center.set_text_wrap()
    format_total_center.set_align('center')
    format_total_center.set_align('vcenter')

    format_total_hyper = workbook_total.add_format()
    format_total_hyper.set_text_wrap()
    format_total_hyper.set_align('center')
    format_total_hyper.set_align('vcenter')
    format_total_hyper.set_font_color('#0000FF')
    format_total_hyper.set_underline()
    
    format_dict = {'left': format_total_left, 'center': format_total_center, 'hyper': format_total_hyper}

    for i in range(len(col_names)):
        worksheet_total.write(0, i, col_names[i], format_total_center)

    # Parse County
    total_row_count = 1
    for county_name, county_dict in root_dict['县市列表'].items():
        total_row_count = parse_county(
            county_dict, province_name, county_name, output_dir,
            worksheet_total, format_dict, total_row_count
        )


def parse_county(county_dict, province_name, county_name, output_dir, worksheet_total,
                 format_dict, total_row_count):
    output_dir = output_dir + '{:s}/'.format(county_name)

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)

    for soil_dict in county_dict['土壤列表']:
        parse_soil(
            soil_dict, province_name, county_name, output_dir, worksheet_total,
            format_dict, total_row_count
        )
        total_row_count += 1

    return total_row_count


def parse_soil(soil_dict, province_name, county_name, output_dir, worksheet_total,
               format_dict, total_row_count):
    # get soil name
    soil_name = soil_dict['土种名称']
    soil_name = soil_name.replace('\n', '')
    soil_filename = "{:s}_{:s}_{:s}.xlsx".format(province_name, county_name, soil_name)

    # Define row_names for single soil
    # Define col_names for all soils
    row_names = ["土种名称", "土类名称", "亚类名称", "链接",
                 "俗名", "描述", "分布和地形地貌",
                 "面积（公顷）", "面积（万亩）", "母质",
                 "剖面构型", "有效土体深度", "主要性状",
                 "生产性能", "土壤障碍因子", "土地利用"]

    col_names = ["省", "县/市", "土种名称", "土类名称", "亚类名称",
                 "土地利用", "描述", "生产性能"]

    col_width = [5, 8, 10, 10, 10, 10, 85, 60]

    col_dict = {}

    # Total worksheet
    col_dict["省"] = province_name
    col_dict["县/市"] = county_name
    col_dict["土种名称"] = soil_name
    for col_name in col_names:
        if col_name not in col_dict.keys():
            col_dict[col_name] = reformat_value(soil_dict[col_name])
    col_dict["描述"] = '\n' + re.sub(' {2,}', '\n', col_dict["描述"]) + '\n'
    col_dict["生产性能"] = '\n' + col_dict["生产性能"] + '\n'

    for i in range(len(col_names)):
        worksheet_total.set_column(total_row_count, i, col_width[i])
        if col_names[i] == "土种名称":
            worksheet_total.write_url(
                total_row_count, i, '{:s}/{:s}'.format(county_name, soil_filename),
                string=soil_name, cell_format=format_dict['hyper']
            )
        elif col_names[i] == "描述" or col_names[i] == "生产性能":
            worksheet_total.write(total_row_count, i, col_dict[col_names[i]], format_dict['left'])
        else:
            worksheet_total.write(total_row_count, i, col_dict[col_names[i]], format_dict['center'])
    
    # Seperate worksheet
    spreadsheet_name = output_dir + soil_filename
    print("正在创建{:s}".format(spreadsheet_name))
    workbook = xlsxwriter.Workbook(spreadsheet_name)
    format = workbook.add_format()
    format.set_text_wrap()
    format.set_align('center')
    format.set_align('vcenter')

    format_hyper = workbook.add_format()
    format_hyper.set_text_wrap()
    format_hyper.set_align('center')
    format_hyper.set_align('vcenter')
    format_hyper.set_underline()
    format_hyper.set_font_color('blue')

    print('\t处理土壤数据...')
    worksheet = workbook.add_worksheet('土种类型')
    worksheet.set_column(0, 0, 20)
    worksheet.set_column(1, 1, 100)

    row_count = 0
    for row_name in row_names:
        if row_name == "链接":
            try:
                soil_id = soil_dict["剖面景观"][0]["土种ID"].replace(",", "")
                content = "http://vdb3.soil.csdb.cn/front/detail-%E6%95%B4%E5%90%88%E6%95%B0%E6%8D%AE%E5%BA%93$integ_cou_soiltype?id={}".format(
                    soil_id
                )
                worksheet.write_url(row_count, 1, content, string=soil_id, cell_format=format_hyper)
            except:
                pass
        else:
            if row_name not in soil_dict.keys():
                print("{:s} {:s} {:s} 缺少项目 {:s}".format(province_name, county_name,
                                                            soil_name, row_name))
                content = ''
            else:
                content = reformat_value(soil_dict[row_name])

            worksheet.write(row_count, 1, content, format)

        worksheet.write(row_count, 0, row_name, format)

        row_count += 1

    print("\t处理剖面景观数据...")
    parse_prof_landspace(soil_dict["剖面景观"], worksheet, row_count, format)

    print("\t处理剖面数据...")
    worksheet_prof = workbook.add_worksheet("典型剖面数据")
    parse_prof(soil_dict["典型剖面数据"], soil_name, worksheet_prof, format)

    workbook.close()


def parse_prof(prof_dict, soil_name, worksheet_prof, format):

    worksheet_prof.set_column(0, 0, 25)
    worksheet_prof.set_column(1, len(prof_dict), 20)
    worksheet_prof.write(0, 0, '土种名称', format)
    worksheet_prof.merge_range(0, 1, 0, len(prof_dict), soil_name, format)

    row_names = ["发生层名称", "发生层序号", "发生层厚度(cm)", "发生层最上深度(cm)",
                 "发生层最下深度(cm)", "发生层颜色", "发生层质地", "发生层结构", "发生层松紧度",
                 "发生层根系和其他", "发生层次名称", "发生层序号", "层次相对厚度(cm)", "层最上深度",
                 "层最下深度", "颗粒组成大于2mm石砾", "颗粒组成2-0.02mm", "颗粒组成2-0.2mm",
                 "颗粒组成0.02-0.002mm", "颗粒组成0.2-0.02mm", "颗粒组成小于0.002mm", "质地",
                 "交换性氢(cmol/kg(+))", "交换性铝(cmol/kg(+))", "交换性酸(cmol/kg(+))",
                 "交换性钙(cmol/kg(+))", "交换性镁(cmol/kg(+))", "交换性钾(cmol/kg(+))",
                 "交换性纳(cmol/kg(+))", "交换性盐基总量(cmol/kg(+))", "阳离子交换量(cmol/kg(+))",
                 "碳酸钙(g/kg)", "有机质(g/kg)", "全氮(g/kg)", "全磷(g/kg)", "全钾(g/kg)", "水提pH值"]

    # sort prof_dict keys on "发生层序号"
    prof_dict_keys_sorted = list(sorted(prof_dict.keys(), key=lambda x: prof_dict[x]["发生层序号"]))

    row_count = 1
    for row_name in row_names:
        worksheet_prof.write(row_count, 0, row_name, format)
        row_count += 1

    col_count = 1
    for prof_single_id in prof_dict_keys_sorted:
        prof_single_dict = prof_dict[prof_single_id]
        row_count = 1
        prof_single_dict_new = dict()
        for title, value in prof_single_dict.items():
            title = title.replace('（', '(')
            title = title.replace('）', ')')
            title = title.replace(' ', '')
            title = title.replace('g.kg-1', 'g/kg')
            prof_single_dict_new[title] = value

        for row_name in row_names:
            if row_name not in prof_single_dict_new.keys():
                content = ''
            else:
                content = reformat_value(prof_single_dict_new[row_name])

            worksheet_prof.write(row_count, col_count, content, format)

            row_count += 1

        col_count += 1


def parse_prof_landspace(prof_landspace_list, worksheet, start_row, format):
    row_names = ["典型剖面近似经度", "典型剖面近似纬度", "典型剖面地形地貌和部位",
                 "典型剖面高程", "典型剖面母质", "典型剖面地点年均温（℃）",
                 ">10积温", "土地利用", "自然植被", "无霜期(天)", "典型剖面年降水"]

    row_count = 0
    for prof_landspace_single_dict in prof_landspace_list:
        for row_name in row_names:
            if row_name not in prof_landspace_single_dict.keys():
                print("缺少项目 {:s}".format(row_name))
                content = ''
            else:
                content = reformat_value(prof_landspace_single_dict[row_name])

            worksheet.write(row_count + start_row, 0, row_name, format)
            worksheet.write(row_count + start_row, 1, content, format)
            row_count += 1


def reformat_value(ustring):
    ustring = ustring.replace('〔', '(')
    ustring = re.sub(r'(\d+)一(\d+)', r'\1-\2', ustring)
    ustring = re.sub(r'(\d+)—(\d+)', r'\1-\2', ustring)
    ustring = ustring.replace('\n', '')
    rstring = ''
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
        if not (0x0021 <= inside_code and inside_code <= 0x7e):
            rstring += uchar
            continue
        rstring += chr(inside_code)

    return rstring

if __name__ == '__main__':
    import glob

    output_dir = 'data_spreadsheet/'
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    prov_names = glob.glob('data_raw/*.json')
    for prov_name in prov_names:
        parse_province(load_json(prov_name), output_dir)