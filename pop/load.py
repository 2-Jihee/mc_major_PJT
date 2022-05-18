from pop.model import *
from data.kosis import convert_admin_div_code_to_admin_div_num
from db.connector import *
from db.query import *

last_age = 100


def get_mois_pyramid(admin_div_code: str, year: int, month=12, resident_type='R', pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    age_data = {}
    for sex_type in ['-', 'M', 'F']:
        sex_key = convert_sex_type_to_key(sex_type)
        pyramid_data = select_one_row_pack_into_dict(pop_conn, 'mois_population', {'resident_type': resident_type, 'admin_div_num': admin_div_num, 'year': year, 'month': month, 'sex_type': sex_type}, [])
        if not pyramid_data:
            continue
        for age in range(last_age + 1):
            if age not in age_data.keys():
                age_data[age] = {}

            if age != last_age:
                age_data[age][sex_key] = pyramid_data[f'age_{age}']
            else:
                age_data[age][sex_key] = pyramid_data[f'age_{age}+']

    if age_data:
        population_pyramid = Pyramid(last_age=last_age, age_data=age_data)
    else:
        population_pyramid = None

    return population_pyramid


def get_kosis_birth(admin_div_code: str, year: int, mother_age_type='----', birth_order_type='--', pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    birth_data = select_one_row_pack_into_dict(pop_conn, 'kosis_birth',
                                               {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'mother_age_type': mother_age_type, 'birth_order_type': birth_order_type},
                                               ['total', 'male', 'female'])

    return birth_data


def get_kosis_death_pyramid(admin_div_code: str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    death_age_data = {}
    for age in range(last_age + 1):
        if age == last_age:
            age_type = f'+{age:0>3}'
        else:
            age_type = f'={age:0>3}'
        layer_data = select_one_row_pack_into_dict(pop_conn, 'kosis_death',
                                                   {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'age_type': age_type},
                                                   ['total', 'male', 'female'])
        death_age_data[age] = layer_data

    if death_age_data:
        death_pyramid = Pyramid(last_age=last_age, age_data=death_age_data)
    else:
        death_pyramid = None

    return death_pyramid


def get_kosis_outflow_pyramid(admin_div_code: str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    outflow_age_data = {}
    for age in range(last_age + 1):
        if age == last_age:
            age_type = f'+{age:0>3}'
        else:
            age_type = f'={age:0>3}'

        sex_type = '-'
        sex_key = convert_sex_type_to_key(sex_type)
        inter_level_1_outflow = select_one_row_one_column(pop_conn, 'kosis_population_move',
                                                          {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'sex_type': sex_type, 'age_type': age_type}, 'inter_level_1_outflow')
        outflow_age_data[age] = {sex_key: inter_level_1_outflow}

    outflow_stack_data = {}
    for age_type in ['0004', '0509', '1014', '1519', '2024', '2529', '3034', '3539', '4044', '4549', '5054', '5559', '6064', '6569', '7074', '7579', '+080']:
        for sex_type in ['-', 'M', 'F']:
            keys = {'admin_div_num': admin_div_num, 'year': year, 'sex_type': sex_type, 'age_type': age_type}
            where_str = dict_to_where(keys)
            query = f"SELECT `inter_level_1_outflow` " \
                    f"FROM `kosis_population_move` " \
                    f"WHERE {where_str} and `month` in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)"
            cur = pop_conn.cursor()
            db_execute(cur, query)
            rows = cur.fetchall()
            if len(rows) != 12:
                error_msg = f"Number of months is not 12: {len(rows)} rows for '{dict_to_set(keys)}'."
                raise ValueError(error_msg)
            inter_level_1_outflow = sum([row[0] for row in rows])

            stack_key = convert_age_type_to_stack_key(age_type)
            sex_key = convert_sex_type_to_key(sex_type)
            if stack_key not in outflow_stack_data.keys():
                outflow_stack_data[stack_key] = {}
            outflow_stack_data[stack_key][sex_key] = inter_level_1_outflow

    if outflow_age_data or outflow_stack_data:
        outflow_pyramid = Pyramid(last_age=last_age, age_data=outflow_age_data, stack_data=outflow_stack_data)
        outflow_pyramid.fill_age_layers()
    else:
        outflow_pyramid = None

    return outflow_pyramid


def get_kosis_outflow_matrix(admin_div_code: str, into_admin_div_code: str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    into_admin_div_num = convert_admin_div_code_to_admin_div_num(into_admin_div_code)

    outflow_stack_data = {}
    for age_type in ['0004', '0509', '1014', '1519', '2024', '2529', '3034', '3539', '4044', '4549', '5054', '5559', '6064', '6569', '7074', '7579', '+080']:
        for sex_type in ['-', 'M', 'F']:
            keys = {'admin_div_num': admin_div_num, 'into_admin_div_num': into_admin_div_num, 'year': year, 'sex_type': sex_type, 'age_type': age_type}
            where_str = dict_to_where(keys)
            query = f"SELECT `outflow` " \
                    f"FROM `kosis_population_move_matrix` " \
                    f"WHERE {where_str} and `month` in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)"
            cur = pop_conn.cursor()
            db_execute(cur, query)
            rows = cur.fetchall()
            if len(rows) != 12:
                error_msg = f"Number of months is not 12: {len(rows)} rows for '{dict_to_set(keys)}'."
                raise ValueError(error_msg)
            outflow = sum([row[0] for row in rows])

            stack_key = convert_age_type_to_stack_key(age_type)
            sex_key = convert_sex_type_to_key(sex_type)
            if stack_key not in outflow_stack_data.keys():
                outflow_stack_data[stack_key] = {}
            outflow_stack_data[stack_key][sex_key] = outflow

    outflow_age_data = {}
    for age in range(last_age + 1):
        if age == last_age:
            age_type = f'+{age:0>3}'
        else:
            age_type = f'={age:0>3}'

        sex_type = '-'
        sex_key = convert_sex_type_to_key(sex_type)
        outflow = select_one_row_one_column(pop_conn, 'kosis_population_move',
                                            {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'sex_type': sex_type, 'age_type': age_type}, 'inter_level_1_outflow')
        outflow_age_data[age] = {sex_key: outflow}

    combined_age_data = {}
    for stack_key, pop_data in outflow_stack_data.items():
        min_age = int(stack_key[0:2])
        if stack_key[2:].isnumeric():
            max_age = int(stack_key[2:])
        elif stack_key[2] == '+':
            max_age = last_age
        else:
            error_msg = f"Unknown stack_key '{stack_key}'."
            raise ValueError(error_msg)
        total_sum = 0
        for age in range(min_age, max_age + 1):
            total_sum += outflow_age_data[age]['total']

        for age in range(min_age, max_age + 1):
            age_ratio = outflow_age_data[age]['total'] / total_sum
            combined_age_data[age] = {'male': age_ratio * pop_data['male'], 'female': age_ratio * pop_data['female']}

    if combined_age_data:
        outflow_pyramid = Pyramid(last_age=last_age, age_data=combined_age_data)
    else:
        outflow_pyramid = None

    return outflow_pyramid


def get_kosis_inflow_pyramid(admin_div_code: str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    inflow_age_data = {}
    for age in range(last_age + 1):
        if age == last_age:
            age_type = f'+{age:0>3}'
        else:
            age_type = f'={age:0>3}'

        sex_type = '-'
        sex_key = convert_sex_type_to_key(sex_type)
        inter_level_1_inflow = select_one_row_one_column(pop_conn, 'kosis_population_move', {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'sex_type': sex_type, 'age_type': age_type}, 'inter_level_1_inflow')
        inflow_age_data[age] = {sex_key: inter_level_1_inflow}

    inflow_stack_data = {}
    for age_type in ['0004', '0509', '1014', '1519', '2024', '2529', '3034', '3539', '4044', '4549', '5054', '5559', '6064', '6569', '7074', '7579', '+080']:
        for sex_type in ['-', 'M', 'F']:
            keys = {'admin_div_num': admin_div_num, 'year': year, 'sex_type': sex_type, 'age_type': age_type}
            where_str = dict_to_where(keys)
            query = f"SELECT `inter_level_1_inflow` " \
                    f"FROM `kosis_population_move` " \
                    f"WHERE {where_str} and `month` in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)"
            cur = pop_conn.cursor()
            db_execute(cur, query)
            rows = cur.fetchall()
            if len(rows) != 12:
                error_msg = f"Number of months is not 12: {len(rows)} rows for '{dict_to_set(keys)}'."
                raise ValueError(error_msg)
            inter_level_1_inflow = sum([row[0] for row in rows])

            stack_key = convert_age_type_to_stack_key(age_type)
            sex_key = convert_sex_type_to_key(sex_type)
            if stack_key not in inflow_stack_data.keys():
                inflow_stack_data[stack_key] = {}
            inflow_stack_data[stack_key][sex_key] = inter_level_1_inflow

    if inflow_age_data or inflow_stack_data:
        inflow_pyramid = Pyramid(last_age=last_age, age_data=inflow_age_data, stack_data=inflow_stack_data)
        inflow_pyramid.fill_age_layers()
    else:
        inflow_pyramid = None

    return inflow_pyramid
