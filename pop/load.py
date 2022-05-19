from pop.model import *
from data.kosis import convert_admin_div_code_to_admin_div_num
from db.connector import *
from db.query import *

LAST_AGE = 100


def convert_sex_type_to_key(sex_type: str):
    if sex_type == '-':
        sex_key = 'total'
    elif sex_type == 'M':
        sex_key = 'male'
    elif sex_type == 'F':
        sex_key = 'female'
    else:
        error_msg = f"sex_type '{sex_type}' is invalid."
        raise ValueError(error_msg)

    return sex_key


def convert_age_type_to_stack_key(age_type: str):
    if age_type.isnumeric() and age_type[0] == '0':
        stack_key = age_type[1:]
    elif age_type[:-2].isnumeric() and age_type[-2:] == '=+':
        age = int(age_type[:-2])
        stack_key = f'{age:0>2}+'
    else:
        error_msg = f"Unavailable age_type '{age_type}'."
        raise ValueError(error_msg)

    return stack_key


def convert_age_to_age_type(age: int, last_age:int):
    if age == last_age:
        age_type = f'{age:0>3}=+'
    else:
        age_type = f'{age:0>3}=='

    return age_type


# # ---------- load pyramid ------------------------------------------------------------------------------------------------------------------------

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
        for age in range(LAST_AGE + 1):
            if age not in age_data.keys():
                age_data[age] = {}

            if age == LAST_AGE:
                age_data[age][sex_key] = pyramid_data[f'age_{age}+']
            else:
                age_data[age][sex_key] = pyramid_data[f'age_{age}']

    if age_data:
        population_pyramid = Pyramid(last_age=LAST_AGE, age_data=age_data)
    else:
        population_pyramid = None

    return population_pyramid


def get_kosis_move_pyramid(from_admin_div_code: str, to_admin_div_code:str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    from_admin_div_num = convert_admin_div_code_to_admin_div_num(from_admin_div_code)
    to_admin_div_num = convert_admin_div_code_to_admin_div_num(to_admin_div_code)
    age_data = {}
    for age in range(LAST_AGE + 1):
        age_type = convert_age_to_age_type(age, LAST_AGE)
        layer_data = select_one_row_pack_into_dict(pop_conn, 'kosis_population_move_matrix_calc',
                                                   {'from_admin_div_num': from_admin_div_num, 'to_admin_div_num': to_admin_div_num, 'year': year, 'age_type': age_type}, ['total_move', 'male_move', 'female_move'])
        if not (layer_data['male_move'] is None or layer_data['female_move'] is None):
            age_data[age] = {'male': layer_data['male_move'], 'female': layer_data['female_move']}
        else:
            age_data[age] = {'total': layer_data['total_move']}

    if age_data:
        move_pyramid = Pyramid(last_age=LAST_AGE, age_data=age_data)
    else:
        move_pyramid = None

    return move_pyramid


def get_kosis_death_pyramid(admin_div_code: str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    death_age_data = {}
    for age in range(LAST_AGE + 1):
        age_type = convert_age_to_age_type(age, LAST_AGE)
        layer_data = select_one_row_pack_into_dict(pop_conn, 'kosis_death', {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'age_type': age_type}, ['total', 'male', 'female'])
        death_age_data[age] = layer_data

    if death_age_data:
        death_pyramid = Pyramid(last_age=LAST_AGE, age_data=death_age_data)
    else:
        death_pyramid = None

    return death_pyramid


def get_kosis_birth(admin_div_code: str, year: int, mother_age_type='-----', birth_order_type='--', pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    birth_data = select_one_row_pack_into_dict(pop_conn, 'kosis_birth',
                                               {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'mother_age_type': mother_age_type, 'birth_order_type': birth_order_type}, ['total', 'male', 'female'])

    return birth_data


def get_kosis_birth_mother_pyramid(admin_div_code: str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    mother_stack_data = {}
    for age_type in ['00014', '01519', '02024', '02529', '03034', '03539', '04044', '04549', '050=+']:
        layer_data = select_one_row_pack_into_dict(pop_conn, 'kosis_birth', {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'mother_age_type': age_type, 'birth_order_type': '--'}, ['male', 'female'])
        stack_key = convert_age_type_to_stack_key(age_type)
        mother_stack_data[stack_key] = layer_data

    mother_age_data = {}
    for age in range(15, 50):
        age_type = convert_age_to_age_type(age, last_age=50)
        layer_data = select_one_row_pack_into_dict(pop_conn, 'kosis_birth', {'admin_div_num': 0, 'year': year, 'month': 0, 'mother_age_type': age_type, 'birth_order_type': '--'}, ['total', 'male', 'female'])
        mother_age_data[age] = layer_data

    combined_age_data = {}
    for stack_key, birth_data in mother_stack_data.items():
        min_age = int(stack_key[:2])
        if min_age < 15 or min_age > 49:
            continue
        max_age = int(stack_key[2:])

        stack_total_sum = 0
        for age in range(min_age, max_age + 1):
            stack_total_sum += mother_age_data[age]['total']

        for age in range(min_age, max_age + 1):
            age_ratio = mother_age_data[age]['total'] / stack_total_sum
            combined_age_data[age] = {'male': age_ratio * birth_data['male'], 'female': age_ratio * birth_data['female']}

    if mother_stack_data:
        birth_mother_pyramid = Pyramid(first_stack_height=15, mid_stack_height=5, num_stacks=9, last_age=LAST_AGE, age_data=combined_age_data, stack_data=mother_stack_data)
    else:
        birth_mother_pyramid = None

    return birth_mother_pyramid


def get_kosis_outflow_pyramid(admin_div_code: str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    outflow_age_data = {}
    for age in range(LAST_AGE + 1):
        age_type = convert_age_to_age_type(age, LAST_AGE)
        sex_type = '-'
        sex_key = convert_sex_type_to_key(sex_type)
        inter_level_1_outflow = select_one_row_one_column(pop_conn, 'kosis_population_move',
                                                          {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'sex_type': sex_type, 'age_type': age_type}, 'inter_level_1_outflow')
        outflow_age_data[age] = {sex_key: inter_level_1_outflow}

    outflow_stack_data = {}
    for age_type in ['00004', '00509', '01014', '01519', '02024', '02529', '03034', '03539', '04044', '04549', '05054', '05559', '06064', '06569', '07074', '07579', '080=+']:
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
        outflow_pyramid = Pyramid(last_age=LAST_AGE, age_data=outflow_age_data, stack_data=outflow_stack_data)
        outflow_pyramid.fill_age_layers()
    else:
        outflow_pyramid = None

    return outflow_pyramid


def get_kosis_outflow_matrix(from_admin_div_code: str, to_admin_div_code: str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    from_admin_div_num = convert_admin_div_code_to_admin_div_num(from_admin_div_code)
    to_admin_div_num = convert_admin_div_code_to_admin_div_num(to_admin_div_code)

    outflow_stack_data = {}
    for age_type in ['00004', '00509', '01014', '01519', '02024', '02529', '03034', '03539', '04044', '04549', '05054', '05559', '06064', '06569', '07074', '07579', '080=+']:
        for sex_type in ['-', 'M', 'F']:
            keys = {'from_admin_div_num': from_admin_div_num, 'to_admin_div_num': to_admin_div_num, 'year': year, 'sex_type': sex_type, 'age_type': age_type}
            where_str = dict_to_where(keys)
            query = f"SELECT `total_move` " \
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
    for age in range(LAST_AGE + 1):
        age_type = convert_age_to_age_type(age, LAST_AGE)

        sex_type = '-'
        sex_key = convert_sex_type_to_key(sex_type)
        outflow = select_one_row_one_column(pop_conn, 'kosis_population_move',
                                            {'admin_div_num': from_admin_div_num, 'year': year, 'month': 0, 'sex_type': sex_type, 'age_type': age_type}, 'inter_level_1_outflow')
        outflow_age_data[age] = {sex_key: outflow}

    combined_age_data = {}
    for stack_key, outflow_data in outflow_stack_data.items():
        min_age = int(stack_key[0:2])
        if stack_key[2:].isnumeric():
            max_age = int(stack_key[2:])
        elif stack_key[2] == '+':
            max_age = LAST_AGE
        else:
            error_msg = f"Unknown stack_key '{stack_key}'."
            raise ValueError(error_msg)

        stack_total_sum = 0
        for age in range(min_age, max_age + 1):
            stack_total_sum += outflow_age_data[age]['total']

        for age in range(min_age, max_age + 1):
            age_ratio = outflow_age_data[age]['total'] / stack_total_sum
            combined_age_data[age] = {'male': age_ratio * outflow_data['male'], 'female': age_ratio * outflow_data['female']}

    if combined_age_data:
        outflow_pyramid = Pyramid(last_age=LAST_AGE, age_data=combined_age_data)
    else:
        outflow_pyramid = None

    return outflow_pyramid


def get_kosis_inflow_pyramid(admin_div_code: str, year: int, pop_conn=None):
    if not pop_conn:
        pop_conn = db_connect(pop_db)

    admin_div_num = convert_admin_div_code_to_admin_div_num(admin_div_code)
    inflow_age_data = {}
    for age in range(LAST_AGE + 1):
        age_type = convert_age_to_age_type(age, LAST_AGE)

        sex_type = '-'
        sex_key = convert_sex_type_to_key(sex_type)
        inter_level_1_inflow = select_one_row_one_column(pop_conn, 'kosis_population_move', {'admin_div_num': admin_div_num, 'year': year, 'month': 0, 'sex_type': sex_type, 'age_type': age_type}, 'inter_level_1_inflow')
        inflow_age_data[age] = {sex_key: inter_level_1_inflow}

    inflow_stack_data = {}
    for age_type in ['00004', '00509', '01014', '01519', '02024', '02529', '03034', '03539', '04044', '04549', '05054', '05559', '06064', '06569', '07074', '07579', '080=+']:
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
        inflow_pyramid = Pyramid(last_age=LAST_AGE, age_data=inflow_age_data, stack_data=inflow_stack_data)
        inflow_pyramid.fill_age_layers()
    else:
        inflow_pyramid = None

    return inflow_pyramid
