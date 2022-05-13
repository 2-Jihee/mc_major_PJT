import re
from datetime import date, datetime


def list_to_select(column_list: list):
    if not column_list:
        return None

    separator = '`, `'
    select_str = '`' + separator.join(column_list) + '`'

    return select_str


def list_to_values(python_values: list):
    sql_values = list(map(python_data_to_sql_value, python_values))
    separator = ', '
    values_str = separator.join(sql_values)

    return values_str


def dict_to_where(column_dict: dict):
    where_str = ''
    for key, value in column_dict.items():
        if where_str:
            where_str += ' and '
        where_str += f"`{key}`={python_data_to_sql_value(value)}"

    return where_str


def dict_to_set(column_dict: dict, python_data_no=0):
    func_name = 'dict_to_set'

    if not column_dict:
        return None

    if python_data_no > len(column_dict):
        print(f">>> {func_name} -  Warning: No. of python_data ({python_data_no}) is greater than no. of set columns ({len(column_dict)}).")

    set_str = ''
    for i, (key, value) in enumerate(column_dict.items()):
        if set_str:
            set_str += ', '

        if (i + 1) <= python_data_no:
            set_str += f"`{key}`=?"
        else:
            set_str += f"`{key}`={python_data_to_sql_value(value)}"

    return set_str


def python_data_to_sql_value(python_data):
    func_name = 'python_data_to_sql_value'
    sql_function_pattern = r'\$[A-Z0-9_]+\(.*\)'

    if python_data is None:
        sql_value = "NULL"
    elif isinstance(python_data, datetime):
        sql_value = f"'{python_data.strftime('%Y-%m-%d %H:%M:%S')}'"
    elif isinstance(python_data, date):
        sql_value = f"'{python_data.strftime('%Y-%m-%d')}'"
    elif isinstance(python_data, int) or isinstance(python_data, float):
        sql_value = f"{python_data}"
    elif isinstance(python_data, str):
        if re.fullmatch(sql_function_pattern, python_data):
            # SQL function in capital letters
            sql_value = f"{python_data[1:]}"
        else:
            sql_value = f"'{python_data}'"
    else:
        sql_value = None
        print(f">>> {func_name} -  Unknown data type:{type(python_data)}, {python_data}.")

    return sql_value
