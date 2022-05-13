import mariadb

from db.connector import db_execute, db_executemany
from db.query_str import list_to_select, dict_to_where


def select_one_column(conn: mariadb.connection, db_table: str, keys: dict, select_column: str):
    where_str = dict_to_where(keys)
    query = f"SELECT `{select_column}` FROM `{db_table}` WHERE {where_str} and `{select_column}` IS NOT NULL"
    cur = conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if not rows:
        return None

    values = [row[0] for row in rows]

    return values


def select_one_row_one_column(conn: mariadb.connection, db_table: str, unique_keys: dict, select_column: str):
    func_name = 'select_one_row_one_column'

    where_str = dict_to_where(unique_keys)
    query = f"SELECT `{select_column}` FROM `{db_table}` WHERE {where_str}"
    cur = conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if not rows:
        return None
    elif len(rows) != 1:
        print(f">>> {func_name} -  DB Data Error: {unique_keys} is not unique in '{db_table}'.")
        return None

    value = rows[0][0]

    return value


def select_one_row(conn: mariadb.connection, db_table: str, unique_keys: dict, select_columns: list, return_col_descs=False):
    func_name = 'select_one_row'

    if not select_columns:
        select_str = '*'
    else:
        select_str = list_to_select(select_columns)

    where_str = dict_to_where(unique_keys)

    query = f"SELECT {select_str} FROM `{db_table}` WHERE {where_str}"
    cur = conn.cursor()
    db_execute(cur, query)
    rows = cur.fetchall()
    if not rows:
        if not return_col_descs:
            return None
        return None, None
    elif len(rows) != 1:
        print(f">>> {func_name} -  DB Data Error: {unique_keys} is not unique in '{db_table}'.")
        if not return_col_descs:
            return None
        return None, None

    if not return_col_descs:
        return rows[0]

    col_descs = cur.description

    return rows[0], col_descs


def select_one_row_pack_into_dict(conn: mariadb.connection, db_table: str, unique_keys: dict, select_columns: list):
    (row, col_descs) = select_one_row(conn, db_table, unique_keys, select_columns, return_col_descs=True)
    if row:
        col_names = [col[0] for col in col_descs]
        row_dict = dict(zip(col_names, row))
    else:
        row_dict = None

    return row_dict


def insert_dict(conn: mariadb.connection, db_table: str, new_data: dict):
    select_str = list_to_select(list(new_data.keys()))
    placeholder_str = ", ".join(['?'] * len(new_data))
    insert_data = tuple(new_data.values())

    query = f"INSERT INTO `{db_table}` ({select_str}) VALUES ({placeholder_str})"
    cur = conn.cursor()
    insert_success = db_execute(cur, query, python_data=insert_data)

    return insert_success


def insert_many(conn: mariadb.connection, db_table: str, columns: list, many_values: list):
    # many_values is a list of values list
    select_str = list_to_select(columns)

    #placeholder_str = ", ".join(['?'] * len(select_str))
    placeholder_str = ", ".join(['?'] * len(columns))

    # insert_data = [tuple(values) for values in many_values]
    insert_data = [tuple(d[k] for k in columns) for d in many_values]

    query = f"INSERT INTO {db_table} ({select_str}) VALUES ({placeholder_str})"
    cur = conn.cursor()
    insert_success = db_executemany(cur, query, insert_data)

    return insert_success
