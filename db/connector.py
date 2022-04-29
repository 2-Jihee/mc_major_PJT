import mariadb
import sys
from db import connector_info


def db_connect(database: str):
    try:
        conn = mariadb.connect(
            user=connector_info.user,
            password=connector_info.password,
            host=connector_info.host,
            port=connector_info.port,
            database=database
        )
        conn.autocommit = False

    except mariadb.Error as e:
        print(f">>> MariaDB Error: {e}")
        sys.exit(1)

    return conn


def db_execute(cur: mariadb.connection.cursor, query: str, python_data=None):
    current_function = 'db_execute'
    # python_data is a tuple

    try:
        cur.execute(query, python_data)
        query_outcome = True

    except mariadb.Error as e:
        print(f">>> MariaDB Error: {e}")
        query_outcome = False

    return query_outcome
