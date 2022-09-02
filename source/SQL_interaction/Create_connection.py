from mysql.connector import connect, Error

from source.config import SQL_PASS, SQL_USER, SQL_HOST


def create_connection():
    try:
        connection = connect(
            host=SQL_HOST,
            user=SQL_USER,
            password=SQL_PASS,
            database='alt_exam',
            charset='utf8',
        )
    except Error as e:
        raise ValueError(f'Error setting connection: {e}')
    return connection
