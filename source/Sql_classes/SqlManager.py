from source.Sql_classes.SqlReader import SqlReader
from source.Sql_classes.SqlWriter import SqlWriter
from source.config import SQL_USER, SQL_PASS
from mysql.connector import connect, Error, ProgrammingError

class SqlManager:
    def __init__(self):
        self.connection = None
        try:
            self.connection = connect(
                host="localhost",
                user=SQL_USER,
                password=SQL_PASS,
                database='alt_exam'
            )
            print('Connected')
        except Error as e:
            raise ValueError(f'Error setting connection: {e}')
        self.writer = SqlWriter(self.connection)
        self.reader = SqlReader(self.connection)
