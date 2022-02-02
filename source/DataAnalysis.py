from source.SQL_interaction.Create_connection import create_connection

from source.SQL_interaction.SqlReader import SqlReader


class DataAnalyser:
    def __init__(self):
        connection = create_connection()
        self.sql_reader = SqlReader(connection)

