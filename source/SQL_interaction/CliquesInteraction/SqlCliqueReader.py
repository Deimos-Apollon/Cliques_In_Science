from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlReader import SqlReader


class SqlCliqueReader:
    def __init__(self):
        self.__sql_reader = SqlReader(create_connection())

    def get_cliques(self, surely_coauthors):
        get_query = fr'''
            SELECT ID from clique where surely_coauthors = {surely_coauthors}
        '''
        return self.__sql_reader.execute_get_query(get_query)
