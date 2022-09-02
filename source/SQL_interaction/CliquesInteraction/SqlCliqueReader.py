from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlReader import SqlReader


class SqlCliqueReader:
    def __init__(self, connection=None):
        if connection is None:
            connection = create_connection()
        self.__sql_reader = SqlReader(connection)

    def get_cliques(self, surely_coauthors):
        get_query = fr'''
            SELECT ID from clique where surely_coauthors = {surely_coauthors}
        '''
        return self.__sql_reader.execute_get_query(get_query)

    def get_cliques_from_component(self, component_id, surely_coauthors):
        get_query = fr'''
            SELECT id from clique where surely_coauthors = {surely_coauthors}
            and component_id = {component_id}
        '''
        return self.__sql_reader.execute_get_query(get_query)

    def get_clique_authors(self, clique_id):
        get_query = fr'''
            SELECT author_ID from clique_has_author where clique_id = {clique_id}
        '''
        return self.__sql_reader.execute_get_query(get_query)

    def get_clique_size(self, clique_id):
        get_query = fr'''
                    SELECT size from clique where id = {clique_id}
                '''
        return self.__sql_reader.execute_get_query(get_query)

    def get_clique_internal_citing(self, clique_id):
        get_query = fr'''
            SELECT internal_citing from clique where id = {clique_id}
        '''
        return self.__sql_reader.execute_get_query(get_query)

    def get_clique_external_citing(self, clique_id):
        get_query = fr'''
            SELECT external_citing from clique where id = {clique_id}
        '''
        return self.__sql_reader.execute_get_query(get_query)

    def get_clique_surely_coauthors(self, clique_id):
        get_query = fr'''
                    SELECT surely_coauthors from clique where id = {clique_id}
                '''
        return self.__sql_reader.execute_get_query(get_query)