from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlReader import SqlReader
from source.SQL_interaction.SqlWriter import SqlWriter


class SqlCliqueWriter:
    def __init__(self, connection=None):
        if connection is None:
            connection = create_connection()
        self.__sql_writer = SqlWriter(connection)
        self.__sql_reader = SqlReader(connection)

    def write_clique(self, component, authors, surely_coauthors):
        size = len(authors)
        self.__add_clique(component, surely_coauthors, size)
        get_last_id = f'''
            SELECT ID FROM clique ORDER BY ID DESC
        '''
        last_id = self.__sql_reader.execute_get_query(get_last_id)[0][0]
        for author in authors:
            self.__add_clique_has_author(last_id, author)

    def __add_clique(self, component, surely_coauthors, size):
        add_query = f'''
                    INSERT INTO clique (component_ID, surely_coauthors, size)
                    VALUES ({component}, {surely_coauthors}, {size})
                '''
        self.__sql_writer.execute_query(add_query)

    def __add_clique_has_author(self, clique_id, author_id):
        add_query = f'''
            INSERT INTO clique_has_author (clique_ID, author_ID)
            VALUES ({clique_id}, {author_id})
        '''
        self.__sql_writer.execute_query(add_query)

    def change_citing_in_clique(self, clique_id, internal, external):
        alter_query = fr'''
            UPDATE clique
            SET internal_citing={internal}, external_citing={external}
            WHERE id={clique_id}
        '''
        self.__sql_writer.execute_query(alter_query)
