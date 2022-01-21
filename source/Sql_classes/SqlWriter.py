from source.config import SQL_USER, SQL_PASS
from mysql.connector import connect, Error, ProgrammingError


class SqlWriter:
    def __init__(self, connection):
        self.connection = connection

    def __insert_new_author__(self, given_name, family_name):

        insert_author_query = fr'''
                INSERT INTO author (given_name, family_name)
                VALUES ("{given_name}", "{family_name}")
                '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_author_query)
        self.connection.commit()

    def add_new_author(self, given_name, family_name):
        check_author_query = fr'''
            SELECT ID FROM author WHERE given_name = ("{given_name}") AND family_name = ("{family_name}")
             Limit 1
        '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_author_query)
            do_exists = cursor.fetchall()
        if not do_exists:
            self.__insert_new_author__(given_name, family_name)

    def __insert_new_work__(self, doi, year, references_count, is_referenced_count):
        insert_work_query = fr'''
                    INSERT INTO work
                    VALUES ("{doi}", {year}, {references_count}, {is_referenced_count})
                    '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_work_query)
        self.connection.commit()

    def add_new_work(self, doi, year, references_count, is_referenced_count):
        check_work_query = fr'''
                   SELECT doi FROM work WHERE DOI = ("{doi}") AND year=("{year}") AND
                   references_count=("{references_count}") AND is_referenced_count=("{is_referenced_count}") Limit 1
               '''

        with self.connection.cursor() as cursor:
            cursor.execute(check_work_query)
            do_exists = cursor.fetchall()
        if not do_exists:
            self.__insert_new_work__(doi, year, references_count, is_referenced_count)

    def __insert_new_author_has_work__(self, ID, DOI):
        insert_author_has_work_query = fr'''
                        INSERT INTO author_has_work (author_ID, work_DOI)
                        VALUES ({ID}, "{DOI}")
                    '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_author_has_work_query)
        self.connection.commit()

    def add_new_author_has_work(self, ID, DOI):
        check_query = fr'''
                  SELECT Count(*) FROM author_has_work WHERE author_ID = {ID}
                   and work_DOI = "{DOI}" Limit 1
             '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_query)
            do_exists = cursor.fetchall()
        if do_exists[0][0] == 0:
            self.__insert_new_author_has_work__(ID, DOI)

    def __insert_new_author_cites_author__(self, main_ID, source_ID):
        insert_work_query = fr'''
                 INSERT INTO author_cites_author (author_ID, Src_ID)
                 VALUES ({main_ID}, {source_ID})
             '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_work_query)
        self.connection.commit()

    def add_new_author_cites_author(self, main_ID, source_ID):
        check_author_cites_author_query = fr'''
                   SELECT Count(*) FROM author_cites_author WHERE author_ID = {main_ID}
                   and Src_ID = {source_ID} Limit 1
               '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_author_cites_author_query)
            do_exists = cursor.fetchall()

        if do_exists[0][0] == 0:
            self.__insert_new_author_cites_author__(main_ID, source_ID)

    def __insert_new_work_cites_work(self, work_DOI, src_DOI):
        insert_query = fr'''
                               INSERT INTO work_cites_work (work_DOI, Src_DOI) VALUES ("{work_DOI}", "{src_DOI}")
                       '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_query)
        self.connection.commit()

    def add_new_work_cites_work(self, work_DOI, src_DOI):
        check_query = fr'''
                        SELECT COUNT(*) FROM work where DOI = "{src_DOI}" LIMIT 1
                '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_query)
            do_exists = cursor.fetchall()
        if do_exists[0][0] != 0:
            self.__insert_new_work_cites_work(work_DOI, src_DOI)

    def __insert_edge_to_graph__(self, entry_ID):
        insert_edge_query = fr'''
                                   INSERT INTO graph (Author_citates_Author_ID)
                                   VALUES ({entry_ID})
                               '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_edge_query)
        self.connection.commit()

    def add_edge_to_graph(self, entry_ID):
        check_edge_query = fr'''
                                Select Author_citates_Author_ID from graph where Author_citates_Author_ID = {entry_ID}
                            '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_edge_query)
            do_exists = cursor.fetchall()
        if not do_exists:
            self.__insert_edge_to_graph__(entry_ID)

    def delete_citation(self, citation_id):
        delete_query = fr'''
                            DELETE from author_cites_author WHERE ID = {citation_id}
                        '''
        with self.connection.cursor() as cursor:
            cursor.execute(delete_query)
        self.connection.commit()

    def update_citation_author(self, citation_id, new_author_id):
        update_query = fr'''
                          UPDATE author_cites_author
                          SET author_id = {new_author_id} WHERE ID = {citation_id}
                      '''
        with self.connection.cursor() as cursor:
            cursor.execute(update_query)
        self.connection.commit()

    def update_citation_src(self, citation_id, new_src_id):
        update_query = fr'''
                         UPDATE author_cites_author
                         SET Src_id = {new_src_id} WHERE ID = {citation_id}
                     '''
        with self.connection.cursor() as cursor:
            cursor.execute(update_query)
        self.connection.commit()

    def execute_query(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
        self.connection.commit()
