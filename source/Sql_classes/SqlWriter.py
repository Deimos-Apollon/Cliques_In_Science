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
                        INSERT INTO Author_has_Work (Author_ID, Work_DOI)
                        VALUES ({ID}, "{DOI}")
                    '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_author_has_work_query)
        self.connection.commit()

    def add_new_author_has_work(self, ID, DOI):
        self.__insert_new_author_has_work__(ID, DOI)

    def __insert_new_author_citates_author__(self, main_ID, source_ID):
        insert_work_query = fr'''
                 INSERT INTO Author_citates_Author (Author_ID, Src_ID, Total_refs)
                 VALUES ({main_ID}, {source_ID}, 1)
             '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_work_query)
        self.connection.commit()

    def __increment_author_citates_author__(self, main_ID, source_ID):
        increment_work_query = fr'''
                 UPDATE Author_citates_Author
                 SET Total_refs = Total_refs+1 WHERE Author_ID = {main_ID}
                 and Src_ID = {source_ID}
             '''
        with self.connection.cursor() as cursor:
            cursor.execute(increment_work_query)
        self.connection.commit()

    def add_new_author_citates_author(self, main_ID, source_ID):
        check_author_citates_author_query = fr'''
                   SELECT * FROM Author_citates_Author WHERE Author_ID = {main_ID}
                   and Src_ID = {source_ID}
               '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_author_citates_author_query)
            do_exists = cursor.fetchall()

        if not do_exists:
            self.__insert_new_author_citates_author__(main_ID, source_ID)
        else:
            self.__increment_author_citates_author__(main_ID, source_ID)

    def __insert_edge_to_graph__(self, entry_ID):
        insert_edge_query = fr'''
                                   INSERT INTO Graph (Author_Citates_Author_ID)
                                   VALUES ({entry_ID})
                               '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_edge_query)
        self.connection.commit()

    def add_edge_to_graph(self, entry_ID):
        check_edge_query = fr'''
                                Select Author_Citates_Author_ID from Graph where Author_Citates_Author_ID = {entry_ID}
                            '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_edge_query)
            do_exists = cursor.fetchall()
        if not do_exists:
            self.__insert_edge_to_graph__(entry_ID)

    def increment_citation(self, citation_id, amount):
        update_query = fr'''
                            UPDATE Author_citates_Author
                            SET Total_refs = Total_refs+{amount} WHERE ID = {citation_id}
                        '''
        with self.connection.cursor() as cursor:
            cursor.execute(update_query)
        self.connection.commit()

    def delete_citation(self, citation_id):
        delete_query = fr'''
                            DELETE from Author_citates_Author WHERE ID = {citation_id}
                        '''
        with self.connection.cursor() as cursor:
            cursor.execute(delete_query)
        self.connection.commit()

    def update_author_id_in_citation(self, citation_id, new_author_id):
        # query_foreign_key = fr'''
        #             ALTER TABLE `alt_exam`.`author_citates_author` DROP FOREIGN KEY `fk_Author_has_Author_Author1`
        #             ALTER TABLE `alt_exam`.`author_citates_author` ADD CONSTRAINT `fk_Author_has_Author_Author1`
        #                 FOREIGN KEY (`Author_ID`) REFERENCES `alt_exam`.`author`(`id`) ON UPDATE SET NULL ON DELETE CASCADE
        #                     '''
        # self.execute_query(query_foreign_key)
        update_query = fr'''
                          UPDATE Author_citates_Author
                          SET Author_id = {new_author_id} WHERE ID = {citation_id}
                      '''
        with self.connection.cursor() as cursor:
            cursor.execute(update_query)
        self.connection.commit()

    def update_src_id_in_citation(self, citation_id, new_src_id):
        # query_foreign_key = fr'''
        #                     ALTER TABLE `alt_exam`.`author_citates_author` DROP FOREIGN KEY `fk_Author_has_Author_Author2`
        #                     ALTER TABLE `alt_exam`.`author_citates_author` ADD CONSTRAINT `fk_Author_has_Author_Author2`
        #                         FOREIGN KEY (`Src_id`) REFERENCES `alt_exam`.`author`(`id`) ON UPDATE CASCADE ON DELETE CASCADE
        #                             '''
        # self.execute_query(query_foreign_key)
        update_query = fr'''
                         UPDATE Author_citates_Author
                         SET Src_id = {new_src_id} WHERE ID = {citation_id}
                     '''
        with self.connection.cursor() as cursor:
            cursor.execute(update_query)
        self.connection.commit()

    def execute_query(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
        self.connection.commit()
