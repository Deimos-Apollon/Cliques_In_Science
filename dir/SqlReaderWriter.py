from dir.config import SQL_USER, SQL_PASS
from mysql.connector import connect, Error, ProgrammingError


class SqlReaderWriter:
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
            SELECT given_name FROM author WHERE given_name = ("{given_name}") AND family_name = ("{family_name}")
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
                    VALUES ("{doi}", "{year}", {references_count}, {is_referenced_count})
                    '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_work_query)
        self.connection.commit()

    def add_new_work(self, doi, year, references_count, is_referenced_count):
        check_work_query = fr'''
                   SELECT doi FROM work WHERE DOI = ("{doi}") AND year=("{year}") AND
                   references_count=("{references_count}") AND is_referenced_count=("{is_referenced_count} Limit 1")
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
                 SET Total_refs = Total_refs+1 WHERE Author_ID = '{main_ID}'
                 and Src_ID = '{source_ID}'
             '''
        with self.connection.cursor() as cursor:
            cursor.execute(increment_work_query)
        self.connection.commit()

    def add_new_author_citates_author(self, main_ID, source_ID):
        check_author_citates_author_query = fr'''
                   SELECT * FROM Author_citates_Author WHERE Author_ID = '{main_ID}' 
                   and Src_ID = '{source_ID}' 
               '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_author_citates_author_query)
            do_exists = cursor.fetchall()

        if not do_exists:
            self.__insert_new_author_citates_author__(main_ID, source_ID)
        else:
            self.__increment_author_citates_author__(main_ID, source_ID)

    def check_if_DOI_exists(self, doi):
        check_query = fr'''
            SELECT DOI FROM Work WHERE DOI = '{doi}'
            '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_query)
            do_exists = cursor.fetchall()
        return do_exists

    def get_last_author_id(self):    # TODO DELETE
        get_query = fr'''
            select id from author ORDER BY id DESC LIMIT 1;
        '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            ans = cursor.fetchall()
        return ans[0][0]

    def count_author_works(self, ID):       # TODO DELETE
        get_query = fr'''
            SELECT COUNT(*) FROM author_has_work WHERE author_id=({ID})
        '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            ans = cursor.fetchall()
        return ans[0][0]

    def get_work_authors(self, doi):
        get_query = fr'''
                       SELECT Author_ID FROM Author_has_Work WHERE Work_DOI = '{doi}'
                       '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            authors = cursor.fetchall()
        return authors

    def get_authors(self):                  # TODO DELETE
        get_query = fr'''
                       SELECT ID FROM Author
                    '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            authors = cursor.fetchall()
        return tuple(elem[0] for elem in authors)

    def get_all_author_citates_author(self):
        get_query = fr'''
                         SELECT * FROM Author_citates_Author
                '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            authors_citates_authors = cursor.fetchall()
        return authors_citates_authors

    def get_number_author_citates_author(self):
        get_query = fr'''
                         SELECT COUNT(*) FROM Author_citates_Author
                '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            num_entries = cursor.fetchall()
        return num_entries[0][0]

    def get_citation_id_from_graph(self, entry_ID):
        get_query = fr'''
                         SELECT Author_Citates_Author_ID from Graph where ID = {entry_ID}
                '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            entry = cursor.fetchall()
        return entry[0][0]

    def get_citation_via_id(self, entry_ID):
        get_query = fr'''
                         SELECT Author_ID, Src_ID FROM Author_citates_Author WHERE ID = {entry_ID}
                '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            entry = cursor.fetchall()
        return entry[0]

    def get_citation_via_authors(self, Author_ID, Src_ID):
        get_query = fr'''
                            SELECT ID  FROM Author_citates_Author WHERE Author_ID = {Author_ID} and Src_ID = {Src_ID}
                   '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            entry = cursor.fetchall()
        return entry[0][0] if entry else None

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
                                Select ID from Graph where Author_Citates_Author_ID = {entry_ID}
                            '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_edge_query)
            do_exists = cursor.fetchall()
        if not do_exists:
            self.__insert_edge_to_graph__(entry_ID)
