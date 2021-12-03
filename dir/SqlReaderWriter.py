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

    def __insert_new_author__(self, given_name, family_name, affilation, ORCID):
        has_orcid = 1 if ORCID else 0

        insert_author_query = fr'''
                INSERT INTO author (given_name, family_name, affilation, has_orcid, ORCID)
                VALUES ("{given_name}", "{family_name}", "{affilation}", {has_orcid}, "{ORCID}")
                '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_author_query)
        self.connection.commit()

    def add_new_author(self, given_name, family_name, affilation, ORCID):
        check_author_query = fr'''
            SELECT given_name FROM author WHERE given_name = ("{given_name}") AND family_name = ("{family_name}")
             AND affilation = ("{affilation}") AND ORCID = ("{ORCID}")  Limit 1
        '''
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(check_author_query)
                do_exists = cursor.fetchall()
        except ProgrammingError as e:
            print(check_author_query)
        if not do_exists:
            self.__insert_new_author__(given_name, family_name, affilation, ORCID)

    def __insert_new_work__(self, doi, date, references_count, is_referenced_count):
        insert_work_query = fr'''
                    INSERT INTO work
                    VALUES ("{doi}", "{date}", {references_count}, {is_referenced_count})
                    '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_work_query)
        self.connection.commit()

    def add_new_work(self, doi, date, references_count, is_referenced_count):
        check_work_query = fr'''
                   SELECT doi FROM work WHERE DOI = ("{doi}") AND date=("{date}") AND
                   references_count=("{references_count}") AND is_referenced_count=("{is_referenced_count} Limit 1")
               '''

        with self.connection.cursor() as cursor:
            cursor.execute(check_work_query)
            do_exists = cursor.fetchall()

        if not do_exists:
            self.__insert_new_work__(doi, date, references_count, is_referenced_count)

    def __insert_new_author_citates_author__(self, main_ORCID, source_ORCID):
        insert_work_query = fr'''
                INSERT INTO Author_citates_Author
                VALUES ('{main_ORCID}', '{source_ORCID}')
            '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_work_query)
        self.connection.commit()

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

    def get_author_id(self, given_name, family_name, affilation, ORCID):    # TODO DELET
        get_query = fr'''
            SELECT ID FROM author WHERE given_name = ("{given_name}") AND family_name = ("{family_name}")
             AND affilation = ("{affilation}") AND ORCID = ("{ORCID}") Limit 1
        '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            ans = cursor.fetchall()
        return ans

    def count_author_works(self, ID):       # TODO DELET
        get_query = fr'''
            SELECT COUNT(*) FROM author_has_work WHERE author_id=({ID})
        '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            ans = cursor.fetchall()
        return ans[0][0]

    def add_new_author_citates_author(self, main_ORCID, source_ORCID):
        check_author_citates_author_query = fr'''
                   SELECT * FROM Author_citates_Author WHERE Author_ORCID = '{main_ORCID}' 
                   and Source_ORCID = '{source_ORCID}' 
               '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_author_citates_author_query)
            do_exists = cursor.fetchall()

        if not do_exists:
            self.__insert_new_author_citates_author__(main_ORCID, source_ORCID)
        # else:
        #     print(f"Add_new_author_citates_author {do_exists}")  # TODO DELETE

    def __insert_work_ref__(self, doi, src_doi):
        insert_work_ref_query = fr'''
                        INSERT INTO citation
                        VALUES ('{doi}', '{src_doi}')
                    '''
        with self.connection.cursor() as cursor:
            cursor.execute(insert_work_ref_query)
        self.connection.commit()

    def add_new_work_ref(self, doi, src_doi):
        check_work_ref_query = fr'''
               SELECT * FROM citation WHERE Work_DOI = '{doi}' and Source_DOI = '{src_doi}'
           '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_work_ref_query)
            do_exists = cursor.fetchall()
        if not do_exists:
            self.__insert_work_ref__(doi, src_doi)

    def check_if_DOI_exists(self, doi):
        check_query = fr'''
            SELECT DOI FROM Work WHERE DOI = '{doi}'
            '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_query)
            do_exists = cursor.fetchall()

        return do_exists

    def check_if_ORCID_exists(self, orcid):
        check_query = fr'''
                SELECT ORCID FROM Author WHERE ORCID = '{orcid}'
                '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_query)
            do_exists = cursor.fetchall()

        return do_exists

    def get_work_authors(self, doi):
        get_query = fr'''
                       SELECT Author_ORCID FROM Author_has_Work WHERE Work_DOI = '{doi}'
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
