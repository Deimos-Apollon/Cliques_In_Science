from source.config import SQL_USER, SQL_PASS
from mysql.connector import connect, Error, ProgrammingError


class SqlReader:
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

    def check_if_DOI_exists(self, doi):
        check_query = fr'''
               SELECT DOI FROM Work WHERE DOI = '{doi}'
               '''
        with self.connection.cursor() as cursor:
            cursor.execute(check_query)
            do_exists = cursor.fetchall()
        return do_exists

    def get_last_author_id(self):
        get_query = fr'''
               select id from author ORDER BY id DESC LIMIT 1;
           '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            ans = cursor.fetchall()
        return ans[0][0]

    def count_author_works(self, ID):
        get_query = fr'''
               SELECT COUNT(*) FROM author_has_work WHERE author_id=({ID})
           '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            ans = cursor.fetchall()
        return ans[0][0]

    def get_authors_of_work(self, doi):
        get_query = fr'''
                          SELECT Author_ID FROM Author_has_Work WHERE Work_DOI = '{doi}'
                          '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            authors = cursor.fetchall()
        return authors

    def get_all_authors(self):
        get_query = fr'''
                          SELECT ID FROM Author
                       '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            authors = cursor.fetchall()
        return tuple(elem[0] for elem in authors)

    def get_all_citations(self):
        get_query = fr'''
                            SELECT * FROM Author_citates_Author
                   '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            authors_citates_authors = cursor.fetchall()
        return authors_citates_authors

    def get_number_citations(self):
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

    def get_citation_from_citations_via_id(self, entry_ID):
        get_query = fr'''
                            SELECT Author_ID, Src_ID FROM Author_citates_Author WHERE ID = {entry_ID}
                   '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            entry = cursor.fetchall()
        return entry[0]

    def get_citation_from_citations_via_authors(self, Author_ID, Src_ID):
        get_query = fr'''
                               SELECT ID  FROM Author_citates_Author WHERE Author_ID = {Author_ID} and Src_ID = {Src_ID}
                      '''
        with self.connection.cursor() as cursor:
            cursor.execute(get_query)
            entry = cursor.fetchall()
        return entry[0][0] if entry else None