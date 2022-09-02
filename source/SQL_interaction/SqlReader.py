class SqlReader:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor()

    def get_all_authors(self):
        get_query = fr'''
                SELECT ID FROM author
        '''
        return self.execute_get_query(get_query)

    def get_author_id(self, given, family):
        get_query = fr'''
                SELECT ID FROM author WHERE given_name = ("{given}") AND family_name = ("{family}")
        '''
        return self.execute_get_query(get_query)

    def get_work_authors(self, doi):
        get_query = fr'''
                SELECT author_ID FROM author_has_work WHERE work_DOI = "{doi}"
        '''
        return self.execute_get_query(get_query)

    def get_work_references(self, work_doi):
        get_query = fr'''
                SELECT Src_DOI FROM work_cites_work WHERE Work_DOI = "{work_doi}"
        '''
        return self.execute_get_query(get_query)

    def get_work_is_referenced_count(self, work_doi):
        get_query = fr'''
                SELECT Referenced_count FROM work WHERE DOI = "{work_doi}"
        '''
        return self.execute_get_query(get_query)

    def get_work_year(self, work_doi):
        get_query = fr'''
                        SELECT year FROM work WHERE DOI = "{work_doi}"
                '''
        return self.execute_get_query(get_query)

    def get_author_works(self, author_id):
        get_query = fr'''
                SELECT Work_DOI FROM author_has_work WHERE Author_ID = {author_id}
        '''
        return self.execute_get_query(get_query)

    def get_citations_number(self):
        get_query = fr'''
                SELECT ID FROM author_cites_author ORDER BY ID DESC Limit 1
        '''
        return self.execute_get_query(get_query)

    def get_citation_id_from_graph(self, entry_id):
        get_query = fr'''
                SELECT Edge_ID from graph where ID = {entry_id}
        '''
        return self.execute_get_query(get_query)

    def get_authors_via_citation(self, entry_id):
        get_query = fr'''
                SELECT author_ID, Src_ID FROM author_cites_author WHERE ID = {entry_id}
        '''
        return self.execute_get_query(get_query)

    def get_citation_via_id(self, entry_id):
        get_query = fr'''
                SELECT author_ID, Src_ID, total_refs FROM author_cites_author WHERE ID = {entry_id}
        '''
        return self.execute_get_query(get_query)

    def get_citation_via_authors(self, author_id, src_id):
        get_query = fr'''
                SELECT ID, total_refs FROM author_cites_author WHERE author_ID = {author_id} and Src_ID = {src_id}
        '''
        return self.execute_get_query(get_query)

    def get_all_citation_via_author(self, author_id):
        get_query = fr'''
                SELECT ID FROM author_cites_author WHERE author_ID = {author_id}
        '''
        return self.execute_get_query(get_query)

    def get_all_citation_via_src(self, src_id):
        get_query = fr'''
                SELECT ID FROM author_cites_author WHERE Src_ID = {src_id}
        '''
        return self.execute_get_query(get_query)

    def get_authors_with_short_names(self):
        get_query = fr'''
                SELECT ID FROM author WHERE CHAR_LENGTH(given_name) < 5
        '''
        return self.execute_get_query(get_query)

    def get_referenced_authors(self, author_id):
        get_query = fr'''
                SELECT src_id FROM author_cites_author WHERE author_ID = {author_id}
        '''
        return self.execute_get_query(get_query)

    def get_referencing_authors(self, src_id):
        get_query = fr'''
                SELECT author_id FROM author_cites_author WHERE src_ID = {src_id}
        '''
        return self.execute_get_query(get_query)

    def get_author_name(self, author_id):
        get_query = fr'''
                SELECT given_name, family_name FROM author WHERE ID = {author_id}
        '''
        return self.execute_get_query(get_query)

    def check_if_coauthors(self, author_id, src_id):
        if author_id == src_id:
            return True
        author_works = self.get_author_works(author_id)
        for work in author_works:
            work = work[0]
            work_authors = self.get_work_authors(work)
            for author in work_authors:
                author = author[0]
                if author == src_id:
                    return True
        return False

    def execute_get_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
