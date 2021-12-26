from source.Sql_classes.SqlManager import SqlManager
from collections import defaultdict


class SqlGraphReader:
    def __init__(self):
        self.sql_manager = SqlManager()

    def get_all_vertices(self):
        vertices = set()
        get_query = fr'''
                          SELECT Author_citates_Author_ID FROM Graph
                     '''
        entry = self.sql_manager.reader.execute_get_query(get_query)
        if entry is not None:
            for citation in entry:
                if citation:
                    citation = citation[0]
                    author_id, source_id = self.sql_manager.reader.get_authors_from_citations_via_id(citation)
                    vertices.add(author_id)
        return vertices if vertices else None

    def get_incidence_lists(self):
        incidence_lists = defaultdict(set)
        get_query = fr'''
                             SELECT Author_citates_Author_ID FROM Graph
                        '''
        entry = self.sql_manager.reader.execute_get_query(get_query)
        if entry is not None:
            for citation in entry:
                if citation:
                    citation = citation[0]
                    author_id, source_id = self.sql_manager.reader.get_authors_from_citations_via_id(citation)
                    incidence_lists[author_id].add(source_id)
                    incidence_lists[source_id].add(author_id)
        return incidence_lists if incidence_lists else None
