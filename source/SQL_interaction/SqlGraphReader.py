from collections import defaultdict

from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlReader import SqlReader
from source.SQL_interaction.SqlWriter import SqlWriter


class SqlGraphReader:
    def __init__(self):
        connection = create_connection()
        self.sql_writer = SqlWriter(connection)
        self.sql_reader = SqlReader(connection)

    def get_components(self):
        get_query = fr'''
            SELECT ID from component
        '''
        return self.sql_reader.execute_get_query(get_query)

    def get_incidence_lists(self):
        incidence_lists = defaultdict(set)
        get_query = fr'''
             SELECT Edge_ID FROM graph
        '''
        entry = self.sql_reader.execute_get_query(get_query)
        for citation in entry:
            if citation:
                citation = citation[0]
                author_id, source_id = self.sql_reader.get_authors_via_citation(citation)[0]
                # храним вершину и ребро по которому они смежны
                incidence_lists[author_id].add((source_id, citation))
        return incidence_lists if incidence_lists else None

    def get_edges(self):
        edges = set()
        get_query = fr'''
            SELECT Edge_ID FROM graph
        '''
        for citation in self.sql_reader.execute_get_query(get_query):
            try_got_authors = self.sql_reader.get_authors_via_citation(citation[0])
            if try_got_authors:
                author_id, src_id = try_got_authors[0], try_got_authors[1]
                edges.add((author_id, src_id))
        return edges

    def get_component_edges(self, component_color):
        edges = set()
        get_query = fr'''
            SELECT graph_edge_ID FROM component_has_edges where Component_ID = {component_color}
        '''
        for citation in self.sql_reader.execute_get_query(get_query):
            try_got_authors = self.sql_reader.get_authors_via_citation(citation[0])
            if try_got_authors:
                author_id, src_id = try_got_authors[0], try_got_authors[1]
                edges.add((author_id, src_id))
        return edges

    def get_unique_component_edges(self, component_color):
        edges = set()
        get_query = fr'''
            SELECT graph_edge_ID FROM component_has_edges where Component_ID = {component_color}
        '''
        for citation in self.sql_reader.execute_get_query(get_query):
            try_got_authors = self.sql_reader.get_authors_via_citation(citation[0])
            if try_got_authors:
                author_id, src_id = try_got_authors[0], try_got_authors[1]
                if (src_id, author_id) not in edges:
                    edges.add((author_id, src_id))
        return edges

    def get_component_incidence_lists(self, component_color):
        incidence_lists = defaultdict(set)
        get_query = fr'''
             SELECT graph_edge_ID FROM component_has_edges where Component_ID = {component_color}
        '''
        entry = self.sql_reader.execute_get_query(get_query)
        if entry is not None:
            for citation in entry:
                if citation:
                    citation = citation[0]
                    author_id, source_id = self.sql_reader.get_authors_via_citation(citation)[0]
                    # храним вершину и ребро по которому они смежны
                    incidence_lists[author_id].add((source_id, citation))
        return incidence_lists if incidence_lists else None

    def get_component_incidence_lists_coauthors(self, component_color):
        incidence_lists = defaultdict(set)
        get_query = fr'''
            SELECT graph_edge_ID FROM component_has_edges where Component_ID = {component_color}
        '''
        entry = self.sql_reader.execute_get_query(get_query)
        if entry is not None:
            for citation in entry:
                if citation:
                    citation = citation[0]
                    author_id, source_id = self.sql_reader.get_authors_via_citation(citation)[0]
                    # храним вершину и ребро по которому они смежны
                    if self.sql_reader.check_if_coauthors(author_id, source_id):
                        incidence_lists[author_id].add((source_id, citation))
        return incidence_lists if incidence_lists else None
