
class SqlWriter:
    def __init__(self, connection):
        self.connection = connection

    def commit(self):
        self.connection.commit()

    def execute_query(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
        self.connection.commit()

    def add_author(self, given_name, family_name):
        insert_author_query = fr'''
                INSERT IGNORE INTO author (given_name, family_name)
                VALUES ("{given_name}", "{family_name}")
        '''
        self.execute_query(insert_author_query)

    def add_work(self, doi, year, referenced_count):
        insert_work_query = fr'''
                INSERT IGNORE INTO work (DOI, year, Referenced_count)
                VALUES ("{doi}", {year}, {referenced_count})
        '''
        self.execute_query(insert_work_query)

    def add_author_has_work(self, author_id, doi):
        insert_author_has_work_query = fr'''
                INSERT IGNORE INTO author_has_work (author_ID, work_doi)
                VALUES ({author_id}, "{doi}")
        '''
        self.execute_query(insert_author_has_work_query)

    def add_author_cites_author(self, main_id, source_id):
        insert_work_query = fr'''
                INSERT into author_cites_author (Author_ID, Src_ID, total_refs)
                VALUES ({main_id}, {source_id}, 1)
                ON DUPLICATE key update total_refs = total_refs + 1;
        '''
        self.execute_query(insert_work_query)

    def add_work_cites_work(self, work_doi, src_doi):
        insert_query = fr'''
                INSERT IGNORE INTO work_cites_work (work_doi, src_doi)
                VALUES ("{work_doi}", "{src_doi}")
        '''
        self.execute_query(insert_query)

    def add_edge_to_graph(self, entry_id):
        insert_edge_query = fr'''
                INSERT IGNORE INTO graph (Edge_ID)
                VALUES ({entry_id})
        '''
        self.execute_query(insert_edge_query)

    def delete_citation(self, citation_id):
        delete_query = fr'''
                DELETE FROM author_cites_author WHERE ID = {citation_id}
        '''
        self.execute_query(delete_query)

    def update_citation_author(self, citation_id, new_author_id):
        update_query = fr'''
                UPDATE author_cites_author
                SET author_id = {new_author_id} WHERE ID = {citation_id}
        '''
        self.execute_query(update_query)

    def update_citation_src(self, citation_id, new_src_id):
        update_query = fr'''
                UPDATE author_cites_author
                SET Src_id = {new_src_id} WHERE ID = {citation_id}
        '''
        self.execute_query(update_query)

    def add_component(self, component_id):
        add_query = fr'''
                INSERT IGNORE INTO component VALUES ({component_id})
        '''
        self.execute_query(add_query)

    def add_edge_in_component(self, component_color, edge_id):
        add_query = fr'''
                INSERT IGNORE INTO component_has_edges (Component_ID, Graph_edge_ID)
                VALUES ({component_color}, {edge_id})
        '''
        self.execute_query(add_query)
