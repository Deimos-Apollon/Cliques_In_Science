from source.SQL_interaction.CliquesInteraction.SqlCliqueReader import SqlCliqueReader
from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlGraphReader import SqlGraphReader
from source.SQL_interaction.SqlReader import SqlReader


class SqlCliqueAnalyser:
    def __init__(self, connection=None):
        if connection is None:
            connection = create_connection()
        self.clique_reader = SqlCliqueReader(connection)
        self.sql_reader = SqlReader(connection)
        self.clique_reader = SqlCliqueReader(connection)

    def get_internal_citing(self, clique_id):
        clique_size = self.clique_reader.get_clique_size(clique_id)[0][0]
        total_refs = self.__count_internal_links_authors(clique_id)
        clique_coef = total_refs / clique_size ** 2
        return clique_coef

    def get_external_citing(self, clique_id):
        clique_authors = self.clique_reader.get_clique_authors(clique_id)
        clique_works_refs = {}
        internal_links_num = self.count_internal_links_works(clique_id)
        for author in clique_authors:
            author = author[0]
            author_works = self.sql_reader.get_author_works(author)
            for work in author_works:
                work = work[0]
                if work not in clique_works_refs:
                    clique_works_refs[work] = self.sql_reader.get_work_is_referenced_count(work)[0][0]
        clique_coef = (sum(clique_works_refs.values()) - internal_links_num) / len(clique_works_refs.keys())
        return clique_coef

    def __count_internal_links_authors(self, clique_id):
        total_refs = 0
        clique_authors = self.clique_reader.get_clique_authors(clique_id)
        if clique_authors:
            clique_authors = [author[0] for author in clique_authors]
        for author in clique_authors:
            author_works = self.sql_reader.get_author_works(author)
            for work in author_works:
                work = work[0]
                src_works = self.sql_reader.get_work_references(work)
                for src_work in src_works:
                    src_work = src_work[0]
                    work_authors = self.sql_reader.get_work_authors(src_work)
                    for work_author in work_authors:
                        work_author = work_author[0]
                        if work_author in clique_authors and work_author != author:
                            total_refs += 1
        return total_refs

    def count_internal_links_works(self, clique_id):
        total_refs = 0
        clique_works = set()
        clique_authors = self.clique_reader.get_clique_authors(clique_id)
        for author in clique_authors:
            author = author[0]
            for author_works in self.sql_reader.get_author_works(author):
                for work in author_works:
                    clique_works.add(work.lower())
        for work in clique_works:
            work_refs = self.sql_reader.get_work_references(work)
            for work_ref in work_refs:
                work_ref = work_ref[0].lower()
                if work_ref in clique_works:
                    if work_ref != work:
                        total_refs += 1
        return total_refs
