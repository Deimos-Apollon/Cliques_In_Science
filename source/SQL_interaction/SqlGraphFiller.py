from tqdm.notebook import trange

from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlReader import SqlReader
from source.SQL_interaction.SqlWriter import SqlWriter


class SqlGraphFiller:
    def __init__(self):
        connection = create_connection()
        self.__sql_writer = SqlWriter(connection)
        self.__sql_reader = SqlReader(connection)

    def fill_graph_table(self):
        citations_num = self.__sql_reader.get_citations_number()[0][0]
        for i in trange(1, citations_num + 1):
            try_get_citation_in_graph = self.__sql_reader.get_authors_via_citation(i)
            if try_get_citation_in_graph:
                author, src = try_get_citation_in_graph[0]
                co_citation_id = self.__sql_reader.get_citation_via_authors(src, author)
                if co_citation_id:
                    co_citation_id = co_citation_id[0][0]
                    self.__sql_writer.add_edge_to_graph(i)
                    if co_citation_id != i:
                        self.__sql_writer.add_edge_to_graph(co_citation_id)

