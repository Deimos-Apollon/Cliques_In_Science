
from source.Sql_classes.SqlWriter import SqlWriter
from source.Sql_classes.SqlReader import SqlReader
from progress.bar import IncrementalBar
import time


sql_writer = SqlWriter()
sql_reader = SqlReader()


class SqlProcessor:
    def citations_to_graph_table(self):
        start = time.time()

        citations_num = sql_reader.get_number_citations()
        bar = IncrementalBar(max=citations_num)
        for i in range(1, citations_num + 1):
            author, src = sql_reader.get_citation_from_citations_via_id(i)
            co_citation_id = sql_reader.get_citation_from_citations_via_authors(src, author)
            if co_citation_id:
                sql_writer.add_edge_to_graph(i)
                sql_writer.add_edge_to_graph(co_citation_id)
            bar.next()
        bar.finish()
        print("Edges adding time in minutes:", (time.time() - start) / 60)

