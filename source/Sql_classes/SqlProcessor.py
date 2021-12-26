from source.Sql_classes.SqlManager import SqlManager
from mysql.connector import Error as mysql_Error
from progress.bar import IncrementalBar
import time


class SqlProcessor:
    def __init__(self):
        self.sql_manager = SqlManager()

    def citations_to_graph_table(self):

        start = time.time()

        citations_num = self.sql_manager.reader.get_number_citations()
        bar = IncrementalBar(max=citations_num)
        for i in range(1, citations_num + 1):
            try_get_citation_in_graph = self.sql_manager.reader.get_authors_from_citations_via_id(i)
            if try_get_citation_in_graph:
                author, src = try_get_citation_in_graph
                co_citation_id = self.sql_manager.reader.get_citation_from_citations_via_authors(src, author)
                if co_citation_id:
                    self.sql_manager.writer.add_edge_to_graph(i)
                    self.sql_manager.writer.add_edge_to_graph(co_citation_id)
            bar.next()

        bar.finish()
        print("Edges adding time in minutes:", (time.time() - start) / 60)

    def merge_authors(self):
        with open("merge_authors_logs.txt", 'w') as file:
            short_author_ids_with_short_names = self.sql_manager.reader.get_authors_with_short_names()
            for short_author_id in short_author_ids_with_short_names:
                short_author_id = short_author_id[0] # распаковываем тапл
                try:
                    short_author_given, short_author_family = self.sql_manager.reader.get_author_name(short_author_id)
                    if short_author_given[1] != '.':  # если не сокращение
                        continue
                    full_authors_ids = self.sql_manager.reader.get_src_authors(short_author_id)
                    # проверяем, можно ли однозначно определить с кем сливать
                    valid_full_author_id = None
                    if full_authors_ids is not None:
                        for full_author_id in full_authors_ids:
                            full_author_id = full_author_id[0] # распаковываем тапл
                            full_author_given, full_author_family = self.sql_manager.reader.get_author_name(full_author_id)
                            if short_author_given[0] == full_author_given[0] and short_author_family == full_author_family:
                                if valid_full_author_id is not None:
                                    file.write(f"Несколько вариаций для автора с id = {short_author_id}\n")
                                    valid_full_author_id = None
                                    break
                                else:
                                    valid_full_author_id = full_author_id
                    # если был только 1 подходящий автор - мержим
                    if valid_full_author_id is not None:

                        # СЛИЯНИЕ В AUTHOR_HAS_WORK
                        self.__merge_author_has_work(short_author_id, valid_full_author_id)

                        # СЛИЯНИЕ В AUTHOR_CITATES_AUTHOR
                        self.__merge_citations_short_to_someone(short_author_id, valid_full_author_id)
                        self.__merge_citations_someone_to_short(short_author_id, valid_full_author_id)

                        # СЛИЯНИЕ В AUTHOR
                        self.__merge_authors(short_author_id, valid_full_author_id)
                except mysql_Error as e:
                    print(f"LOG: error in merge_authors: {e.msg}\n")

    def __merge_authors(self, short_author_id, full_author_id):
        update_query = fr'''
                         Delete from Author where id = {short_author_id}
                     '''
        self.sql_manager.writer.execute_query(update_query)

    def __merge_author_has_work(self, short_author_id, full_author_id):
        update_query = fr'''
                         UPDATE Author_has_work
                         SET Author_ID = {full_author_id} WHERE Author_ID = {short_author_id}
                     '''
        self.sql_manager.writer.execute_query(update_query)

    def __merge_citations_short_to_someone(self, short_author_id, full_author_id):
        # берем айди всех цитат, где источник цитаты - автор короткий
        ids_short_to_someone = self.sql_manager.reader.get_all_citation_from_citations_via_author_id(
            short_author_id)
        for short_to_someone in ids_short_to_someone:
            # получаем из цитаты айди автора короткого и айди на кого он ссылается
            short_to_someone = short_to_someone[0]   # распаковываем тапл
            someone_id = self.sql_manager.reader.get_authors_from_citations_via_id(short_to_someone)
            if someone_id:
                someone_id = someone_id[1]  # берем только на кого ссылается, кто ссылается мы уже знаем
                full_to_someone = self.sql_manager.reader.get_citation_from_citations_via_authors(full_author_id, someone_id)
                # проверяем, есть ли такая же запись только где автор с полным именем ссылается на того же
                if full_to_someone:
                    total_short_to_someone_refs = self.sql_manager.reader.get_total_refs_from_citation(short_to_someone)
                    self.sql_manager.writer.increment_citation(full_to_someone, total_short_to_someone_refs)
                    self.sql_manager.writer.delete_citation(short_to_someone)
                else:
                    self.sql_manager.writer.update_author_id_in_citation(short_to_someone, full_author_id)

    def __merge_citations_someone_to_short(self, short_author_id, full_author_id):
        # берем айди всех цитат, где ссылаются на автора короткого
        ids_someone_to_short = self.sql_manager.reader.get_all_citation_from_citations_via_src_id(
            short_author_id)
        for someone_to_short in ids_someone_to_short:
            # получаем из цитаты айди кто ссылается и айди автора короткого
            someone_to_short = someone_to_short[0]  # распаковываем тапл
            someone_id = self.sql_manager.reader.get_authors_from_citations_via_id(someone_to_short)
            if someone_id:
                someone_id = someone_id[0]  # берем только кто ссылается, на кого ссылается мы уже знаем
                someone_to_full = self.sql_manager.reader.get_citation_from_citations_via_authors(someone_id,
                                                                                                  full_author_id)
                # проверяем, есть ли такая же запись только где тот же автор ссылается на автора полного
                if someone_to_full:
                    total_someone_to_short_refs = self.sql_manager.reader.get_total_refs_from_citation(someone_to_short)
                    self.sql_manager.writer.increment_citation(someone_to_full, total_someone_to_short_refs)
                    self.sql_manager.writer.delete_citation(someone_to_short)
                else:
                    self.sql_manager.writer.update_src_id_in_citation(someone_to_short, full_author_id)
