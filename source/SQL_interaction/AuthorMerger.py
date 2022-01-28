from tqdm.notebook import tqdm

from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlReader import SqlReader
from source.SQL_interaction.SqlWriter import SqlWriter
from source.time_decorator import time_method_decorator


class AuthorMerger:
    def __init__(self):
        connection = create_connection()
        self.__sql_writer = SqlWriter(connection)
        self.__sql_reader = SqlReader(connection)

    def merge_authors(self):
        merged_authors = 0
        with open(r"logs\merge_authors_logs.txt", 'w') as log:
            short_author_ids = self.__sql_reader.get_authors_with_short_names()
            for short_author_id in tqdm(short_author_ids):
                short_author_id = short_author_id[0]
                short_author_given, short_author_family = self.__sql_reader.get_author_name(short_author_id)[0]
                if len(short_author_given) < 2 or short_author_given[1] != '.':  # если не сокращение
                    continue
                # ищем ссылки "короткий на длинного"
                full_authors_ids = set(self.__sql_reader.get_referenced_authors(short_author_id))
                full_authors_ids.update(self.__sql_reader.get_referencing_authors(short_author_id))
                # проверяем, можно ли однозначно определить с кем сливать
                valid_full_author_id = self.__find_valid_full_id(full_authors_ids, short_author_id,
                                                                 short_author_given, short_author_family, log)

                # если был только 1 подходящий автор - мержим
                if valid_full_author_id is not None:
                    # СЛИЯНИЕ В AUTHOR_CITES_AUTHOR
                    self.__merge_citations_short_to_someone(short_author_id, valid_full_author_id)
                    self.__merge_citations_someone_to_short(short_author_id, valid_full_author_id)

                    # СЛИЯНИЕ В AUTHOR_HAS_WORK
                    self.__merge_author_has_work(short_author_id, valid_full_author_id)

                    # СЛИЯНИЕ В AUTHOR
                    self.__merge_authors(short_author_id)
                    log.write(f"COMP: Выполнилось слияние {short_author_id} с {valid_full_author_id}\n")
                    merged_authors += 1
            log.write(f"Слито авторов: {merged_authors}")

    def __find_valid_full_id(self, full_authors_ids, short_author_id, short_author_given, short_author_family,
                             log):
        valid_full_author_id = None
        if full_authors_ids:
            for full_author_id in full_authors_ids:
                full_author_id = full_author_id[0]  # распаковываем тапл
                if short_author_id != full_author_id:
                    full_author_given, full_author_family = self.__sql_reader.get_author_name(
                        full_author_id)[0]
                    if short_author_given[0] == full_author_given[0] \
                            and short_author_family == full_author_family:
                        if valid_full_author_id is not None:
                            log.write(f"WARN: Несколько вариаций для автора с id = {short_author_id}\n")
                            valid_full_author_id = None
                            break
                        else:
                            valid_full_author_id = full_author_id
        return valid_full_author_id

    def __merge_authors(self, short_author_id):
        update_query = fr'''
                            Delete from author where id = {short_author_id}
                        '''
        self.__sql_writer.execute_query(update_query)

    def __merge_author_has_work(self, short_author_id, full_author_id):
        get_query = fr'''
                            SELECT * FROM author_has_work where author_ID = {short_author_id}
                        '''
        entries_author_has_work = self.__sql_reader.execute_get_query(get_query)
        for entry in entries_author_has_work:
            update_query = fr'''
                                REPLACE INTO author_has_work VALUES ({entry[0]}, {full_author_id}, "{entry[2]}")
                            '''
            self.__sql_writer.execute_query(update_query)

    def __merge_citations_short_to_someone(self, short_author_id, full_author_id):
        ids_short_to_someone = self.__sql_reader.get_all_citation_via_author(short_author_id)
        for short_to_someone in ids_short_to_someone:
            # получаем из цитаты айди автора короткого и айди на кого он ссылается
            short_to_someone = short_to_someone[0]  # распаковываем тапл
            someone_id = self.__sql_reader.get_authors_via_citation(short_to_someone)[0]
            if someone_id:
                someone_id = someone_id[1]  # берем только на кого ссылается, кто ссылается мы уже знаем
                full_to_someone = self.__sql_reader.get_citation_via_authors(full_author_id, someone_id)
                # проверяем, есть ли такая же запись только где автор с полным именем ссылается на того же
                if full_to_someone:
                    self.__sql_writer.delete_citation(short_to_someone)
                else:
                    self.__sql_writer.update_citation_author(short_to_someone, full_author_id)

    def __merge_citations_someone_to_short(self, short_author_id, full_author_id):
        # берем айди всех цитат, где ссылаются на автора короткого
        ids_someone_to_short = self.__sql_reader.get_all_citation_via_src(short_author_id)
        for someone_to_short in ids_someone_to_short:
            # получаем из цитаты айди кто ссылается и айди автора короткого
            someone_to_short = someone_to_short[0]  # распаковываем тапл
            someone_id = self.__sql_reader.get_authors_via_citation(someone_to_short)[0]
            if someone_id:
                someone_id = someone_id[0]  # берем только кто ссылается, на кого ссылается мы уже знаем
                someone_to_full = self.__sql_reader.get_citation_via_authors(someone_id,
                                                                             full_author_id)
                # проверяем, есть ли такая же запись только где тот же автор ссылается на автора полного
                if someone_to_full:
                    self.__sql_writer.delete_citation(someone_to_short)
                else:
                    self.__sql_writer.update_citation_src(someone_to_short, full_author_id)
