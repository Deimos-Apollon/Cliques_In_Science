import json
import time
import mysql.connector
from progress.bar import IncrementalBar

from source.Sql_classes.SqlWriter import SqlWriter
from source.Sql_classes.SqlReader import SqlReader


class JsonToSqlWriter:
    def data_read_first_phase_from_files(self, filenames):

        for filename in filenames:
            with open(filename) as file:
                samples = json.load(file)['items']
                self.__data_read_first_phase(samples)

    def __data_read_first_phase(self, samples):
        sql_writer = SqlWriter()
        sql_reader = SqlReader()

        bar1 = IncrementalBar("Reading first phase", max=len(samples))
        start = time.time()

        # data reading first phase: works, authors
        for work in samples:
            # read work's DOI
            DOI = work["DOI"]
            year = work["year"]
            references_count = work["references-count"]
            is_referenced_count = work["is-referenced-by-count"]

            authors = work.get('author')
            if authors:
                sql_writer.add_new_work(DOI, year, references_count, is_referenced_count)
                for author in authors:
                    given_name = author["given"]
                    family_name = author["family"]
                    try:
                        sql_writer.add_new_author(given_name, family_name)
                        sql_writer.add_new_author_has_work(sql_reader.get_last_author_id(), DOI)
                    except mysql.connector.errors.DataError:
                        print(given_name, family_name)

            bar1.next()

        bar1.finish()
        print()
        first_phase_time = time.time() - start
        print(f"LOG: JsonToSqlWriter first phase time in minutes: {first_phase_time / 60}")

    def __data_read_second_phase(self, samples):
        sql_writer = SqlWriter()
        sql_reader = SqlReader()
        bar1 = IncrementalBar("Reading second phase", max=len(samples))
        start = time.time()
        # data reading second phase: adding refs
        for work in samples:
            work_DOI = work["DOI"]
            work_authors = sql_reader.get_authors_of_work(work_DOI)
            references = work['reference']
            for src in references:
                src_DOI = src['DOI']

                # read author_citates_author
                src_authors = sql_reader.get_authors_of_work(src_DOI)
                if src_authors:
                    for src_author in src_authors:
                        for work_author in work_authors:
                            sql_writer.add_new_author_citates_author(work_author[0], src_author[0])
            bar1.next()
        bar1.finish()
        print()
        second_phase_time = time.time() - start
        print(f"LOG: JsonToSqlWriter second phase time in minutes: {second_phase_time / 60}")
