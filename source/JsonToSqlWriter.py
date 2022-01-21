import json
import time
import mysql.connector
from progress.bar import IncrementalBar
from source.Sql_classes.SqlManager import SqlManager
from source.time_decorator import time_method_decorator


class JsonToSqlWriter:
    @time_method_decorator
    def data_read_first_phase_from_files(self, filenames, file_number_left):
        for file_number, filename in enumerate(filenames, start=file_number_left):
            with open(filename) as file:
                samples = json.load(file)['items']
                self.__data_read_first_phase(samples, file_number)

    @time_method_decorator
    def __data_read_first_phase(self, samples, file_number):
        sql_manager = SqlManager()

        bar1 = IncrementalBar(f"Reading first phase file #{file_number}", max=len(samples))

        # data reading first phase: works, authors
        for work in samples:
            # read work's DOI
            doi = work["DOI"]
            year = work["year"]
            authors = work.get('author')
            given_name = ""
            family_name = ""
            if authors:
                try:
                    sql_manager.writer.add_work(doi, year)
                    for author in authors:
                        given_name = author["given"]
                        family_name = author["family"]

                        sql_manager.writer.add_author(given_name, family_name)
                        author_id = sql_manager.reader.get_author_id(given_name, family_name)

                        if author_id:
                            sql_manager.writer.add_author_has_work(author_id, doi)
                        else:
                            print(f"Error: file {file_number}, name: {given_name}, surname: {family_name}, "
                                  f"error msg: no author id via name and family")

                except mysql.connector.Error as err:
                    print(f"Error: file {file_number}, name: {given_name}, surname: {family_name}, "
                          f"error msg: {err.msg}")
            bar1.next()
        bar1.finish()
        print()

    @time_method_decorator
    def data_read_second_phase_from_files(self, filenames, file_number_left):
        for file_number, filename in enumerate(filenames, start=file_number_left):
            with open(filename) as file:
                samples = json.load(file)['items']
                self.__data_read_second_phase(samples, file_number)

    @time_method_decorator
    def __data_read_second_phase(self, samples, file_number):
        sql_manager = SqlManager()

        bar1 = IncrementalBar(f"Reading second phase file #{file_number}", max=len(samples))
        start = time.time()
        # data reading second phase: adding refs
        for work in samples:
            work_doi = work["DOI"]
            try:
                work_authors = sql_manager.reader.get_work_authors(work_doi)
                references = work['reference']
                for src in references:
                    src_doi = src['DOI']

                    # add work cites work
                    sql_manager.writer.add_work_cites_work(work_doi, src_doi)

                    # read author_citates_author
                    src_authors = sql_manager.reader.get_work_authors(src_doi)
                    if src_authors:
                        for src_author in src_authors:
                            for work_author in work_authors:
                                sql_manager.writer.add_author_cites_author(work_author[0], src_author[0])

            except mysql.connector.Error as err:
                print(f"Error: file {file_number}, work_doi: {work_doi} "
                      f"error msg: {err.msg}")

            bar1.next()
        bar1.finish()
        print()
        second_phase_time = time.time() - start
        print(f"LOG: JsonToSqlWriter second phase time in minutes: {second_phase_time / 60}")
