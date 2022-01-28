import json
from threading import Thread

from tqdm.autonotebook import tqdm

from source.DirManager import get_filenames_in_dir
from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlReader import SqlReader
from source.SQL_interaction.SqlWriter import SqlWriter
from source.time_decorator import time_method_decorator


class JsonToSqlWriter:
    @time_method_decorator
    def write_first_phase(self, dataset_dir, threads_num):
        filenames = get_filenames_in_dir(dataset_dir)
        threads = self.__create_threads(filenames, threads_num, self.__first_phase_on_file)
        self.__proceed_threads(threads)

    @staticmethod
    def __first_phase_on_file(samples, raw_filename):
        connection = create_connection()
        sql_writer = SqlWriter(connection)
        sql_reader = SqlReader(connection)

        for work in tqdm(samples, f"Reading first phase file {raw_filename}"):
            doi = work["DOI"]
            year = work["year"]
            authors = work.get('author')
            sql_writer.add_work(doi, year)
            for author in authors:
                given_name = author["given"]
                family_name = author["family"]

                sql_writer.add_author(given_name, family_name)
                author_id = sql_reader.get_author_id(given_name, family_name)[0][0]
                sql_writer.add_author_has_work(author_id, doi)

    @time_method_decorator
    def write_second_phase(self, dataset_dir, threads_num):
        filenames = get_filenames_in_dir(dataset_dir)
        threads = self.__create_threads(filenames, threads_num, self.__second_phase_on_file)
        self.__proceed_threads(threads)

    @staticmethod
    def __second_phase_on_file(samples, raw_filename):
        connection = create_connection()
        sql_writer = SqlWriter(connection)
        sql_reader = SqlReader(connection)

        for work in tqdm(samples, f"Reading second phase file #{raw_filename}"):
            work_doi = work["DOI"]
            work_authors = sql_reader.get_work_authors(work_doi)
            references = work['reference']
            for src_doi in references:
                sql_writer.add_work_cites_work(work_doi, src_doi)

                src_authors = sql_reader.get_work_authors(src_doi)
                for src_author in src_authors:
                    for work_author in work_authors:
                        sql_writer.add_author_cites_author(work_author[0], src_author[0])

    def __create_threads(self, filenames, threads_num, target_phase):
        files_num = len(filenames)
        mod = files_num % threads_num
        div = files_num // threads_num
        threads = []

        if files_num < threads_num:
            threads_num = files_num

        # остаток распределяется между первыми mod потоками
        for i in range(mod):
            left_border = (div + 1) * i
            right_border = (div + 1) * (i + 1)
            thread_filenames = filenames[left_border:right_border]
            threads.append(Thread(target=self.__launch_phase_on_files, args=(thread_filenames, target_phase)))

        # распределение с учетом сдвига предыдущего шага
        for i in range(threads_num - mod):
            left_border = (div + 1) * mod + div * i
            right_border = (div + 1) * mod + div * (i + 1)
            thread_filenames = filenames[left_border:right_border]
            threads.append(Thread(target=target_phase, args=(thread_filenames,)))
        return threads

    @staticmethod
    def __proceed_threads(threads):
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    @staticmethod
    def __launch_phase_on_files(filenames, target_phase):
        for filename in filenames:
            with open(filename) as file:
                samples = json.load(file)['items']
                raw_filename = filename[filename.rfind('\\') + 1:]
                target_phase(samples, raw_filename)
