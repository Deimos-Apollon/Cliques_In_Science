import json
from collections import Counter
from threading import Thread
from time import time, process_time

import graphviz
from progress.bar import IncrementalBar

from source.DataAnalysis import DataAnalyser
from source.Graph_Processing.GraphAlgos.BronKerbosch import BronKerboschManager
from source.Graph_Processing.GraphAlgos.FindComponents import ComponentsFinder
from source.Graph_Processing.GraphPrinter import GraphPrinter
from source.Graph_Processing.SqlGraphManager import SqlGraphManager

from source.JsonToSqlWriter import JsonToSqlWriter
from source.Json.JsonReaderWriter import JsonReaderWriter
from source.Json.JsonAnalyser import JsonAnalyser
from source.Sql_classes.SqlManager import SqlManager
from source.Sql_classes.SqlProcessor import SqlProcessor

def find_mean(directory):
    internal_mean, external_mean = 0, 0
    values_sum, values_num = 0, 0
    with open(f"{directory}\punkt_3.json") as file:
        items = json.load(file)
        for comp in items.values():
            for value in comp:
                values_sum += value
                values_num += 1
    internal_mean = values_sum / values_num

    values_sum, values_num = 0, 0
    with open(f"{directory}\punkt_5_6.json") as file:
        items = json.load(file)
        for comp in items.values():
            for value in comp:
                values_sum += value
                values_num += 1
    external_mean = values_sum / values_num
    return internal_mean, external_mean


if __name__ == "__main__":

    # src_filenames = [fr'C:\Users\user\PycharmProjects\Alt_exam\dataset\compressed_refs_dataset\compressed_refs_{i}.json'
    #                  for i in range(1036)]
    # prefix = fr'C:\Users\user\PycharmProjects\Alt_exam\dataset\mechanical_eng\mechanical_eng_'

    # jsonrw = JsonReaderWriter(src_filenames, prefix, 0, 0, 1036)
    # jsonrw.proceed()

    # subj_name = 'electrical_eng'
    # main_output = fr'C:\Users\user\PycharmProjects\Alt_exam\dataset\eltech_output\{subj_name}_main_info.txt'
    # all_subjects = fr'C:\Users\user\PycharmProjects\Alt_exam\dataset\eltech_output\{subj_name}_all_subjects.txt'
    # file_names = [fr'C:\Users\user\PycharmProjects\Alt_exam\dataset\{subj_name}\{subj_name}_{i}.json'
    #               for i in range(23)]
    # json_analyser = JsonAnalyser(file_names, 23)
    # json_analyser.proceed()
    # json_analyser.save_results(main_output, all_subjects)


    # json_to_sql_writer = JsonToSqlWriter()
    # file_names = [
    #     [fr'C:\Users\user\PycharmProjects\Alt_exam\dataset\electrical_eng\electrical_eng_{i}.json'
    #      for i in range(j, j + 5)] for j in range(0, 20, 5)
    # ]
    # file_names.append([fr'C:\Users\user\PycharmProjects\Alt_exam\dataset\electrical_eng\electrical_eng_{i}.json'
    #                    for i in range(20, 23)])
    #
    # start = time()
    #
    # threads = [Thread(target=json_to_sql_writer.data_read_second_phase_from_files, args=(file_names[i], 5 * i))
    #            for i in range(5)]
    #
    # # init and start threads
    # for thread in threads:
    #     thread.start()
    #
    # # finish threads
    # for thread in threads:
    #     thread.join()
    #
    # print('Second phase finished in minutes', (time() - start) / 60)

    # json_to_sql_writer = JsonToSqlWriter()
    # sql_processor = SqlProcessor()
    # sql_processor.citations_to_graph_table()

    # sql_processor.merge_authors()
    # filename = [fr"" for i in range(17)]
    # # json_to_sql_writer.data_read_second_phase_from_files([filename[0]], 0)
    # cliques_directory = r'C:\Users\user\PycharmProjects\Alt_exam\dataset\software_cliques'
    # data_analyser = DataAnalyser(cliques_directory)
    # # data_analyser.write_all_cliques()
    # output_directory = r'C:\Users\user\PycharmProjects\Alt_exam\output\software_output'
    # # max_size, color = data_analyser.find_largest_clique()
    # # print(max_size, color)
    # # data_analyser.punkt_3(output_directory)
    # data_analyser.punkt_5_6(output_directory)
    #
    # print(data_analyser.punkt_3("/media/deimos/Мои_файл_/PyCharmProjects/Alt_exam/data_analys_files/ant"))

    # sql_processor.merge_authors()
    #sql_processor.citations_to_graph_table()

    # finder = ComponentsFinder()
    # finder.find_comps()

    cliques_directory = fr'C:\Users\user\PycharmProjects\Alt_exam\research_cliques'
    data_analyser = DataAnalyser(cliques_directory)
    # data_analyser.write_all_cliques_coauthors()
    output_directory = r'C:\Users\user\PycharmProjects\Alt_exam\output\research'
    # data_analyser.save_internal_citing_coef(output_directory)
    data_analyser.save_external_citing_coef(output_directory)

    # cliq_res_dir = r'C:\Users\user\PycharmProjects\Alt_exam\output\el_tech_output\without_coauthors'
    # cliq_coauth_res_dir = r'C:\Users\user\PycharmProjects\Alt_exam\output\el_tech_output\with_coauthors'
    # print(find_mean(cliq_res_dir))
    # print(find_mean(cliq_coauth_res_dir))
    # data_analyser.save_largest_clique(output_directory)
    # data_analyser.save_internal_citing_coef(output_directory)
    #data_analyser.save_external_citing_coef(output_directory)
    #


    # graph_printer = GraphPrinter()
    # graph_printer.save_graph()
