import json
from collections import Counter
from threading import Thread
from time import time, process_time

from source.Graph_Processing.GraphAlgos.BronKerbosch import BronKerboschManager
from source.Graph_Processing.GraphAlgos.FindComponents import ComponentsFinder
from source.Graph_Processing.GraphPrinter import GraphPrinter
from source.Graph_Processing.SqlGraphManager import SqlGraphManager

from source.JsonToSqlWriter import JsonToSqlWriter
from source.Sql_classes.SqlProcessor import SqlProcessor

if __name__ == "__main__":
    json_to_sql_writer = JsonToSqlWriter()

    filename = [fr"F:\Programming\PyCharmProjects\Alt_exam\dataset\astronomy\astronomy_compressed_refs_{i}.json"
                for i in range(17)]

    # json_to_sql_writer.data_read_second_phase_from_files([filename[0]], 0)

    threads = [Thread(target=json_to_sql_writer.data_read_second_phase_from_files, args=(filename[0:6], 0)),
               Thread(target=json_to_sql_writer.data_read_second_phase_from_files, args=(filename[6:12], 6)),
               Thread(target=json_to_sql_writer.data_read_second_phase_from_files, args=(filename[12:17], 12))
               ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # sql_processor.merge_authors()
    # sql_processor.citations_to_graph_table()

    # finder = ComponentsFinder()
    # finder.find_comps()

    # graph_printer = GraphPrinter()
    # graph_printer.save_component(8)

    # bron_kerbosch = BronKerboschManager()
    # cliques = []
    # with open("cliques_in_8.txt", 'r') as file:
    #     for clique in file.readlines():
    #         cliques.append(clique)
