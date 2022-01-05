import json
from collections import Counter
from threading import Thread
from time import time, process_time

from source.DataAnalysis import DataAnalyser
from source.Graph_Processing.GraphAlgos.BronKerbosch import BronKerboschManager
from source.Graph_Processing.GraphAlgos.FindComponents import ComponentsFinder
from source.Graph_Processing.GraphPrinter import GraphPrinter
from source.Graph_Processing.SqlGraphManager import SqlGraphManager

from source.JsonToSqlWriter import JsonToSqlWriter
from source.Sql_classes.SqlProcessor import SqlProcessor

if __name__ == "__main__":
    json_to_sql_writer = JsonToSqlWriter()
    sql_processor = SqlProcessor()
    filename = [fr"" for i in range(17)]

    # json_to_sql_writer.data_read_second_phase_from_files([filename[0]], 0)

    data_analyser = DataAnalyser("/media/deimos/Мои_файл_/PyCharmProjects/Alt_exam/cliques/ant")
    max_size, color = data_analyser.find_largest_clique()
    print(max_size, color)

    print(data_analyser.punkt_3("/media/deimos/Мои_файл_/PyCharmProjects/Alt_exam/data_analys_files/ant"))

    # sql_processor.merge_authors()
    #sql_processor.citations_to_graph_table()

    # finder = ComponentsFinder()
    # finder.find_comps()

    # graph_printer = GraphPrinter()
    # graph_printer.save_graph()
