import json
from collections import Counter

from source.Graph_Processing.GraphAlgos.BronKerbosch import BronKerboschManager
from source.Graph_Processing.GraphAlgos.FindComponents import ComponentsFinder
from source.Graph_Processing.GraphPrinter import GraphPrinter
from source.Graph_Processing.SqlGraphManager import SqlGraphManager

from source.JsonToSqlWriter import JsonToSqlWriter
from source.Sql_classes.SqlProcessor import SqlProcessor

if __name__ == "__main__":
    json_to_sql_writer = JsonToSqlWriter()

    filename = r"F:\Programming\PyCharmProjects\Alt_exam\dataset\medicine_refs_dataset\medicine_compressed_refs_0.json"

    json_to_sql_writer.data_read_second_phase_from_files([filename], 0)


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


