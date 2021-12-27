import json
from collections import Counter

from source.Graph_Processing.GraphAlgos.BronKerbosch import BronKerboschManager
from source.Graph_Processing.GraphAlgos.FindComponents import ComponentsFinder
from source.Graph_Processing.GraphPrinter import GraphPrinter
from source.Graph_Processing.SqlGraphManager import SqlGraphManager

from source.JsonToSqlWriter import JsonToSqlWriter
from source.Sql_classes.SqlProcessor import SqlProcessor

if __name__ == "__main__":
    # json_to_sql_writer = JsonToSqlWriter()
    # sql_processor = SqlProcessor()
    # filename = r"F:\Programming\PyCharmProjects\Alt_exam\dataset\romanina_test.json"

    # json_to_sql_writer.data_read_first_phase_from_files([filename], 0)
    # json_to_sql_writer.data_read_second_phase_from_files([filename], 0)


    # sql_processor.merge_authors()
    # sql_processor.citations_to_graph_table()

    # finder = ComponentsFinder()
    # finder.find_comps()

    graph_printer = GraphPrinter()
    graph_printer.save_component(2)

    # bron_kerbosch = BronKerboschManager()
    # cliques = bron_kerbosch.bron_kerbosch(2)
    # with open("cliques_in_2.txt", 'w') as file:
    #     for clique in cliques:
    #         file.write(f"{clique}\n")

    c = Counter()
    with open("cliques_in_2.txt", 'r') as file:
        for clique in file.readlines():
            clique_list = clique.rstrip('{').lstrip('}').split(',')
            c[len(clique_list)] += 1

    print(c.keys())
# file_names = [
    #     [fr'C:\Users\diest\PycharmProjects\Alt_exam\source\dataset\medicine_refs_dataset\medicine_compressed_refs_{i}.json' for i in range(j, j+10)]
    #      for j in range(5, 134, 10)
    # ]
    #
    # start = time()
    #
    # threads = [Thread(target=json_to_sql_writer.data_read_first_phase_from_files, args=(file_names[i], 5+10*i))
    #            for i in range(12)]
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

