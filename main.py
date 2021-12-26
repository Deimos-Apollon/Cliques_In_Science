import json
from collections import Counter

from source.Graph_Processing.GraphAlgos.FindComponents import ComponentsFinder
from source.Graph_Processing.GraphPrinter import GraphPrinter
from source.Graph_Processing.SqlGraphManager import SqlGraphManager

from source.JsonToSqlWriter import JsonToSqlWriter
from source.Sql_classes.SqlProcessor import SqlProcessor

if __name__ == "__main__":
    json_to_sql_writer = JsonToSqlWriter()
    sql_processor = SqlProcessor()
    filename = r"F:\Programming\PyCharmProjects\Alt_exam\dataset\romanina_test.json"

    finder = ComponentsFinder()


    # json_to_sql_writer.data_read_first_phase_from_files([filename], 0)
    # json_to_sql_writer.data_read_second_phase_from_files([filename], 0)


    # sql_processor.merge_authors()
    # sql_processor.citations_to_graph_table()

    # finder.find_comps()

    graph_printer = GraphPrinter()
    graph_printer.save_component(8)



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

