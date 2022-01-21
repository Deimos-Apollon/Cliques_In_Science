import json
from collections import Counter
from threading import Thread
from time import time, process_time

import graphviz
from progress.bar import IncrementalBar

from source.DataAnalysis import DataAnalyser
from source.Graph_Processing.GraphAlgos.BronKerbosch import BronKerboschManager
from source.Graph_Processing.GraphPrinter import GraphPrinter

from source.JsonToSqlWriter import JsonToSqlWriter
from source.Json.JsonReaderWriter import JsonReaderWriter
from source.Json.JsonAnalyser import JsonAnalyser
from source.Sql_classes.SqlManager import SqlManager
from source.Sql_classes.SqlProcessor import SqlProcessor


if __name__ == "__main__":
    subj_name = "Logic"
    sql_manager = SqlManager()
    json_to_sql = JsonToSqlWriter()
    src_filename = (r'C:\Users\user\PycharmProjects\Alt_exam\dataset\logic\logic_0.json',)
    # json_to_sql.data_read_first_phase_from_files(src_filename, 0)
    # json_to_sql.data_read_second_phase_from_files(src_filename, 0)

    proc = SqlProcessor()

    proc.find_comps()


