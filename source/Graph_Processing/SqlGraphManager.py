from source.Graph_Processing.SqlGraphReader import SqlGraphReader
from source.Graph_Processing.SqlGraphWriter import SqlGraphWriter
from source.Sql_classes.SqlManager import SqlManager


class SqlGraphManager:
    def __init__(self):
        self.__sql_manager__ = SqlManager()
        self.graph_reader = SqlGraphReader(self.__sql_manager__)
        self.graph_writer = SqlGraphWriter(self.__sql_manager__)
