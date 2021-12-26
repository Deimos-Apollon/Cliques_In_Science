from source.Sql_classes.SqlManager import SqlManager


class SqlGraphWriter:
    def __init__(self, sql_manager):
        self.sql_manager = sql_manager

    def add_edge_in_component(self, component_color, edge_ID):
        add_query = fr'''
                        INSERT INTO Component (Component_color, Graph_edge_ID) VALUES ({component_color}, {edge_ID})
                    '''
        self.sql_manager.writer.execute_query(add_query)

