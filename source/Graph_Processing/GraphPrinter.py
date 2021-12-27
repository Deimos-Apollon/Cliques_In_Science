import graphviz
from progress.bar import IncrementalBar

from source.Graph_Processing.SqlGraphManager import SqlGraphManager

class GraphPrinter:
    def __init__(self):
        self.sql_graph_manager = SqlGraphManager()

    def save_graph(self):
        g = graphviz.Graph('G', filename='test_graph.gv')
        edges = self.sql_graph_manager.graph_reader.get_edges()
        bar = IncrementalBar("SavingGraph", max=len(edges))
        bar.start()
        if len(edges) > 10000:
            print('ERR: too big graph!')
            return
        edges = list(edges)
        for elem in edges:
            g.edge(f"{elem[0]}", f"{elem[1]}")
            bar.next()
        bar.start()
        g.view()

    def save_component(self, component_color):
        g = graphviz.Digraph('G', filename=f'component_{component_color}.gv')
        edges = self.sql_graph_manager.graph_reader.get_component_edges(component_color)
        bar = IncrementalBar("SavingGraph", max=len(edges))
        bar.start()

        edges = list(edges)
        for elem in edges:
            g.edge(f"{elem[0]}", f"{elem[1]}")
            bar.next()
        bar.finish()
        g.save()
