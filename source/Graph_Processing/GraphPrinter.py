import graphviz
from progress.bar import IncrementalBar

from source.Graph_Processing.SqlGraphReader import SqlGraphReader


class GraphPrinter:
    def __init__(self):
        self.graph_reader = SqlGraphReader()

    def save_graph(self):
        g = graphviz.Graph('G', filename='test_graph.gv')
        edges = self.graph_reader.get_edges()
        bar = IncrementalBar("SavingGraph", max=len(edges))
        bar.start()
        if len(edges) > 20000:
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
        edges = self.graph_reader.get_unique_component_edges(component_color)
        bar = IncrementalBar("SavingGraph", max=len(edges))
        bar.start()

        edges = list(edges)
        for elem in edges:
            g.edge(f"{elem[0]}", f"{elem[1]}")
            bar.next()
        bar.finish()
        g.save()
