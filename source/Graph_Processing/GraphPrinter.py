import graphviz

class GraphPrinter:
    def __init__(self):
        self.sql_graph_reader = SqlGraphReader()

    def save_graph(self):
        g = graphviz.Graph('G', filename='process.gv')

        g.edge("1", "2")
        g.edge("2", "3")
        g.edge("1", "4")
        g.edge("3", "5")

        g.view()
