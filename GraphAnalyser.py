import graphviz


if __name__ == "__main__":
    g = graphviz.Graph('G', filename='process.gv')

    g.edge("1", "2")
    g.edge("2", "3")
    g.edge("1", "4")
    g.edge("3", "5")

    g.view()
