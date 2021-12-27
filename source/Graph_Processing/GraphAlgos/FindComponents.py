from progress.bar import IncrementalBar

from source.Graph_Processing.SqlGraphManager import SqlGraphManager
from collections import defaultdict

from source.Sql_classes.SqlManager import SqlManager


class ComponentsFinder:
    def __init__(self):
        self.sql_graph_manager = SqlGraphManager()

        self.incidence_lists = self.sql_graph_manager.graph_reader.get_incidence_lists()
        self.vertices_not_visited = set(self.incidence_lists.keys())
        self.comps = defaultdict(set)
        self.vertices_not_visited_num = len(self.vertices_not_visited)

    def find_comps(self):
        current_color = 1

        print(f"START: Let's find components! all: {current_color - 1}, vertices left: {self.vertices_not_visited_num}")
        while self.vertices_not_visited:
            self.bfs(current_color)
            current_color += 1
            print(f"CONGRATS: +1 Component, all: {current_color - 1}, vertices left: {self.vertices_not_visited_num}")
        return self.comps

    def bfs(self, current_color):
        vertex = self.vertices_not_visited.pop()
        self.vertices_not_visited.add(vertex)
        bar = IncrementalBar("Processing in bfs", max=self.vertices_not_visited_num)
        queue = [vertex]
        while queue:
            vertex = queue[0]
            for child, edge_id in self.incidence_lists[vertex]:
                if child in self.vertices_not_visited:
                    co_id = self.__find_co_edge(vertex, child)
                    if co_id:
                        self.sql_graph_manager.graph_writer.add_edge_in_component(current_color, edge_id)
                        if co_id != edge_id:
                            self.sql_graph_manager.graph_writer.add_edge_in_component(current_color, co_id)
                    else:
                        print("ERROR: нет co_id")
                    if child not in queue:
                        queue.append(child)
            queue.pop(0)
            self.vertices_not_visited.remove(vertex)
            self.vertices_not_visited_num -= 1
            bar.next()
        bar.finish()

    def __find_co_edge(self, vertex, child):
        for elem in self.incidence_lists[child]:
            if elem[0] == vertex:
                return elem[1]
        return None
