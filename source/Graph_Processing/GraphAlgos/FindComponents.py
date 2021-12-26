
from source.Graph_Processing.SqlGraphManager import SqlGraphManager
from collections import defaultdict


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
        self.vertices_not_visited_num -= 1

        queue = [vertex]
        while queue:
            vertex = queue[0]
            for child, edge_id in self.incidence_lists[vertex]:
                if child in self.vertices_not_visited:
                    self.sql_graph_manager.graph_writer.add_edge_in_component(current_color, edge_id)
                    if child not in queue:
                        queue.append(child)
            queue.pop(0)
            self.vertices_not_visited.remove(vertex)
