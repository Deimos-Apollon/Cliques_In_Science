from source.Graph_Processing.SqlGraphReader import SqlGraphReader
from collections import defaultdict

class ComponentsFinder:
    def __init__(self):
        self.sql_graph_reader = SqlGraphReader()
        self.vertices_not_visited = self.sql_graph_reader.get_all_vertices()
        self.incidence_lists = self.sql_graph_reader.get_incidence_lists()
        self.comps = defaultdict(set)

    def find_comps(self):
        current_color = 1
        while self.vertices_not_visited:
            vertex = self.vertices_not_visited.pop()
            self.comps[current_color].add(vertex)
            self.dfs(vertex, current_color)
            current_color += 1
        return self.comps

    def dfs(self, vertex, current_color):
        for child in self.incidence_lists[vertex]:
            if child in self.vertices_not_visited:
                self.vertices_not_visited.remove(child)
                self.comps[current_color].add(child)
                self.dfs(child, current_color)