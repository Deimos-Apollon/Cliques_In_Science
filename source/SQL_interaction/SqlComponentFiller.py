from tqdm.notebook import tqdm

from collections import defaultdict

from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlGraphReader import SqlGraphReader

from source.SQL_interaction.SqlWriter import SqlWriter


class SqlComponentFiller:
    def __init__(self):
        connection = create_connection()
        self.__sql_writer = SqlWriter(connection)
        self.__graph_reader = SqlGraphReader()
        self.__incidence_lists = self.__graph_reader.get_incidence_lists()
        self.__vertices_not_visited = set(self.__incidence_lists.keys())
        self.__comps = defaultdict(set)
        self.__vertices_not_visited_num = len(self.__vertices_not_visited)

    def find_comps(self):
        current_color = 1
        bar = tqdm("Finding components", total=self.__vertices_not_visited_num)
        while self.__vertices_not_visited:
            before = self.__vertices_not_visited_num
            self.__sql_writer.add_component(current_color)
            self.__bfs(current_color)
            current_color += 1
            bar.update(before - self.__vertices_not_visited_num)
        bar.close()

    def __bfs(self, current_color):
        vertex = self.__vertices_not_visited.pop()
        self.__vertices_not_visited.add(vertex)
        queue = [vertex]
        while queue:
            vertex = queue[0]
            for child, edge_id in self.__incidence_lists[vertex]:
                if child in self.__vertices_not_visited:
                    co_id = self.__find_co_edge(vertex, child)
                    if co_id:
                        self.__sql_writer.add_edge_in_component(current_color, edge_id)
                        if co_id != edge_id:
                            self.__sql_writer.add_edge_in_component(current_color, co_id)
                    else:
                        print("ERROR: нет co_id")
                    if child not in queue:
                        queue.append(child)
            queue.pop(0)
            self.__vertices_not_visited.remove(vertex)
            self.__vertices_not_visited_num -= 1

    def __find_co_edge(self, vertex, child):
        for elem in self.__incidence_lists[child]:
            if elem[0] == vertex:
                return elem[1]
        return None
