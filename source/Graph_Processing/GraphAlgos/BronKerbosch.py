import sys

from progress.bar import IncrementalBar

from source.Graph_Processing.SqlGraphManager import SqlGraphManager


class BronKerboschManager:
    def __init__(self):
        self.sql_graph_manager = SqlGraphManager()
        self.incidence_lists = []
        self.candidates = set()
        self.compsub = set()
        self.vertices_not = set()
        self.clique = []

    def bron_kerbosh(self, component_color):
        self.incidence_lists = self.sql_graph_manager.\
            graph_reader.get_component_incidence_lists(component_color)
        return self.__bron_kerbosch()

    def bron_kerbosh_coauthors(self, component_color):
        self.incidence_lists = self.sql_graph_manager.\
            graph_reader.get_component_incidence_lists_coauthors(component_color)
        return self.__bron_kerbosch()

    def __bron_kerbosch(self):
        if self.incidence_lists:
            candidates = set(self.incidence_lists.keys())
        else:
            return None
        self.compsub = set()
        vertices_not = set()
        self.clique = []

        bar = IncrementalBar("Bron_Kerbosch", max=len(candidates))
        # extend 0 recurs level
        bar.start()
        while candidates and self.__check_is_adjacent_with_all_candidates(vertices_not, candidates):
            bar.next()
            # 1
            vertex = candidates.pop()
            candidates.add(vertex)
            self.compsub.add(vertex)
            # 2
            new_candidates, new_vertices_not = self.__get_new_candidates_and_vertices_not(candidates, vertices_not, vertex)
            # 3
            if not (new_candidates or new_vertices_not):
                # 4
                self.clique.append(self.compsub.copy())
            # 5
            else:
                self.__extend(new_candidates, new_vertices_not)
            # 6
            self.compsub.remove(vertex)
            candidates.remove(vertex)
            vertices_not.add(vertex)

        bar.finish()
        return self.clique

    def __extend(self, candidates, vertices_not):
        while candidates and self.__check_is_adjacent_with_all_candidates(vertices_not, candidates):
            # 1
            vertex = candidates.pop()
            candidates.add(vertex)
            self.compsub.add(vertex)
            # 2
            new_candidates, new_vertices_not = self.__get_new_candidates_and_vertices_not(candidates, vertices_not, vertex)
            # 3
            if not (new_candidates or new_vertices_not):
                # 4
                self.clique.append(self.compsub.copy())
            # 5
            else:
                self.__extend(new_candidates, new_vertices_not)
            # 6
            self.compsub.remove(vertex)
            candidates.remove(vertex)
            vertices_not.add(vertex)

    def __check_is_adjacent_with_all_candidates(self, vertices_not, candidates):
        for vertex_not in vertices_not:
            adjacent_verts = set(elem[0] for elem in self.incidence_lists[vertex_not])
            if not candidates.difference(adjacent_verts):
                return False
        return True

    def __get_new_candidates_and_vertices_not(self, candidates, vertices_not, vertex):
        adjacent_to_vertex = set(elem[0] for elem in self.incidence_lists[vertex])
        if vertex in adjacent_to_vertex:
            adjacent_to_vertex.remove(vertex)
        new_candidates = candidates.intersection(adjacent_to_vertex)
        new_vertices_not = vertices_not.intersection(adjacent_to_vertex)
        return new_candidates, new_vertices_not