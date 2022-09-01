from source.SQL_interaction.CliquesInteraction.SqlCliqueWriter import SqlCliqueWriter
from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlGraphReader import SqlGraphReader


class BronKerboschManager:
    def __init__(self):
        connection = create_connection()
        self.graph_reader = SqlGraphReader(connection)
        self.__clique_writer = SqlCliqueWriter(connection)
        self.incidence_lists = {}
        self.compsub = set()

    def bron_kerbosch(self, component_color, surely_coauthors):
        self.incidence_lists = self.graph_reader.get_component_incidence_lists_coauthors(component_color) \
            if surely_coauthors else self.graph_reader.get_component_incidence_lists(component_color)
        self.__bron_kerbosch(component_color, surely_coauthors)

    def __bron_kerbosch(self, component_color, surely_coauthors):
        if self.incidence_lists:
            candidates = set(self.incidence_lists.keys())
        else:
            return []
        self.compsub = set()
        vert_not = set()

        # extend 0 recurs level
        while candidates and self.__check_is_adjacent_with_all_candidates(vert_not, candidates):
            # 1
            vertex = candidates.pop()
            candidates.add(vertex)
            self.compsub.add(vertex)
            # 2
            new_candidates, new_vertices_not = self.__get_new_candidates_and_vertices_not(candidates, vert_not, vertex)
            # 3
            if not (new_candidates or new_vertices_not):
                # 4
                # self.clique.append(self.compsub.copy())
                self.__clique_writer.write_clique(component_color, self.compsub, surely_coauthors)
            # 5
            else:
                self.__extend(new_candidates, new_vertices_not, component_color, surely_coauthors)
            # 6
            self.compsub.remove(vertex)
            candidates.remove(vertex)
            vert_not.add(vertex)

    def __extend(self, candidates, vert_not, component_color, surely_coauthors):
        while candidates and self.__check_is_adjacent_with_all_candidates(vert_not, candidates):
            # 1
            vertex = candidates.pop()
            candidates.add(vertex)
            self.compsub.add(vertex)
            # 2
            new_candidates, new_vertices_not = self.__get_new_candidates_and_vertices_not(candidates, vert_not, vertex)
            # 3
            if not (new_candidates or new_vertices_not):
                # 4
                self.__clique_writer.write_clique(component_color, self.compsub, surely_coauthors)
            # 5
            else:
                self.__extend(new_candidates, new_vertices_not, component_color, surely_coauthors)
            # 6
            self.compsub.remove(vertex)
            candidates.remove(vertex)
            vert_not.add(vertex)

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
