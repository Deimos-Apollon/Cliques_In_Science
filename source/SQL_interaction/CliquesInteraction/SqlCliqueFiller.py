from tqdm.autonotebook import tqdm
from source.Graph.GraphAlgorithms.BronKerbosch import BronKerboschManager
from source.SQL_interaction.CliquesInteraction.SqlCliqueAnalyser import SqlCliqueAnalyser
from source.SQL_interaction.CliquesInteraction.SqlCliqueReader import SqlCliqueReader
from source.SQL_interaction.Create_connection import create_connection
from source.SQL_interaction.SqlGraphReader import SqlGraphReader
from source.SQL_interaction.CliquesInteraction.SqlCliqueWriter import SqlCliqueWriter


class SqlCliqueFiller:
    def __init__(self):
        self.__bk_manager = BronKerboschManager()
        connection = create_connection()
        self.__graph_reader = SqlGraphReader(connection)
        self.__clique_writer = SqlCliqueWriter(connection)
        self.__clique_reader = SqlCliqueReader(connection)
        self.__clique_analyser = SqlCliqueAnalyser(connection)

    def fill_cliques_authors(self):
        if self.__is_clique_in_bd_empty(0):
            components = self.__graph_reader.get_components()
            for component in tqdm(components, "Writing Cliques"):
                component = component[0]
                cliques_authors = self.__bk_manager.bron_kerbosch(component)
                for clique_authors in cliques_authors:
                    self.__clique_writer.write_clique(component, clique_authors, 0)

    def fill_cliques_coauthors(self):
        if self.__is_clique_in_bd_empty(1):
            components = self.__graph_reader.get_components()
            for component in tqdm(components, "Writing coauthors cliques"):
                component = component[0]
                cliques = self.__bk_manager.bron_kerbosch_coauthors(component)
                for clique_authors in cliques:
                    self.__clique_writer.write_clique(component, clique_authors, 1)

    def update_cliques_citing(self, surely_coauthors):
        clique_ids = self.__clique_reader.get_cliques(surely_coauthors)
        for clique in tqdm(clique_ids, "Filling citing coefs"):
            clique = clique[0]
            self.__change_clique_citing_coefs(clique)

    def __is_clique_in_bd_empty(self, surely_coauthors):
        cliques = self.__clique_reader.get_cliques(surely_coauthors)
        return len(cliques) == 0

    def __change_clique_citing_coefs(self, clique_id):
        internal = self.__clique_analyser.get_internal_citing(clique_id)
        external = self.__clique_analyser.get_external_citing(clique_id)
        self.__clique_writer.change_citing_in_clique(clique_id, internal, external)
