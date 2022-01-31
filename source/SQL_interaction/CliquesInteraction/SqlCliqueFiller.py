from tqdm.autonotebook import tqdm
from source.Graph.GraphAlgorithms.BronKerbosch import BronKerboschManager
from source.SQL_interaction.CliquesInteraction.SqlCliqueReader import SqlCliqueReader
from source.SQL_interaction.SqlGraphReader import SqlGraphReader
from source.SQL_interaction.CliquesInteraction.SqlCliqueWriter import SqlCliqueWriter


class SqlCliqueFiller:
    def __init__(self):
        self.__bk_manager = BronKerboschManager()
        self.__graph_reader = SqlGraphReader()
        self.__clique_writer = SqlCliqueWriter()
        self.__clique_reader = SqlCliqueReader()

    def fill_cliques_authors(self):
        if self.is_clique_in_bd_empty(0):
            components = self.__graph_reader.get_components()
            for component in tqdm(components):
                component = component[0]
                cliques = self.__bk_manager.bron_kerbosch(component)
                for clique_authors in cliques:
                    self.__clique_writer.write_clique(component, clique_authors, 0)

    def is_clique_in_bd_empty(self, surely_coauthors):
        cliques = self.__clique_reader.get_cliques(surely_coauthors)
        return len(cliques) == 0

    def fill_cliques_coauthors(self):
        if self.is_clique_in_bd_empty(1):
            components = self.__graph_reader.get_components()
            for component in tqdm(components):
                component = component[0]
                cliques = self.__bk_manager.bron_kerbosch_coauthors(component)

                for clique_authors in cliques:
                    self.__clique_writer.write_clique(component, clique_authors, 1)
