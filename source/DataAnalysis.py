import json
from collections import defaultdict

from source.Graph_Processing.GraphAlgos.BronKerbosch import BronKerboschManager
from source.Sql_classes.SqlManager import SqlManager
from source.Sql_classes.SqlProcessor import SqlProcessor

import os


class DataAnalyser:
    def __init__(self, cliques_directory):
        self.sql_manager = SqlManager()
        self.directory = cliques_directory

    def write_all_cliques(self):
        get_query = fr'''
                        SELECT component_color FROM component ORDER BY component_color DESC LIMIT 1
                   '''
        component_last_color = self.sql_manager.reader.execute_get_query(get_query)[0][0]
        bron_kerbosch = BronKerboschManager()
        for comp_color in range(1, component_last_color + 1):
            cliques = bron_kerbosch.bron_kerbosch(comp_color)
            if len(cliques) >= 2 or (len(cliques) == 1 and len(cliques[0]) > 2):
                with open(fr"{self.directory}/cliques_in_{comp_color}.txt", 'w') as file:
                    for clique in cliques:
                        file.write(str(clique))
                        file.write("\n")

    def find_largest_clique(self):
        """
        :return: max_size, comp_color
        """
        max_clique_size, max_clique_size_component = 0, 0
        for filename in os.listdir(self.directory):
            with open(fr"{self.directory}/{filename}", 'r') as file:
                for line in file.readlines():
                    clique_elems = (line.strip('{').strip('}\n')).split(',')
                    clique_size = len(clique_elems)
                    if clique_size > max_clique_size:
                        max_clique_size = clique_size
                        max_clique_size_component = filename.split('_')[2][0:-4]
        return max_clique_size, int(max_clique_size_component)

    def punkt_3(self, output_directory):
        coefs = defaultdict(list)
        for filename in os.listdir(self.directory):
            with open(fr"{self.directory}/{filename}", 'r') as file:
                for line in file.readlines():
                    clique_authors = list(map(int, (line.strip('{').strip('}\n')).split(',')))
                    total_refs = 0
                    for author in clique_authors:
                        author_works = self.sql_manager.reader.get_author_works(author)
                        for work in author_works:
                            work = work[0]
                            src_works = self.sql_manager.reader.get_references_dois_of_work(work)
                            for src_work in src_works:
                                src_work = src_work[0]
                                work_authors = self.sql_manager.reader.get_authors_of_work(src_work)
                                for work_author in work_authors:
                                    work_author = work_author[0]
                                    if work_author in clique_authors and work_author != author:
                                        total_refs += 1
                    clique_coef = total_refs / len(clique_authors)**2
                    comp_color = filename.split('_')[2][0:-4]
                    coefs[comp_color].append(clique_coef)

        with open(fr"{output_directory}/punkt_3.json", 'w') as file:
            json.dump(coefs, file, indent=4)
