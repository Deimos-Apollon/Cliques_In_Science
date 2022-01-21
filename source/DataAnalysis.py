import json
from collections import defaultdict
from progress.bar import IncrementalBar
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
                with open(fr"{self.directory}\cliques_in_{comp_color}.txt", 'w') as file:
                    for clique in cliques:
                        file.write(str(clique))
                        file.write("\n")

    def write_all_cliques_coauthors(self):
        get_query = fr'''
                        SELECT component_color FROM component ORDER BY component_color DESC LIMIT 1
                   '''
        component_last_color = self.sql_manager.reader.execute_get_query(get_query)[0][0]
        bron_kerbosch = BronKerboschManager()
        for comp_color in range(1, component_last_color + 1):
            cliques = bron_kerbosch.bron_kerbosch_coauthors(comp_color)
            if cliques and (len(cliques) >= 2 or (len(cliques) == 1 and len(cliques[0]) > 2)):
                with open(fr"{self.directory}\cliques_in_{comp_color}.txt", 'w') as file:
                    for clique in cliques:
                        file.write(str(clique))
                        file.write("\n")

    def save_largest_clique(self, output_directory):
        max_clique_size, max_clique_size_component, max_clique_elems = 0, 0, []
        for filename in os.listdir(self.directory):
            with open(fr"{self.directory}/{filename}", 'r') as file:
                for line in file.readlines():
                    clique_elems = (line.strip('{').strip('}\n')).split(',')
                    clique_size = len(clique_elems)
                    if clique_size > max_clique_size:
                        max_clique_size = clique_size
                        max_clique_size_component = filename.split('_')[2][0:-4]
                        max_clique_elems = clique_elems
        for i in range(len(max_clique_elems)):
            max_clique_elems[i] = int(max_clique_elems[i])
        with open(fr"{output_directory}\max_clique.json", 'w') as file:
            file.write('size: ' + str(max_clique_size) + '\n')
            file.write('color: ' + str(max_clique_size_component) + '\n')
            file.write('clique: ')
            file.write(str(max_clique_elems))

    def count_internal_links_authors(self, clique_authors):
        total_refs = 0
        for author in clique_authors:
            author_works = self.sql_manager.reader.get_author_works(author)
            for work in author_works:
                work = work[0]
                src_works = self.sql_manager.reader.get_work_references(work)
                for src_work in src_works:
                    src_work = src_work[0]
                    work_authors = self.sql_manager.reader.get_work_authors(src_work)
                    for work_author in work_authors:
                        work_author = work_author[0]
                        if work_author in clique_authors and work_author != author:
                            total_refs += 1
        return total_refs

    def count_internal_links_works(self, clique_authors):
        total_refs = 0
        clique_works = set()

        for author in clique_authors:
            for author_works in self.sql_manager.reader.get_author_works(author):
                for work in author_works:
                    clique_works.add(work.lower())

        for work in clique_works:
            work_refs = self.sql_manager.reader.get_work_references(work)
            for work_ref in work_refs:
                work_ref = work_ref[0].lower()
                if work_ref in clique_works:
                    if work_ref != work:
                        total_refs += 1

        return total_refs

    def save_internal_citing_coef(self, output_directory):
        coefs = defaultdict(list)
        bar = IncrementalBar(max=len(os.listdir(self.directory)))
        for filename in os.listdir(self.directory):
            with open(fr"{self.directory}/{filename}", 'r') as file:
                for line in file.readlines():
                    clique_authors = list(map(int, (line.strip('{').strip('}\n')).split(',')))
                    total_refs = self.count_internal_links_authors(clique_authors)
                    clique_coef = total_refs / len(clique_authors) ** 2
                    comp_color = filename.split('_')[2][0:-4]
                    coefs[comp_color].append(clique_coef)
            bar.next()
        bar.finish()

        with open(fr"{output_directory}/punkt_3.json", 'w') as file:
            json.dump(coefs, file, indent=4)

    def save_external_citing_coef(self, output_directory):
        coefs = defaultdict(list)
        bar = IncrementalBar(max=len(os.listdir(self.directory)))
        min_coef, min_coef_comp_color, min_coef_clique_row = 1000, 0, 0
        max_coef, max_coef_comp_color, max_coef_clique_row = 0, 0, 0
        for filename in os.listdir(self.directory):
            with open(fr"{self.directory}/{filename}", 'r') as file:
                clique_row = 0
                for line in file.readlines():
                    clique_row += 1
                    clique_authors = list(map(int, (line.strip('{').strip('}\n')).split(',')))
                    clique_works_refs = {}
                    internal_links_num = self.count_internal_links_works(clique_authors)

                    for author in clique_authors:
                        author_works = self.sql_manager.reader.get_author_works(author)
                        for work in author_works:
                            work = work[0]
                            if work not in clique_works_refs:
                                clique_works_refs[work] = max(self.sql_manager.reader.get_work_is_referenced_count(work),
                                                              self.sql_manager.reader.get_work_is_referenced_count_in_db(work))

                    clique_coef = (sum(clique_works_refs.values()) - internal_links_num) / len(clique_works_refs.keys())
                    comp_color = filename.split('_')[2][0:-4]
                    coefs[comp_color].append(clique_coef)
                    if clique_coef < min_coef:
                        min_coef = clique_coef
                        min_coef_comp_color = comp_color
                        min_coef_clique_row = clique_row
                    if clique_coef > max_coef:
                        max_coef = clique_coef
                        max_coef_comp_color = comp_color
                        max_coef_clique_row = clique_row
            bar.next()
        bar.finish()

        with open(fr"{output_directory}/punkt_5_6.json", 'w') as file:
            json.dump(coefs, file, indent=4)

        with open(fr"{output_directory}/punkt_5.json", 'w') as file:
            items = {'Max coef': max_coef, 'Component color': max_coef_comp_color,
                     'Clique row': max_coef_clique_row}
            json.dump(items, file, indent=4)

        with open(fr"{output_directory}/punkt_6.json", 'w') as file:
            items = {'Min coef': min_coef, 'Component color': min_coef_comp_color,
                     'Clique row': min_coef_clique_row}
            json.dump(items, file, indent=4)

    def find_mean(self, directory):
        internal_mean, external_mean = 0, 0
        values_sum, values_num = 0, 0
        with open(f"{directory}\punkt_3.json") as file:
            items = json.load(file)
            for comp in items.values():
                for value in comp:
                    values_sum += value
                    values_num += 1
        internal_mean = values_sum / values_num

        values_sum, values_num = 0, 0
        with open(f"{directory}\punkt_5_6.json") as file:
            items = json.load(file)
            for comp in items.values():
                for value in comp:
                    values_sum += value
                    values_num += 1
        external_mean = values_sum / values_num
        return internal_mean, external_mean