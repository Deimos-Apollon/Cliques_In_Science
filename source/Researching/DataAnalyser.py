from numpy.random import randint
from tqdm.notebook import tqdm

from source.SQL_interaction.CliquesInteraction.SqlCliqueReader import SqlCliqueReader
from source.SQL_interaction.Create_connection import create_connection

from source.SQL_interaction.SqlReader import SqlReader


class DataAnalyser:
    def __init__(self):
        connection = create_connection()
        self.sql_reader = SqlReader(connection)
        self.clique_reader = SqlCliqueReader(connection)

    def get_authors_distribution(self):
        """
        Распределение кол-ва публикующихся  в году авторов
        :return: словарь год: кол-во авторов
        """
        author_distribution = {year: 0 for year in range(2000, 2021)}
        authors = self.sql_reader.get_all_authors()
        for author in tqdm(authors, "Collecting authors' info"):
            author_years = {year: 0 for year in range(2000, 2021)}
            author = author[0]
            author_works = self.sql_reader.get_author_works(author)
            for work in author_works:
                work = work[0]
                year = self.sql_reader.get_work_year(work)[0][0]
                if year >= 2000:
                    author_years[year] = 1
            for year in author_years:
                author_distribution[year] += author_years[year]

        return author_distribution

    def get_work_distribution(self):
        """
        Распределение работ по годам
        :return: словарь год: кол-во работ
        """
        get_years_query = fr'''
            SELECT year FROM work
        '''
        years_from_works = self.sql_reader.execute_get_query(get_years_query)
        years_distribution = {i: 0 for i in range(2000, 2021)}
        for work_year in tqdm(years_from_works, "Collecting works' info"):
            work_year = work_year[0]
            if work_year >= 2000:
                years_distribution[work_year] += 1
        return years_distribution

    def get_work_authors_num_distribution(self):
        """
        Среднее количество авторов у работы по годам
        :return: словарь год: кол-во работ
        """
        get_years_query = fr'''
            SELECT DOI, year from work
        '''
        works_dois = self.sql_reader.execute_get_query(get_years_query)
        authors_distribution = {year: 0 for year in range(2000, 2021)}

        for work_doi, year in tqdm(works_dois, "Collecting works' authors num info"):
            count_query = fr'''
                SELECT COUNT(id) from author_has_work where Work_DOI = '{work_doi}'
            '''
            work_authors_num = self.sql_reader.execute_get_query(count_query)[0][0]
            if year >= 2000:
                authors_distribution[year] += work_authors_num
        works_distribution = self.get_work_distribution()
        mean_author_distribution = {year: 0 for year in range(2000, 2021)}
        for year in authors_distribution:
            mean_author_distribution[year] = authors_distribution[year] / works_distribution[year]
        return mean_author_distribution

    def get_max_clique_sizes(self, max_num, surely_coauthors):
        get_query = fr'''
            SELECT id, size FROM clique where surely_coauthors={surely_coauthors}
            ORDER BY size DESC LIMIT {max_num}
        '''
        return self.sql_reader.execute_get_query(get_query)

    def get_max_internal_citing(self, surely_coauthors):
        get_query = fr'''
            SELECT internal_citing from clique where surely_coauthors={surely_coauthors}
            ORDER BY internal_citing DESC LIMIT 1
        '''
        return self.sql_reader.execute_get_query(get_query)

    def get_min_internal_citing(self, surely_coauthors):
        get_query = fr'''
            SELECT internal_citing from clique where surely_coauthors={surely_coauthors} and internal_citing > 0
            ORDER BY internal_citing LIMIT 1
        '''
        return self.sql_reader.execute_get_query(get_query)

    def get_mean_internal_citing(self, surely_coauthors):
        get_query = fr'''
                    SELECT AVG(internal_citing) from clique
                    where surely_coauthors={surely_coauthors}
                '''
        return self.sql_reader.execute_get_query(get_query)

    def get_max_external_citing(self, surely_coauthors):
        get_query = fr'''
            SELECT external_citing from clique where surely_coauthors={surely_coauthors}
            ORDER BY external_citing DESC LIMIT 1
        '''
        return self.sql_reader.execute_get_query(get_query)

    def get_mean_external_citing(self, surely_coauthors):
        get_query = fr'''
            SELECT AVG(external_citing) from clique
            where surely_coauthors={surely_coauthors}
        '''
        return self.sql_reader.execute_get_query(get_query)

    def get_min_external_citing(self, surely_coauthors):
        get_query = fr'''
            SELECT external_citing from clique where surely_coauthors={surely_coauthors}
            ORDER BY external_citing LIMIT 1
        '''
        return self.sql_reader.execute_get_query(get_query)

    def get_cliques_internal_citings(self, cliques_ids):
        internal_citings = []
        for clique_id in cliques_ids:
            citing = self.clique_reader.get_clique_internal_citing(clique_id)[0][0]
            internal_citings.append(citing)
        return internal_citings

    def get_cliques_external_citings(self, cliques_ids):
        external_citings = []
        for clique_id in cliques_ids:
            citing = self.clique_reader.get_clique_external_citing(clique_id)[0][0]
            external_citings.append(citing)
        return external_citings

    def get_random_cliques(self, cliques_num, surely_coauthors):
        get_ids_query = fr'''
            SELECT id from clique where surely_coauthors={surely_coauthors}
        '''
        ids = self.sql_reader.execute_get_query(get_ids_query)
        max_ind = len(ids)
        random_cliques = []
        for i in range(cliques_num):
            rand_id = randint(1, max_ind)
            while ids[rand_id][0] in random_cliques:
                rand_id = randint(1, max_ind)
            random_cliques.append(ids[rand_id][0])
        return random_cliques

    def get_cliques_biggest_internal(self, cliques_num, surely_coauthors):
        get_ids_query = fr'''
                   SELECT id from clique where surely_coauthors={surely_coauthors}
                   ORDER BY internal_citing DESC LIMIT {cliques_num}
               '''
        return self.sql_reader.execute_get_query(get_ids_query)

    def get_cliques_least_internal(self, cliques_num, surely_coauthors):
        get_ids_query = fr'''
                   SELECT id from clique where surely_coauthors={surely_coauthors} and external_citing > 0
                   ORDER BY external_citing LIMIT {cliques_num}
               '''
        return self.sql_reader.execute_get_query(get_ids_query)
