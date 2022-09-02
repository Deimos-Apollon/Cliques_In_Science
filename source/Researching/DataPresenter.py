from matplotlib.ticker import MaxNLocator

from source.Researching.DataAnalyser import DataAnalyser
import matplotlib.pyplot as plt


class DataPresenter:
    def __init__(self):
        self.__data_analyser = DataAnalyser()

    def show_author_distribution(self, width, height):
        authors_distribution = self.__data_analyser.get_authors_distribution()
        x = authors_distribution.keys()
        y = authors_distribution.values()
        f, ax = plt.subplots()
        f.set_size_inches(width, height)
        ax.plot(x, y, marker='o')
        ax.grid()
        ax.set_title("Распределение по годам авторов, публикующихся в этом году")
        ax.set_xlabel("Год")
        ax.set_ylabel("Кол-во авторов")
        ax.xaxis.set_major_locator(MaxNLocator(21, integer=True))
        print_distribution_dict(authors_distribution)

    def show_work_distribution(self, width, height):
        work_distribution = self.__data_analyser.get_work_distribution()
        x = work_distribution.keys()
        y = work_distribution.values()
        f, ax = plt.subplots()
        f.set_size_inches(width, height)
        ax.plot(x, y, marker='o')
        ax.grid()
        ax.set_title("Распределение работ по годам")
        ax.set_xlabel("Год")
        ax.set_ylabel("Кол-во работ")
        ax.xaxis.set_major_locator(MaxNLocator(21, integer=True))
        print_distribution_dict(work_distribution)

    def show_work_authors_num_distribution(self, width, height, ):
        distribution = self.__data_analyser.get_work_authors_num_distribution()
        x = distribution.keys()
        y = distribution.values()

        f, ax = plt.subplots()
        f.set_size_inches(width, height)
        ax.plot(x, y, marker='o')
        ax.plot(x, [sum(y) / len(x) for _ in range(len(x))], '-', label='Среднее значение по всем годам')
        ax.grid()
        ax.legend()
        ax.set_title("Распределение среднего кол-ва авторов у работы")
        ax.set_xlabel("Год")
        ax.set_ylabel("Среднее кол-во авторов")
        ax.xaxis.set_major_locator(MaxNLocator(21, integer=True))
        print_distribution_dict(distribution)
        print("Среднее значение по годам:", sum(distribution.values()) / len(distribution))

    def show_max_clique_sizes(self, max_num, width, height, surely_coauthors):
        max_cliques_info = self.__data_analyser.get_max_clique_sizes(max_num, surely_coauthors)
        f, ax = plt.subplots()
        f.set_size_inches(width, height)
        cliques_ids = [f"{elem[0]}" for elem in max_cliques_info]
        cliques_sizes = [elem[1] for elem in max_cliques_info]
        ax.bar(cliques_ids, cliques_sizes)
        ax.grid()
        ax.set_title(f"Топ {max_num} размеров клики {'соавторов' if surely_coauthors else 'авторов'}")
        ax.set_xlabel("ID клики")
        ax.set_ylabel("кол-во авторов")

    def show_internal_citing_info(self, surely_coauthors):
        max_coef_authors = self.__data_analyser.get_max_internal_citing(surely_coauthors=surely_coauthors)[0][0]
        mean_coef_authors = self.__data_analyser.get_mean_internal_citing(surely_coauthors=surely_coauthors)[0][0]
        min_coef_authors = self.__data_analyser.get_min_internal_citing(surely_coauthors=surely_coauthors)[0][0]
        print(f"Максимальный: {max_coef_authors}\n"
              f"Средний: {mean_coef_authors}\n"
              f"Минимальный: {min_coef_authors}\n")

    def show_external_citing_info(self, surely_coauthors):
        max_coef_authors = self.__data_analyser.get_max_external_citing(surely_coauthors=surely_coauthors)[0][0]
        mean_coef_authors = self.__data_analyser.get_mean_external_citing(surely_coauthors=surely_coauthors)[0][0]
        min_coef_authors = self.__data_analyser.get_min_external_citing(surely_coauthors=surely_coauthors)[0][0]
        print(f"Максимальный: {max_coef_authors}\n"
              f"Средний: {mean_coef_authors}\n"
              f"Минимальный: {min_coef_authors}\n")

    def show_random_cliques_analysis(self, cliques_num, width, height, surely_coauthors):
        rand_ids = self.__data_analyser.get_random_cliques(cliques_num, surely_coauthors)
        int_citings = self.__data_analyser.get_cliques_internal_citings(rand_ids)
        mean_int = self.__data_analyser.get_mean_internal_citing(surely_coauthors)[0][0]
        f, (ax1, ax2) = plt.subplots(1, 2)
        f.set_size_inches(width, height)
        ax1.bar([f'{rand_id}' for rand_id in rand_ids], int_citings)
        ax1.plot([mean_int for _ in range(len(rand_ids))], 'y', label='Средн. знач. в БД')
        ax1.grid()
        ax1.legend()
        ax1.set_title(f"{cliques_num} случайных клик {'соавторов' if surely_coauthors else 'авторов'}")
        ax1.set_xlabel("ID клики")
        ax1.set_ylabel("Внутреннее цитирование")

        ext_citings = self.__data_analyser.get_cliques_external_citings(rand_ids)
        mean_ext = self.__data_analyser.get_mean_external_citing(surely_coauthors)[0][0]
        ax2.plot([mean_ext for _ in range(len(rand_ids))], 'y', label='Средн. знач. в БД')
        ax2.bar([f'{rand_id}' for rand_id in rand_ids], ext_citings)
        ax2.grid()
        ax2.legend()
        ax2.set_title(f"{cliques_num} случайных клик {'соавторов' if surely_coauthors else 'авторов'}")
        ax2.set_xlabel("ID клики")
        ax2.set_ylabel("Внешнее цитирование")
        f.tight_layout()

    def show_cliques_biggest_internal(self, width, height, cliques_num, surely_coauthors):
        ids = self.__data_analyser.get_cliques_biggest_internal(cliques_num, surely_coauthors)
        ids = [id[0] for id in ids]
        coefs = self.__data_analyser.get_cliques_internal_citings(ids)
        mean_int = self.__data_analyser.get_mean_internal_citing(surely_coauthors)[0][0]
        f, (ax1, ax2) = plt.subplots(1, 2)
        f.set_size_inches(width, height)
        ax1.bar([f'{id}' for id in ids], coefs)
        ax1.plot([mean_int for _ in range(len(ids))], 'y', label='Средн. знач. в БД')
        ax1.grid()
        ax1.legend()
        ax1.set_title(f"{cliques_num} клик {'соавторов' if surely_coauthors else 'авторов'} с наиб. внутрн. цит.")
        ax1.set_xlabel("ID клики")
        ax1.set_ylabel("Внутреннее цитирование")
        print(f"Среднее значение внутреннего цитирования {'соавторов' if surely_coauthors else 'авторов'} "
              f"по БД: {mean_int}")

        ext_citings = self.__data_analyser.get_cliques_external_citings(ids)
        mean_ext = self.__data_analyser.get_mean_external_citing(surely_coauthors)[0][0]
        ax2.plot([mean_ext for _ in range(len(ids))], 'y', label='Средн. знач. в БД')
        ax2.bar([f'{id}' for id in ids], ext_citings)
        ax2.grid()
        ax2.legend()
        ax2.set_title(f"Внешнее цитирование у тех же {cliques_num} клик ")
        ax2.set_xlabel("ID клики")
        ax2.set_ylabel("Внешнее цитирование")
        f.tight_layout()
        print(f"Среднее значение внешнего цитирования {'соавторов' if surely_coauthors else 'авторов'} "
              f"по БД: {mean_ext}")

    def show_cliques_least_external(self, width, height, cliques_num, surely_coauthors):
        ids = self.__data_analyser.get_cliques_least_internal(cliques_num, surely_coauthors)
        ids = [id[0] for id in ids]
        coefs = self.__data_analyser.get_cliques_external_citings(ids)
        mean_ext = self.__data_analyser.get_mean_external_citing(surely_coauthors)[0][0]
        f, (ax1, ax2) = plt.subplots(1, 2)
        f.set_size_inches(width, height)
        ax1.bar([f'{id}' for id in ids], coefs)
        ax1.plot([mean_ext for _ in range(len(ids))], 'y', label='Средн. знач. в БД')
        ax1.grid()
        ax1.legend()
        ax1.set_title(f"{cliques_num} клик {'соавторов' if surely_coauthors else 'авторов'} с наим. внешн. цит.")
        ax1.set_xlabel("ID клики")
        ax1.set_ylabel("Внешнее цитирование")
        print(f"Среднее значение внешнего цитирования {'соавторов' if surely_coauthors else 'авторов'} "
              f"по БД: {mean_ext}")

        int_citings = self.__data_analyser.get_cliques_internal_citings(ids)
        mean_int = self.__data_analyser.get_mean_internal_citing(surely_coauthors)[0][0]
        ax2.plot([mean_int for _ in range(len(ids))], 'y', label='Средн. знач. в БД')
        ax2.bar([f'{id}' for id in ids], int_citings)
        ax2.grid()
        ax2.legend()
        ax2.set_title(f"Внутреннее цитирование у тех же {cliques_num} клик")
        ax2.set_xlabel("ID клики")
        ax2.set_ylabel("Внутреннее цитирование цитирование")
        f.tight_layout()
        print(f"Среднее значение внутреннего цитирования {'соавторов' if surely_coauthors else 'авторов'} "
              f"по БД: {mean_int}")

    def show_most_suspicious_cliques(self, surely_coauthors, limit):
        data = self.__data_analyser.get_suspicious_cliques(surely_coauthors, limit)
        mean_ext = self.__data_analyser.get_mean_external_citing(surely_coauthors=surely_coauthors)[0][0]
        mean_int = self.__data_analyser.get_mean_internal_citing(surely_coauthors=surely_coauthors)[0][0]
        print(f"Среднее внешнее цитирование: {mean_ext}")
        print(f"Среднее внутреннее цитирование: {mean_int}")
        print()
        print("  row | clique id | size |   ext/int  |   external |    internal")
        for row_number, elem in enumerate(data, start=1):
            id, size, ratio, external_citing, internal_citing = elem
            print(f"{row_number:4}: | {id:9} | {size:4} | {ratio:10.5f} | {external_citing:10.5f} | {internal_citing:10.5f}")


def print_distribution_dict(dictionary):
    new_line_counter = 0
    for key, value in dictionary.items():
        print(f"{key}: {value} ", end=' ')
        new_line_counter += 1
        if new_line_counter == 10:
            print()
            new_line_counter = 0
    print()
