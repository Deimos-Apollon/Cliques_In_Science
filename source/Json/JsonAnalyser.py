import json
from collections import Counter
import time
from progress.bar import IncrementalBar


class JsonAnalyser:
    def __init__(self, file_names, last_file_number):
        self.__works_count__ = 0
        self.__works_with_refs_list__ = 0
        self.__works_with_is_referenced__ = 0
        self.__subjects_count__ = Counter()
        self.__year_distribution__ = Counter()
        self.last_file_number = last_file_number
        self.files_names = file_names

    def proceed(self):
        bar = IncrementalBar("JsonAnalyser proceed", max=self.last_file_number)
        start = time.time()
        for file_name in self.files_names:
            with open(file_name, "r") as file:
                data = json.load(file)['items']
                for work in data:
                    self.__works_count__ += 1
                    self.__works_with_refs_list__ += 1 if work.get("reference") else 0
                    self.__works_with_is_referenced__ += 1 if work.get("is-referenced-by-count") else 0

                    subjects = work.get("subject")
                    if subjects:
                        for subject in subjects:
                            self.__subjects_count__[subject] += 1

                    year = work.get("year")
                    if year:
                        self.__year_distribution__[year] += 1
            bar.next()
        print("\nСейчас будет мусор: ", end=' ')
        bar.finish()
        print()
        print(f"LOG: JsonReaderWriter.proceed total time in minutes: {(time.time() - start) / 60}\n")

    def save_results(self, main_info_file, all_subjects_file):
        year_distribution_repr = {i[0]: i[1] for i in sorted(self.__year_distribution__.items())}

        subjects_count_repr = {i[0]: i[1] for i in self.__subjects_count__.most_common(100)}

        items = {"items": {"Total works count": self.__works_count__,
                           "Total works with ref_list": self.__works_with_refs_list__,
                           "Total works with is referenced by count": self.__works_with_is_referenced__,
                           "Year distribution": year_distribution_repr,
                           "Top 100 subjects distribution": subjects_count_repr,}
                 }
        with open(main_info_file,
                  'w') as file:
            json.dump(items, file, indent=4)

        all_subject_distribution = {i[0]: i[1] for i in self.__subjects_count__.most_common()}
        with open(all_subjects_file, 'w') as file:
            json.dump(all_subject_distribution, file, indent=4)
