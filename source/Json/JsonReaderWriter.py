import json
import time

from progress.bar import IncrementalBar
from source.time_decorator import time_method_decorator


class JsonReaderWriter:
    def __init__(self, src_file_names, file_to_write_prefix, subj_name):
        self.__max_entry__ = 5000
        self.__current_json_file_number = 0
        self.src_file_names = src_file_names
        self.file_to_write_prefix = file_to_write_prefix
        self.subj = subj_name

    def proceed(self):
        compressed_data = {'items': []}

        current_entry = 0
        filename = self.file_to_write_prefix + str(self.__current_json_file_number) + ".json"

        with open(filename, 'w') as out_file:
            start = time.time()
            bar = IncrementalBar(f"Processing input files", max=len(self.src_file_names))
            for input_path in self.src_file_names:
                with open(input_path, 'r') as file:

                    data = json.load(file)['items']

                    for work in data:
                        if self.__is_valid__(work):
                            compressed_data['items'].append(self.__work_json_repr__(work))
                            current_entry += 1
                            if current_entry == self.__max_entry__:
                                current_entry = 0
                                json.dump(compressed_data, out_file)
                                compressed_data = {'items': []}
                                out_file.close()
                                out_file = open(self.__get_new_file_name__(), 'w')
                bar.next()

            print("\nСейчас будет мусор: ", end=' ')
            bar.finish()
            print()
            if compressed_data:
                json.dump(compressed_data, out_file)
        print(f"LOG: JsonReader.proceed total time in minutes: {(time.time() - start) / 60}\n")

    def __get_new_file_name__(self):
        self.__current_json_file_number += 1
        return self.file_to_write_prefix + str(self.__current_json_file_number) + ".json"

    def __is_valid__(self, elem):
        if not elem.get("subject") \
                or self.subj not in elem["subject"]\
                or elem.get("DOI") is None \
                or not elem.get("author") \
                or elem.get("references-count") is None \
                or elem.get("is-referenced-by-count") is None:
            return False
        for author in elem["author"]:
            if not author.get("given") or not author.get("family") or len(author["family"]) > 40 \
                    or '"' in author["given"] or ',' in author["given"] \
                    or '"' in author["family"] or ',' in author["family"]:
                return False
        if not len(elem["reference"]):
            return False
        return True

    def __work_json_repr__(self, elem):
        doi = elem["DOI"]
        subject = elem['subject']

        year = elem['year']
        references_count = elem["references-count"]
        is_referenced_by_count = elem['is-referenced-by-count']

        authors = elem['author']
        # new_authors = []
        # for author in authors:
        #     given = author.get("given")
        #     family = author.get("family")
        #     if given and family:
        #         new_authors.append({"given": given, "family": family})

        references = elem["reference"]
        # new_references = []
        # for reference in references:
        #     DOI = reference.get("DOI")
        #     if DOI:
        #         new_references.append({"DOI": DOI})

        json_view = {"DOI": doi,
                     "year": year,
                     "subject": subject,
                     "references-count": references_count,
                     "is-referenced-by-count": is_referenced_by_count,
                     "author": authors,
                     "reference": references,
                     }
        return json_view
