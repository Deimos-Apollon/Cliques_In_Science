import gzip
import json

from tqdm import tqdm

from source.DirManager import get_filenames_in_dir


class CrossrefCompressor:
    def __init__(self, dataset_dir):
        self.__max_entry__ = 5000
        self.__current_json_file_number = 0
        self.src_file_names = get_filenames_in_dir(dataset_dir)
        self.output_dir = fr"C:\Users\user\PycharmProjects\Alt_exam\dataset\crossref"

    def proceed(self):
        compressed_data = {'items': []}

        current_entry = 0
        filename = fr"{self.output_dir}\{self.__current_json_file_number}.json"

        with open(filename, 'w') as out_file:
            for input_path in tqdm(self.src_file_names, f"Processing input files"):
                with gzip.open(input_path, 'r') as fin:
                    data = json.loads(fin.read().decode('utf-8'))
                    for work in data['items']:
                        if self.__is_valid(work):
                            work_repr = self.__work_json_repr__(work)
                            if work_repr:
                                compressed_data['items'].append(work_repr)
                                current_entry += 1
                                if current_entry == self.__max_entry__:
                                    current_entry = 0
                                    json.dump(compressed_data, out_file)
                                    compressed_data = {'items': []}
                                    out_file.close()
                                    out_file = open(self.__get_new_file_name__(), 'w')
            if compressed_data:
                json.dump(compressed_data, out_file)

    def __get_new_file_name__(self):
        self.__current_json_file_number += 1
        return fr"{self.output_dir}\{self.__current_json_file_number}.json"

    def __is_valid(self, work):
        if work.get('author') and work.get('DOI') and work.get('reference') and work.get('subject') \
                and work.get('published-print'):
            if work['published-print'].get('date-parts'):
                if 2000 <= work['published-print']['date-parts'][0][0]:
                    for author in work['author']:
                        if self.__is_valid_author(author):
                            return True
        return False

    @staticmethod
    def __is_valid_author(author):
        if author.get('given') and author.get('family'):
            given, family = author['given'], author['family']
            if not any(x in given for x in ['"', ","]) and not any(x in family for x in ['"', ","]) and \
                    len(given) < 40 and len(family) < 40:
                return True
        return False

    def __work_json_repr__(self, elem):
        doi = elem["DOI"]
        subject = elem['subject']
        year = elem['published-print']['date-parts'][0][0]
        authors = []
        for author in elem['author']:
            if self.__is_valid_author(author):
                author_repr = {'given': author['given'], 'family': author['family']}
                authors.append(author_repr)
        references = []
        for ref in elem['reference']:
            if ref.get('DOI'):
                references.append(ref['DOI'])
        if not references:
            return None
        json_view = {
            "DOI": doi,
            "year": year,
            "subject": subject,
            "author": authors,
            "reference": references,
        }
        return json_view
