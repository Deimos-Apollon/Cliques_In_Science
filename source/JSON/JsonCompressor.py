import json
from tqdm.notebook import tqdm

from source.DirManager import get_filenames_in_dir, create_dir


class JsonCompressor:
    def __init__(self, dataset_dir, subj_name):
        self.__max_entry__ = 5000
        self.__current_json_file_number = 0
        self.src_file_names = get_filenames_in_dir(fr"{dataset_dir}\crossref")
        self.output_dir = fr"{dataset_dir}\{subj_name}"
        create_dir(self.output_dir)
        self.subj = subj_name

    def proceed(self):
        compressed_data = {'items': []}

        current_entry = 0
        filename = fr"{self.output_dir}\{self.__current_json_file_number}.json"

        with open(filename, 'w') as out_file:
            for input_path in tqdm(self.src_file_names, f"Processing input files"):
                with open(input_path, 'r') as file:
                    for work in json.load(file)['items']:
                        if self.subj in work['subject']:
                            compressed_data['items'].append(self.__work_json_repr__(work))
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

    @staticmethod
    def __work_json_repr__(elem):
        doi = elem["DOI"]
        subject = elem['subject']
        year = elem['year']
        authors = elem['author']
        references = elem["reference"]
        json_view = {
            "DOI": doi,
            "year": year,
            "subject": subject,
            "author": authors,
            "reference": references,
        }
        return json_view
