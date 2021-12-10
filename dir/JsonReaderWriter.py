import json
import gzip
import time

from progress.bar import IncrementalBar


class JsonReaderWriter:
    def __init__(self, min_vacant_json_file_number, left_border, right_border):
        self.__max_entry__ = 5000
        self.__current_json_file_number = min_vacant_json_file_number
        self.file_range = range(left_border, right_border)
        self.file_names = file_names
        self.file_to_write_prefix = file_to_write_prefix

    def proceed(self):
        compressed_data = {'items': []}

        current_entry = 0
        filename = self.file_to_write_prefix + str(self.__current_json_file_number) + ".json"

        out_file = open(filename, 'w')

        start = time.time()
        bar = IncrementalBar(f"Processing input files", max=len(self.file_range))
        for file_number, input_path in enumerate(self.file_names):
            with gzip.open(input_path, 'r') as archive:
                archive_data = archive.read()
                data = json.loads(archive_data.decode('utf-8'))['items']

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
        compressed_data = {'items': []}
        out_file.close()
        print(f"LOG: JsonReaderWriter.proceed total time in minutes: {(time.time() - start) / 60}\n")

    def __get_new_file_name__(self):
        self.__current_json_file_number += 1
        return self.file_to_write_prefix + str(self.__current_json_file_number) + ".json"

    def __is_valid__(self, elem):
        if elem.get("subject") is None \
                or elem.get("DOI") is None \
                or elem.get("author") is None \
                or elem.get("references-count") is None \
                or elem.get("is-referenced-by-count") is None \
                or elem.get("created") is None:
            return False
        for author in elem["author"]:
            if not author.get("given") or not author.get("family") or len(author["family"]) > 40 \
                    or '"' in author["given"] or ',' in author["given"] \
                    or '"' in author["family"] or ',' in author["family"]:
                return False
        return True

    def __work_json_repr__(self, elem):
        doi = elem["DOI"]
        subject = elem['subject']

        year = elem['created']['date-parts'][0][0]
        references_count = elem["references-count"]
        references = elem.get("reference") or []

        is_referenced_by_count = elem['is-referenced-by-count']

        authors = elem['author']
        new_authors = []  # list of dicts
        for author in authors:
            new_authors.append({"given": author.get("given"),
                                "family": author.get("family")})
        new_references = []
        for reference in references:
            ref_doi = reference.get("DOI")
            if ref_doi is not None:
                new_references.append({"DOI": ref_doi})

        json_view = {"DOI": doi,
                     "year": year,
                     "subject": subject,
                     "references-count": references_count,
                     "is-referenced-by-count": is_referenced_by_count,
                     "author": new_authors,
                     "reference": new_references,
                     }
        return json_view
