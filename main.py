import time
from collections import Counter
from dir.SqlReaderWriter import SqlReaderWriter
from progress.bar import IncrementalBar

import json
import gzip


def data_read_first_phase(samples):
    bar1 = IncrementalBar("Reading first phase", max=len(samples))
    start = time.time()

    work_without_auth = 0
    works_without_doi = 0
    # data reading first phase: works, authors
    for work in samples:
        # read work's DOI
        DOI = work.get('DOI')
        if DOI:
            authors = work.get('author')
            if authors:
                date = f"{work.get('created')['date-parts'][0][0]}-{work.get('created')['date-parts'][0][1]}-" \
                       f"{work.get('created')['date-parts'][0][2]}"
                referenced_count = work.get('references-count') or 0
                is_referenced_count = work.get('is-referenced-by-count') or 0
                sqlrw.add_new_work(DOI, date if date else "00-00-00", referenced_count, is_referenced_count)

                for author in authors:

                    given_name = author.get('given') or ""
                    family_name = author.get('family') or ""
                    affilation = author.get('affiliation') or ""
                    if affilation:
                        affilation = affilation[0]['name']
                        if len(affilation) > 130:
                            affilation = 'Affilation is trash, >130'
                    ORCID = author.get('ORCID') or ""

                    sqlrw.add_new_author(given_name, family_name, affilation, ORCID)
                    author_ID = sqlrw.get_author_id(given_name, family_name, affilation, ORCID)  # TODO VERY BAD!!

                    sqlrw.add_new_author_has_work(author_ID[0][0], DOI)  # TODO УБРАТЬ ДУБЛИРОВАНИЕ ЗАПИСЕЙ
            else:
                work_without_auth += 1
        else:
            works_without_doi += 1
        bar1.next()

    bar1.finish()
    print()
    first_phase_time = time.time() - start
    print(first_phase_time)


def data_read_second_phase(samples):
    # data reading second phase: adding refs
    for work in samples:
        work_DOI = work.get('DOI')
        work_authors = sqlrw.get_work_authors(work_DOI)
        references = work.get('reference')
        if references:
            for src in references:
                src_DOI = src.get('DOI')
                if src_DOI:
                    # read work_ref
                    if sqlrw.check_if_DOI_exists(src_DOI):
                        sqlrw.add_new_work_ref(work_DOI, src_DOI)

                    # read author_citates_author
                    src_authors = sqlrw.get_work_authors(src_DOI)
                    if src_authors:
                        for src_author in src_authors:
                            for work_author in work_authors:
                                sqlrw.add_new_author_citates_author(work_author[0], src_author[0])


if __name__ == "__main__":
    # start = time.time()
    sqlrw = SqlReaderWriter()

    file_names = [fr'C:\Users\user\Downloads\ps\crossref\{i}.json.gz' for i in range(32000, 33000)]
    subject_distribution = Counter()
    glob_start = time.time()
    subjects = set()
    with open(fr'C:\Users\user\PycharmProjects\alt_exam_1\dataset\SUPER.json', 'w') as new_ar:
        new_data = {'items': []}
        for file_number, path in enumerate(file_names):
            print(f"\nProcessing file #{file_number}: \n")
            with gzip.open(path, 'r') as archive:

                archive_data = archive.read()
                data = json.loads(archive_data.decode('utf-8'))['items']

                samples = data
                bar1 = IncrementalBar("Reading first phase", max=len(samples))
                start = time.time()
                # data reading first phase: works, authors
                for work in samples:
                    bar1.next()
                    # read work's DOI
                    DOI = work.get('DOI')
                    SUBJECT = work.get('subject')
                    if SUBJECT and DOI:
                        work_json_repr = {"Subject": SUBJECT}
                        new_data['items'].append(work_json_repr)
                        for elem in SUBJECT:
                            subject_distribution[elem] += 1
                bar1.finish()
                print()
                first_phase_time = time.time() - start
                print(first_phase_time)

                data = json.loads(archive_data.decode('utf-8'))
        json.dump(new_data, new_ar)

    print(time.time() - glob_start)
    print(len(subjects))
    print(subject_distribution)
