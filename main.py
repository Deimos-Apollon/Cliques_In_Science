import time
from collections import Counter

from dir.DataReader import data_read_first_phase, data_read_second_phase
from dir.JsonAnalyser import JsonAnalyser
from dir.SqlReaderWriter import SqlReaderWriter
from progress.bar import IncrementalBar
from dir.JsonReaderWriter import JsonReaderWriter

from dir.config import SQL_USER, SQL_PASS
from mysql.connector import connect, Error, ProgrammingError


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

def citations_to_graph_table():
    start = time.time()
    sql_rw = SqlReaderWriter()
    citations_num = sql_rw.get_number_author_citates_author()
    bar = IncrementalBar(max=citations_num)
    for i in range(1, citations_num + 1):
        author, src = sql_rw.get_citation_via_id(i)
        co_id = sql_rw.get_citation_via_authors(src, author)
        if co_id:
            sql_rw.add_edge_to_graph(i)
            sql_rw.add_edge_to_graph(co_id)
        bar.next()
    bar.finish()
    print("Edges adding time in minutes:", (time.time() - start) / 60)


def edges_to_json():
    sqlrw = SqlReaderWriter()
    data = []
    for id in range(1, 930):
        entry_id = sqlrw.get_citation_id_from_graph(id)
        author, src = sqlrw.get_citation_via_id(entry_id)
        data.append([author, src])
    items = {}
    items["items"] = data
    with open("edges.json", 'w') as file:
        json.dump(items, file, indent=1)


if __name__ == "__main__":
    file_names = (fr'C:\Users\user\PycharmProjects\alt_exam_1\compressed_refs_dataset\compressed_refs_{i}.json' for i in range(1036))
    file_to_write_prefix = r'C:\Users\user\PycharmProjects\alt_exam_1\medicine_refs_dataset\medicine_compressed_refs_'
    json_rw = JsonReaderWriter(file_names, file_to_write_prefix, 0, 0, 1036)
    json_rw.proceed()

    # 0 - 5000 началось
    # 128.9418012022972 min остальное
