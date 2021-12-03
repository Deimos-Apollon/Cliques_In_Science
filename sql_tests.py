from crossref.restful import Works
from dir.SqlReaderWriter import SqlReaderWriter

import json
from collections import Counter

def test():
    works = Works()
    sqlrw = SqlReaderWriter()
    res = works.filter(has_orcid='true', from_created_date='2000', until_created_date='2005')
    for work in res.sample(5):
        authors = work.get('author')
        if authors:
            for author in authors:
                ORCID = author.get('ORCID')
                if ORCID:
                    # read author
                    ORCID = ORCID.split('/')[-1]
                    sqlrw.add_new_author(ORCID)

        # read work's DOI
        DOI = work.get('DOI')
        if DOI:
            year = work.get('created')['date-parts'][0][0]
            sqlrw.add_new_work(DOI, year if year else 0)

    # read author_citates_author
    sqlrw.add_new_author('1')
    sqlrw.add_new_author('2')
    sqlrw.add_new_author('3')
    sqlrw.add_new_author('4')
    author_sources = {'1': ['1', '2', '3', '4'],
                      '2': ['1', '3', '4'],
                      '3': ['1', '2'],
                      '4': ['1', '5']}

    for author, sources in author_sources.items():
        for source in sources:
            sqlrw.add_new_author_citates_author(author, source)

    # read author_has_work
    DOI = ['doi_1', 'doi_2', 'doi_3']
    for doi in DOI:
        sqlrw.add_new_work(doi, 2002)
    sqlrw.add_new_author_has_work('1', 'doi_1')
    sqlrw.add_new_author_has_work('3', 'doi_3')
    sqlrw.add_new_author_has_work('1', 'doi_2')
    sqlrw.add_new_author_has_work('1', 'doi_2992')
    sqlrw.add_new_author_has_work('2', 'doi_1')
    sqlrw.add_new_author_has_work('2', 'doi_2')

    # read work_ref
    sqlrw.add_new_work_ref(DOI[0], DOI[1])
    sqlrw.add_new_work_ref(DOI[1], DOI[0])
    sqlrw.add_new_work_ref(DOI[1], DOI[2])
    sqlrw.add_new_work_ref(DOI[0], 'doi2922')


def test_get_authors():
    sqlrw = SqlReaderWriter()
    print(sqlrw.get_work_authors('doi_2'))
    print(sqlrw.get_work_authors('doi2'))


if __name__ == '__main__':
    #test()
    works = Works()
    res = works.filter(has_orcid='true', from_created_date='2005', until_created_date='2011')

    sqlrw = SqlReaderWriter()
    got = sqlrw.get_all_author_citates_author()


    c = Counter()
    for author, src in got:
        c[author] += 1

    print(f"Most common keys: {c.most_common(10)}")
    print(f"Authors: {len(c.keys())}")
    print(f"References: {sum(c.values())}")

