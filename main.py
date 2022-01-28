from source.SQL_interaction.SqlComponentFiller import SqlComponentFiller

if __name__ == "__main__":
    subjects = ("Analysis",)

    dataset_dir = r"C:\Users\user\PycharmProjects\Alt_exam\dataset"

    # json_compressor = JsonCompressor(dataset_dir, subjects[0])
    # json_compressor.proceed()

    # src_dir = fr"{dataset_dir}\{subjects[0]}"
    # json_to_sql = JsonToSqlWriter()
    # json_to_sql.write_second_phase(src_dir, 4)

    # src_filename = [r"C:\Users\user\PycharmProjects\Alt_exam\dataset\Logic\logic_0.json"]
    # json_to_sql = JsonToSqlWriter()
    # json_to_sql.data_read_first_phase_from_files(src_filename, 0)
    # json_to_sql.data_read_second_phase_from_files(src_filename, 0)
    # reader = SqlReader(create_connection())
    # query = r'''
    #     SELECT COUNT(*) FROM author_has_work WHERE work_DOI = 'adfgshja'
    # '''
    # print(reader.execute_get_query(query))

    # subject = "Logic"  #
    #
    # l = [tqdm(range(10), postfix=f"{position}", position=position) for position in [0,1,2]]
    #
    # def first(amount, tqdm_par):
    #     for _ in tqdm_par:
    #         time.sleep(amount)

    # thread1 = Thread(target=first, args=(0.3, l[0]))
    # thread2 = Thread(target=first, args=(0.2, l[1]))
    # thread3 = Thread(target=first, args=(0.1, l[2]))
    #
    # thread1.start()
    # thread2.start()
    # thread3.start()
    #
    # thread1.join()
    # thread2.join()
    # thread3.join()

    # author_merger = AuthorMerger()
    # author_merger.merge_authors()

    # graph_filler = SqlGraphFiller()
    # graph_filler.fill_graph_table()

    comps_finder = SqlComponentFiller()
    comps_finder.find_comps()

    # bk_manager = BronKerboschManager()
    # print(bk_manager.bron_kerbosch(73))
    # jupyter notebook --NotebookApp.allow_origin='https://colab.research.google.com' --port=8888 --NotebookApp.port_retries=0
