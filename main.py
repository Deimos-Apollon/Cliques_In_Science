from threading import Thread
from time import time, sleep
from source.JsonToSqlWriter import JsonToSqlWriter


if __name__ == "__main__":
    file_names = [
        [fr'C:\Users\user\PycharmProjects\alt_exam_1\dataset\medicine_refs_dataset\medicine_compressed_refs_{i}.json'
         for i in range(130, 131)],
        [fr'C:\Users\user\PycharmProjects\alt_exam_1\dataset\medicine_refs_dataset\medicine_compressed_refs_{i}.json'
         for i in range(131, 132)],
        [fr'C:\Users\user\PycharmProjects\alt_exam_1\dataset\medicine_refs_dataset\medicine_compressed_refs_{i}.json'
         for i in range(132, 133)]
    ]
    json_to_sql_writer = JsonToSqlWriter()

    threads = [Thread(target=json_to_sql_writer.data_read_first_phase_from_files, args=(file_names[i], 130+i))
               for i in range(3)]

    start = time()
    # init and start threads
    for thread in threads:
        thread.start()

    # finish threads
    for thread in threads:
        thread.join()

    print('finished', time() - start)
