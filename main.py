from threading import Thread
from time import time, sleep
from source.JsonToSqlWriter import JsonToSqlWriter


if __name__ == "__main__":
    json_to_sql_writer = JsonToSqlWriter()

    file_names = [
        [fr'C:\Users\diest\PycharmProjects\Alt_exam\source\dataset\medicine_refs_dataset\medicine_compressed_refs_{i}.json' for i in range(j, j+10)]
         for j in range(5, 134, 10)
    ]

    start = time()

    threads = [Thread(target=json_to_sql_writer.data_read_first_phase_from_files, args=(file_names[i], 5+10*i))
               for i in range(12)]

    # init and start threads
    for thread in threads:
        thread.start()

    # finish threads
    for thread in threads:
        thread.join()

    print('Second phase finished in minutes', (time() - start) / 60)

