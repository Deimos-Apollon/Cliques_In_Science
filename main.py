from threading import Thread
from time import time, sleep
from source.JsonToSqlWriter import JsonToSqlWriter


if __name__ == "__main__":
    file_names = (fr'C:\Users\user\PycharmProjects\alt_exam_1\compressed_refs_dataset\compressed_refs_{i}.json' for i in range(1036))
    file_to_write_prefix = r'C:\Users\user\PycharmProjects\alt_exam_1\medicine_refs_dataset\medicine_compressed_refs_'
    json_rw = JsonReaderWriter(file_names, file_to_write_prefix, 0, 0, 1036)
    json_rw.proceed()

    # 0 - 5000 началось
    # 128.9418012022972 min остальное
