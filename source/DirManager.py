from pathlib import Path
from os import listdir


def count_files_in_dir(path):
    return len(listdir(path))


def create_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)


def get_filenames_in_dir(path):
    return tuple(fr"{path}/{file_name}" for file_name in listdir(path))
