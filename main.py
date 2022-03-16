from source.Researching.DataAnalyser import DataAnalyser
from source.Researching.DataPresenter import DataPresenter

if __name__ == "__main__":
    from source.JSON_to_SQL.JsonToSqlWriter import JsonToSqlWriter
    from source.DirManager import get_filenames_in_dir

    subjects = ("Electrical and Electronic Engineering",)
    dataset_dir = r"/media/deimos/Мои_файл_/PyCharmProjects/Alt_exam/dataset"
    threads_num = 1

    for subject in subjects:
        src_dir = fr"{dataset_dir}/{subject}"
        json_to_sql = JsonToSqlWriter()
        json_to_sql.write_first_phase(src_dir, threads_num)
    # jupyter notebook --NotebookApp.allow_origin='https://colab.research.google.com' --port=8888 --NotebookApp.port_retries=0
