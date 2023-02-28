import re
import pandas as pd
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.sensors.filesystem import FileSensor
from airflow.providers.mongo.hooks.mongo import MongoHook


AIRFLOW_HOME = "/opt/airflow"
DATA_INPUT = f"{AIRFLOW_HOME}/data/raw"
DATA_OUTPUT = f"{AIRFLOW_HOME}/data/proc"
FILENAME = "tiktok_google_play_reviews.csv"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["toMongo", "pandas"],
)
def from_padnas_to_mongo_taskflow_api():

    def download(flag: bool) -> str:
        """
        Simulates downloading process of main file and return path/to/file
        """
        if flag:
            return f"{DATA_INPUT}/{FILENAME}"

    @task_group
    def transform(file: str) -> str:
        """
        Task group combines transformational tasks for processing dataframe
        """
        return sort_by_created_at(fill_nulls(replace_emojis_content(file)))

    @task()
    def fill_nulls(file) -> str:
        df = pd.read_csv(file)
        df["content"] = df["content"].fillna("-")
        output = f"{DATA_OUTPUT}/tiktok_without_nulls.csv"
        df.to_csv(output, index=False)
        return output

    @task
    def replace_emojis_content(file) -> str:
        df = pd.read_csv(file)
        df["content"] = df["content"].str.replace(
            "[^A-Za-z0-9_ ]", "", flags=re.UNICODE
        )
        output = f"{DATA_OUTPUT}/tiktok_without_nulls_and_emojis.csv"
        df.to_csv(output, index=False)
        return output

    @task
    def sort_by_created_at(file) -> str:
        df = pd.read_csv(file)
        df.sort_values("at", inplace=True)
        output = f"{DATA_OUTPUT}/ordered_tiktok_without_nulls_and_emojis.csv"
        df.to_csv(output, index=False)
        return output

    @task()
    def upload(file) -> None:
        hook = MongoHook(mongo_conn_id="mongo_conn")
        client = hook.get_conn()
        db = client.Educational
        currency_collection = db.tiktok_comments
        df = pd.read_csv(file)
        df.reset_index(inplace=True)
        data_dict = df.to_dict("records")
        currency_collection.insert_many(data_dict)

    file_sensor = FileSensor(
        task_id="file_sensor",
        filepath=f"{DATA_INPUT}/{FILENAME}",
        fs_conn_id="my_fs",
        poke_interval=20,
    )

    # TODO: figure out how to set up old fashion sensor with new TaskFlow API
    # https://stackoverflow.com/questions/75586272/airflow-taskflow-api-connect-with-filesensor
    # _download = download()
    # file_sensor >> _download
    # show(transform(_download)) >> upload()
    upload(transform(download(file_sensor)))


from_padnas_to_mongo_taskflow_api()
