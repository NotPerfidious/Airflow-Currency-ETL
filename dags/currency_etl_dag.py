import requests
from datetime import datetime
from datetime import timedelta
import pendulum

from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook

API_URL = "https://api.frankfurter.dev/v2/rates"


@dag(
    dag_id="daily_currency_etl",
    description="Daily Currency ETL pipeline: Frankfurter API to MongoDB",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Karachi"),
    catchup=False,
    default_args={
        "owner": "mlops",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["ETL", "Currency", "MongoDB"],
)
def daily_currency_etl():

    @task
    def extract_currency_rates(**kwargs):

        response = requests.get(
            API_URL, params={"base": "EUR", "quotes": "USD"}, timeout=30
        )

        response.raise_for_status()

        data = response.json()

        data = data[0]

        print("FrankFurter responded successfully")

        retrieved_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        ti = kwargs["ti"]

        ti.xcom_push(key="raw_exchange_data", value=data)
        ti.xcom_push(key="retrieved_at", value=retrieved_at)

        print("FrankFurter responded successfully")
        print(data)


    @task
    def transform_currency_rates(**kwargs):

        ti=kwargs["ti"]

        raw_data = ti.xcom_pull(task_ids="extract_currency_rates", key="raw_exchange_data")
        retrieved_at = ti.xcom_pull(task_ids="extract_currency_rates", key="retrieved_at")

        if not raw_data:
            raise ValueError("No data found in XCom.")

        transformed_data = {
            "base_currency": raw_data["base"],
            "target_currency":raw_data["quote"],
            "exchange_rate": raw_data["rate"],
            "rate_date":raw_data["date"],
            "source":"Frankfurter API",
            "retrieved_at":retrieved_at
        }

        ti.xcom_push(key="transformed_exchange_data", value=transformed_data)

        print(f"Transformed data ready for MongoDB: {transformed_data}")


    @task
    def load_data_to_mongo(**kwargs):

        ti = kwargs["ti"]

        transformed_data = ti.xcom_pull(task_ids="transform_currency_rates", key="transformed_exchange_data")

        if not transformed_data:
            raise ValueError("No data received from transform task.")

        hook = MongoHook(conn_id="mongo_local")

        client = hook.get_conn()

        try:

            db = client["currency_etl_db"]
            collection = db['exchange_rates']

            result = collection.insert_one(transformed_data)

            print(f"Successfully inserted document ID: {result.inserted_id}")

        except Exception as e:

            print(f"[EXCEPTION]: {e}")

        finally:
            client.close()

        

    extract_task =  extract_currency_rates()
    transform_task = transform_currency_rates()
    load_task = load_data_to_mongo()

    extract_task >> transform_task >> load_task


daily_currency_etl()
