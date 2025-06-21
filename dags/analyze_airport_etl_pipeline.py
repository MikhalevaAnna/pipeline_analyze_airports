from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import csv, os, pendulum, random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

files = ["sample_data/airlines.csv",
         "sample_data/airports.csv",
         "sample_data/flights_pak.csv"]
table_name = "airlines_data"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}


dag = DAG(
    'pipeline_analyze_airport',
    default_args=default_args,
    description='A simple DAG to interact with PySpark and ClickHouse',
    schedule_interval=None,
)
def create_chart(x, y, x_label, y_label, title, chart_name):
    plt.figure(figsize=(15, 10), dpi=100)
    plt.bar(x, y)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(rotation=25)
    plt.title(title)
    plt.savefig(chart_name)
    plt.show()

def migration_from_spark_to_postgres():
    spark = SparkSession.builder \
        .appName("Airflow_PySpark") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
        .master("local") \
        .getOrCreate()
    dataframes = {}
    df_distinct = {}
    df_dropna = {}
    df_unique = {}
    df_flights_pak = None
    df_airlines = None
    df_airports = None
    url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

    for file in files:
        df_name = file.split('/')[1].split('.')[0]
        print(df_name)
        try:
            dataframes[df_name] = spark.read.csv(file, header=True, inferSchema=True, multiLine=True)
        except Exception as error:
            raise Exception(
                f'Ошибка при чтении файла {file}! '
                f'Ошибка: {error}!')
        print(f"Начальная структура датафрейма {df_name}:")
        dataframes[df_name].printSchema()
        dataframes[df_name].show(10)
        count_total = dataframes[df_name].count()
        print(f"Количество записей в датафрейме {df_name} всего: {count_total}.")
        df_distinct[df_name] = dataframes[df_name].distinct()
        count_clear = df_distinct[df_name].count()
        print(f"Количество записей в датафрейме всего без дубляжа: {count_clear}.")
        df_distinct[df_name].show(5)
        if df_name == "flights_pak":
            df_dropna[df_name] = df_distinct[df_name].filter("AIRLINE is not NULL OR DATE is not NULL OR "
                                                             "ORIGIN_AIRPORT IS NOT NULL OR "
                                                             "DESTINATION_AIRPORT IS NOT NULL OR "
                                                             "DISTANCE IS NOT NULL  OR "
                                                             "DEPARTURE_HOUR IS NOT NULL"
                                                             ).fillna(0)
            df_flights_pak = (df_dropna[df_name].withColumn("DATE", F.col("DATE").cast(T.DateType()))
                                        .withColumn("DAY_OF_WEEK", F.col("DAY_OF_WEEK").cast(T.StringType()))
                                        .withColumn("AIRLINE", F.col("AIRLINE").cast(T.StringType()))
                                        .withColumn("FLIGHT_NUMBER", F.col("FLIGHT_NUMBER").cast(T.IntegerType()))
                                        .withColumn("TAIL_NUMBER", F.col("TAIL_NUMBER").cast(T.StringType()))
                                        .withColumn("ORIGIN_AIRPORT", F.col("ORIGIN_AIRPORT").cast(T.StringType()))
                                        .withColumn("DESTINATION_AIRPORT", F.col("DESTINATION_AIRPORT").cast(T.StringType()))
                                        .withColumn("DEPARTURE_DELAY", F.col("DEPARTURE_DELAY").cast(T.DoubleType()))
                                        .withColumn("DISTANCE", F.col("DISTANCE").cast(T.DoubleType()))
                                        .withColumn("ARRIVAL_DELAY", F.col("ARRIVAL_DELAY").cast(T.DoubleType()))
                                        .withColumn("DIVERTED", F.col("DIVERTED").cast(T.IntegerType()))
                                        .withColumn("CANCELLED", F.col("CANCELLED").cast(T.IntegerType()))
                                        .withColumn("CANCELLATION_REASON", F.col("CANCELLATION_REASON").cast(T.StringType()))
                                        .withColumn("AIR_SYSTEM_DELAY", F.col("AIR_SYSTEM_DELAY").cast(T.DoubleType()))
                                        .withColumn("SECURITY_DELAY", F.col("SECURITY_DELAY").cast(T.DoubleType()))
                                        .withColumn("AIRLINE_DELAY", F.col("AIRLINE_DELAY").cast(T.DoubleType()))
                                        .withColumn("LATE_AIRCRAFT_DELAY", F.col("LATE_AIRCRAFT_DELAY").cast(T.DoubleType()))
                                        .withColumn("WEATHER_DELAY", F.col("WEATHER_DELAY").cast(T.DoubleType()))
                                        .withColumn("DEPARTURE_HOUR", F.col("DEPARTURE_HOUR").cast(T.IntegerType()))
                                        .withColumn("ARRIVAL_HOUR", F.col("ARRIVAL_HOUR").cast(T.DoubleType()))
                                        )
        else:
            df_unique[df_name] = df_distinct[df_name].dropna()
            df_dropna[df_name] = df_unique[df_name].dropDuplicates(["IATA CODE"])
            if df_name == 'airlines':
                df_airlines = (df_dropna[df_name].withColumnRenamed("IATA CODE", "IATA_CODE")
                                                 .withColumn("IATA_CODE", F.col("IATA_CODE").cast(T.StringType()))
                                                 .withColumn("AIRLINE", F.col("AIRLINE").cast(T.StringType()))
                               )
            else:
                df_airports = (df_dropna[df_name].withColumnRenamed("IATA CODE", "IATA_CODE")
                                                 .withColumn("IATA_CODE", F.col("IATA_CODE").cast(T.StringType()))
                                                 .withColumn("Airport", F.col("Airport").cast(T.StringType()))
                                                 .withColumn("City", F.col("City").cast(T.StringType()))
                                                 .withColumn("Latitude", F.col("Latitude").cast(T.DoubleType()))
                                                 .withColumn("Longitude", F.col("Longitude").cast(T.DoubleType()))
                               )
        count_drop = df_dropna[df_name].count()
        print(f"Количество записей в датафрейме {df_name} всего без пустых записей и без дубляжа "
              f"по столбцу IATA CODE: {count_drop}.")
        df_dropna[df_name].show(5)
    df_flights_pak.printSchema()
    print(f"Количество записей в датафрейме df_flights_pak всего: {df_flights_pak.count()}.")
    df_airlines.printSchema()
    print(f"Количество записей в датафрейме df_airlines всего: {df_airlines.count()}.")
    df_airports.printSchema()
    print(f"Количество записей в датафрейме df_airports всего: {df_airports.count()}.")
    df_with_origin = df_flights_pak.join(df_airports, df_flights_pak.ORIGIN_AIRPORT == df_airports.IATA_CODE, how="inner") \
        .withColumnRenamed("IATA_CODE", "Origin_IATA_CODE") \
        .withColumnRenamed("Airport", "Origin_Airport_Name") \
        .withColumnRenamed("City", "Origin_City") \
        .withColumnRenamed("Latitude", "Origin_Latitude") \
        .withColumnRenamed("Longitude", "Origin_Longitude")

    df_with_dest = df_with_origin.join(df_airports, df_with_origin.DESTINATION_AIRPORT == df_airports.IATA_CODE, how="inner") \
        .withColumnRenamed("IATA_CODE", "Destination_IATA_CODE") \
        .withColumnRenamed("Airport", "Destination_Airport_Name") \
        .withColumnRenamed("City", "Destination_City") \
        .withColumnRenamed("AIRLINE", "Airline_Code") \
        .withColumnRenamed("Latitude", "Destination_Latitude") \
        .withColumnRenamed("Longitude", "Destination_Longitude")

    df_flights = df_with_dest.join(df_airlines, df_with_dest.Airline_Code == df_airlines.IATA_CODE, how="inner") \
        .withColumnRenamed("IATA_CODE", "Airline_IATA_CODE") \
        .withColumnRenamed("AIRLINE", "Airline_Name")
    df_flights.show(5)
    print(f"Количество записей в новом датафрейме df_flights всего: {df_flights.count()}.")
    print(f"Количество записей на одну меньше, потому что в поле DESTINATION_AIRPORT указано "
          f"некорректное значение = 10666. Оно не соответствует ни одному обозначению аэропорта."
          f"Запись некорректная, поэтому исключается из датафрейма.")
    df_airlines_delays = df_flights.groupby("Airline_Name").agg(
                         F.mean("DEPARTURE_DELAY").alias("avg_departure_delay"))
    df_top5_airlines_delays = df_airlines_delays.select("Airline_Name", "avg_departure_delay").orderBy(
                         F.col("avg_departure_delay").desc())
    print(f"Топ 5 авиалиний с самым большим средним временем задержки рейса:")
    df_top5_airlines_delays.show()
    df_airports_canceled = df_flights.groupby("Origin_Airport_Name").agg(
        F.count("Origin_Airport_Name").alias("total"),
        F.count(F.when(F.col("CANCELLED") == 1, True)).alias("cancelled")
    ).withColumn("per_cancelled", F.round((F.col("cancelled") / F.col("total")) * 100, 2))
    df_top_airports_canceled = df_airports_canceled.select("Origin_Airport_Name", "per_cancelled").orderBy(
                               F.col("per_cancelled").desc())
    print(f"Процент отмененных рейсов для каждого аэропорта:")
    df_top_airports_canceled.show()
    pandas_df_top_airports_canceled = df_top_airports_canceled.limit(10).toPandas()
    create_chart(pandas_df_top_airports_canceled['Origin_Airport_Name'],
                 pandas_df_top_airports_canceled['per_cancelled'],
                 'Название аэропорта',
                 'Отмененные рейсы, в %',
                 'График отмененных рейсов для каждого аэропорта, в %:',
                 'sample_data/Top_chart_airports_cancelled.png'
                 )

    df_flights_number = df_flights.withColumn("IS_LONG_HAUL", F.when(F.col("DISTANCE") > 1000.0, True))
    df_top_flights_number = df_flights_number.select("Airline_Name", "FLIGHT_NUMBER"
                                                     , "DISTANCE", "Origin_Airport_Name"
                                                     , "IS_LONG_HAUL").distinct().orderBy(
                               F.col("IS_LONG_HAUL").desc(),
                               F.col("DISTANCE").desc()
    )
    print(f"Дальнемагистральные рейсы (если расстояние больше 1000 миль):")
    df_top_flights_number.show()
    df_flights_day_part = df_flights_number.withColumn("DAY_PART",
        F.when((F.col("DEPARTURE_HOUR") <= 3) | (F.col("DEPARTURE_HOUR") >= 21), "Night")
         .when((F.col("DEPARTURE_HOUR") >= 15) & (F.col("DEPARTURE_HOUR") < 21), "Evening")
         .when((F.col("DEPARTURE_HOUR") >= 9) & (F.col("DEPARTURE_HOUR") < 15), "Daytime")
         .when((F.col("DEPARTURE_HOUR") >= 3) & (F.col("DEPARTURE_HOUR") < 9), "Morning")
    .otherwise("Unknown")
    )
    df_top_flights_day_part = df_flights_day_part.select("Airline_Name", "FLIGHT_NUMBER"
                                                     , "DEPARTURE_HOUR", "Origin_Airport_Name"
                                                     , "DAY_PART").distinct().orderBy(
                               F.col("DAY_PART").desc(),
                               F.col("DEPARTURE_HOUR").desc()
    )
    print(f"Добавили новое поле DAY_PART в датафрейм:")
    df_top_flights_day_part.show()
    df_flights_day_part_group = df_flights_day_part.filter("DEPARTURE_DELAY < 0.0").groupby("DAY_PART").agg(
        F.count("DAY_PART").alias("count_day_part")
    )
    df_flights_delayed = df_flights_day_part_group.select("DAY_PART", "count_day_part").orderBy(
                               F.col("count_day_part").desc()
    )
    print(f"Определим, какое время суток (утро, день, вечер, ночь) чаще всего связано с задержками рейсов:")
    df_flights_delayed.show()
    pandas_df_flights_delayed = df_flights_delayed.limit(10).toPandas()
    create_chart(pandas_df_flights_delayed['DAY_PART'],
                 pandas_df_flights_delayed['count_day_part'],
                 'Время суток',
                 'Количество задержанных рейсов',
                 'График времени суток (утро, день, вечер, ночь) которые чаще всего связаны с задержками рейсов:',
                 'sample_data/Top_chart_flights_delayed.png'
                 )
    df_flights_day_part.printSchema()
    df_flights_limit = df_flights_day_part.limit(10000)

    df_flights_limit.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("overwrite") \
        .save()
    spark.stop()

def select_in_postgres():
    spark = SparkSession.builder \
        .appName("Pyspark_PostgreSQL") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
        .master("local") \
        .getOrCreate()
    try:
        url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
        query = (f"WITH cte AS ("
                 f" SELECT \"Airline_Name\", "
                 f" SUM(CASE "
                 f" WHEN \"DEPARTURE_HOUR\" <= \"ARRIVAL_HOUR\" THEN \"ARRIVAL_HOUR\" - \"DEPARTURE_HOUR\" "
                 f" WHEN \"DEPARTURE_HOUR\" > \"ARRIVAL_HOUR\" THEN \"ARRIVAL_HOUR\" + (24 - \"DEPARTURE_HOUR\") "
                 f" END) OVER(PARTITION BY \"Airline_Name\") AS hours_in_flights "
                 f" FROM {table_name} "
                 f" WHERE \"CANCELLED\" = 0 "
                 f" ORDER BY \"Airline_Name\" DESC "
                 f") "
                 f" SELECT DISTINCT \"Airline_Name\", hours_in_flights FROM cte")
        df_read = spark.read.format("jdbc") \
            .option("url", url) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("dbtable", f'({query}) AS subquery') \
            .load()
        print(f"Выведем авиакомпании - общее время полетов ее самолетов для 10000 записей из таблицы {airlines_data}:")
        df_read.show()
    except Exception as error:
        raise Exception(
            f'Выполнить запрос к таблице {table_name} на выгрузку данных из PostgreSQL не удалось! '
            f'Ошибка: {error}!')
    finally:

        spark.stop()


task_migration_from_spark_to_postgres = PythonOperator(
    task_id='migration_from_spark_to_postgres',
    python_callable=migration_from_spark_to_postgres,
    dag=dag,
)


task_select_in_postgres = PythonOperator(
    task_id='select_in_postgres',
    python_callable=select_in_postgres,
    dag=dag,
)



(
    task_migration_from_spark_to_postgres >>
    task_select_in_postgres
 )
