# Провести аналитику рейсов в аэропортах
Есть **csv** файл, расположенный по ссылке. Скачать можно по ссылке: https://disk.yandex.ru/d/xPxSLEfEt6tkvQ. <br>
Там внутри 3 файла, их нужно скачать и положить в папку в проекте **sample_data**. <br>
В этой же папке **sample_data**, после работы **DAG** <br>
будут лежать графики (сейчас их можно там увидеть), расчитанные по на основе данных из файлов.

## Файл с данными о полетах:

`DATE`: Дата полета. <br>
`DAY_OF_WEEK`: День недели. <br>
`AIRLINE`: Авиалиния. <br>
`FLIGHT_NUMBER`: Номер рейса. <br>
`TAIL_NUMBER`: Номер самолета. <br>
`ORIGIN_AIRPORT`: Аэропорт отправления (IATA код). <br>
`DESTINATION_AIRPORT`: Аэропорт назначения (IATA код). <br>
`DEPARTURE_DELAY`: Задержка при вылете (в минутах). <br>
`DISTANCE`: Расстояние (в милях). <br>
`ARRIVAL_DELAY`: Задержка при прибытии (в минутах). <br>
`DIVERTED`: Был ли рейс перенаправлен. <br>
`CANCELLED`: Был ли рейс отменен. <br>
`CANCELLATION_REASON`: Причина отмены (A - авиалиния, B - погода, C - NAS, D - безопасность). <br>
`AIR_SYSTEM_DELAY`: Задержка по вине системы авиадиспетчерского управления. <br>
`SECURITY_DELAY`: Задержка по причинам безопасности. <br>
`AIRLINE_DELAY`: Задержка по вине авиалинии. <br>
`LATE_AIRCRAFT_DELAY`: Задержка по вине позднего прибытия предыдущего рейса. <br>
`WEATHER_DELAY`: Задержка из-за погоды. <br>
`DEPARTURE_HOUR`: Час вылета. <br>
`ARRIVAL_HOUR`: Час прибытия. <br>

## Файл с данными о аэропортах:

`IATA CODE`: Код IATA. <br>
`Airport`: Название аэропорта. <br>
`City`: Город. <br>
`Latitude`: Широта. <br>
`Longitude`: Долгота. <br>

## Файл с данными о авиалиниях: 

`IATA CODE`: Код IATA. <br>
`AIRLINE`: Название авиалинии. <br>

## Задания:

1. Загрузим файл данных в **DataFrame** **PySpark**. Обязательно выведем количество строк.

2. Убедимся, что данные корректно прочитаны (правильный формат, отсутствие пустых строк). Проверим соответствует ли содержимое полей названиям в файлах.

3. Преобразуем текстовые и числовые поля в соответствующие типы данных (например, дата, число).

4. Найдем топ-5 авиалиний с наибольшей средней задержкой.

5. Вычислим процент отмененных рейсов для каждого аэропорта.

6. Определим, какое время суток (утро, день, вечер, ночь) чаще всего связано с задержками рейсов.

7. Добавим в данные о полетах новые столбцы, рассчитанные на основе существующих данных:

**IS_LONG_HAUL**: Флаг, указывающий, является ли рейс дальнемагистральным (если расстояние больше 1000 миль). <br>
**DAY_PART**: Определим, в какое время суток происходит вылет (утро, день, вечер, ночь). <br>

8. Создадим схему таблицы в **PostgreSQL**, которая будет соответствовать структуре ваших данных в даге.

9. Настроим соединение с **PostgreSQL** из кода из **PySpark**. (обязательно сделать это нужно в **Airflow**)

10. Загрузим только 10.000 строк из **DataFrame** в таблицу в **PostgreSQL**. (обязательно сделать это нужно в **Airflow**)

11. Выполним **SQL** скрипт в **Python-PySpark** скрипте, который выведет компанию - общее время полетов ее самолетов.

### Просмотр данных через DBeaver:
В **DBeaver**, после успешной работы пайплайна, можно посмотреть полученную таблицу `airlines_data` в **PostgreSQL**.


#### Для подключение к **PostgreSQL** используются следующие параметры:
```
    Хост: localhost
    Порт: 5432
    База данных: test
    Пользователь: user
    Пароль: password
```

#### Команды для запуска проекта:
```bash
    git clone https://github.com/MikhalevaAnna/pipeline_analyze_airports.git
    cd pipeline_analyze_airports
    docker build -t airflow-with-java-airlines .
    docker-compose up --build
```
    
- Далее идем по адресу - **http://localhost:8080**
- Логин и пароль - **airflow**.


## Результат выполнения папйплайна:

Начальная структура датафрейма **airlines**:
```
 |-- IATA CODE: string (nullable = true)
 |-- AIRLINE: string (nullable = true)
```
```
+---------+--------------------+
|IATA CODE|             AIRLINE|
+---------+--------------------+
|       PK|Pakistan Internat...|
|       PA|             Airblue|
|       PF|             AirSial|
|       ER|           SereneAir|
+---------+--------------------+
```
Количество записей в датафрейме airlines всего: 4.
Количество записей в датафрейме всего без дубляжа: 4.
```
+---------+--------------------+
|IATA CODE|             AIRLINE|
+---------+--------------------+
|       ER|           SereneAir|
|       PF|             AirSial|
|       PK|Pakistan Internat...|
|       PA|             Airblue|
+---------+--------------------+
```
Количество записей в датафрейме airlines всего без пустых записей и без дубляжа по столбцу IATA CODE: 4.
```
+---------+--------------------+
|IATA CODE|             AIRLINE|
+---------+--------------------+
|       ER|           SereneAir|
|       PA|             Airblue|
|       PF|             AirSial|
|       PK|Pakistan Internat...|
+---------+--------------------+
```
**airports**
Начальная структура датафрейма airports:
```
 |-- IATA CODE: string (nullable = true)
 |-- Airport: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Latitude: double (nullable = true)
 |-- Longitude: double (nullable = true)
```
```
+---------+--------------------+--------------------+--------+---------+
|IATA CODE|             Airport|                City|Latitude|Longitude|
+---------+--------------------+--------------------+--------+---------+
|      KHI|Karachi Jinnah In...|             Karachi| 24.9067|  67.1608|
|      ISB|Islamabad Interna...|Islamabad/Rawalpindi| 33.5658|  72.8257|
|      LHE|Lahore Allama Iqb...|              Lahore| 31.5214|  74.4039|
|      LYP|Faisalabad Intern...|          Faisalabad|  31.365|  72.9947|
|      SKT|Sialkot Internati...|             Sialkot| 32.5356|  74.3639|
|      MUX|Multan Internatio...|              Multan| 30.2033|  71.4192|
|      PEW|Peshawar Bacha Kh...|            Peshawar| 33.9939|  71.5147|
|      GWD|Gwadar Internatio...|              Gwadar| 25.2322|  62.3272|
|      UET|Quetta Internatio...|              Quetta|   30.24|    66.94|
|      DEA|Dera Ghazi Khan I...|     Dera Ghazi Khan| 29.9608|   7.4858|
+---------+--------------------+--------------------+--------+---------+
```
Количество записей в датафрейме airports всего: 49.
Количество записей в датафрейме всего без дубляжа: 49.
```
+---------+--------------------+------------+--------+---------+
|IATA CODE|             Airport|        City|Latitude|Longitude|
+---------+--------------------+------------+--------+---------+
|      MFG|Muzaffarabad Airport|Muzaffarabad| 34.3392|  73.5086|
|      MBI|Maai Bakhtawar In...|    Islamkot| 24.8472|   7.0964|
|      RAZ|   Rawalakot Airport|   Rawalakot| 33.8497|  73.7983|
|      PZH|        Zhob Airport|        Zhob| 31.3583|  69.4636|
|      ABT|  Abbottabad Airport|  Abbottabad|   34.15|  73.2167|
+---------+--------------------+------------+--------+---------+
```
Количество записей в датафрейме airports всего без пустых записей и без дубляжа по столбцу IATA CODE: 48.
```
+---------+------------------+----------+--------+---------+
|IATA CODE|           Airport|      City|Latitude|Longitude|
+---------+------------------+----------+--------+---------+
|      ABT|Abbottabad Airport|Abbottabad|   34.15|  73.2167|
|      BHV|Bahawalpur Airport|Bahawalpur| 29.3481|  71.7178|
|      BNP|     Bannu Airport|     Bannu| 32.9719|   7.5242|
|      CHA|   Chashma Airport|   Chashma| 32.4244|  71.4583|
|      CHB|    Chilas Airport|    Chilas| 35.4269|   74.085|
+---------+------------------+----------+--------+---------+
```
**flights_pak**
Начальная структура датафрейма flights_pak:
```
 |-- DATE: date (nullable = true)
 |-- DAY_OF_WEEK: string (nullable = true)
 |-- AIRLINE: string (nullable = true)
 |-- FLIGHT_NUMBER: integer (nullable = true)
 |-- TAIL_NUMBER: string (nullable = true)
 |-- ORIGIN_AIRPORT: string (nullable = true)
 |-- DESTINATION_AIRPORT: string (nullable = true)
 |-- DEPARTURE_DELAY: double (nullable = true)
 |-- DISTANCE: double (nullable = true)
 |-- ARRIVAL_DELAY: double (nullable = true)
 |-- DIVERTED: integer (nullable = true)
 |-- CANCELLED: integer (nullable = true)
 |-- CANCELLATION_REASON: string (nullable = true)
 |-- AIR_SYSTEM_DELAY: double (nullable = true)
 |-- SECURITY_DELAY: double (nullable = true)
 |-- AIRLINE_DELAY: double (nullable = true)
 |-- LATE_AIRCRAFT_DELAY: double (nullable = true)
 |-- WEATHER_DELAY: double (nullable = true)
 |-- DEPARTURE_HOUR: integer (nullable = true)
 |-- ARRIVAL_HOUR: double (nullable = true)
```
```
+----------+-----------+-------+-------------+-----------+--------------+-------------------+---------------+------------------+-------------+--------+---------+-------------------+----------------+--------------+-------------+-------------------+-------------+--------------+------------+
|      DATE|DAY_OF_WEEK|AIRLINE|FLIGHT_NUMBER|TAIL_NUMBER|ORIGIN_AIRPORT|DESTINATION_AIRPORT|DEPARTURE_DELAY|          DISTANCE|ARRIVAL_DELAY|DIVERTED|CANCELLED|CANCELLATION_REASON|AIR_SYSTEM_DELAY|SECURITY_DELAY|AIRLINE_DELAY|LATE_AIRCRAFT_DELAY|WEATHER_DELAY|DEPARTURE_HOUR|ARRIVAL_HOUR|
+----------+-----------+-------+-------------+-----------+--------------+-------------------+---------------+------------------+-------------+--------+---------+-------------------+----------------+--------------+-------------+-------------------+-------------+--------------+------------+
|2015-01-01|  Wednesday|     PA|           98|     N407AS|           TUK|                DEA|          -11.0|1001.6897451334537|        -22.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         3.0|
|2015-01-01|  Wednesday|     PF|         2336|     N3KUAA|           SKT|                ISB|           -8.0| 751.7631948625326|         -9.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         4.0|
|2015-01-01|  Wednesday|     PK|          840|     N171US|           KDU|                BNP|           -2.0| 761.3975516756974|          5.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         4.0|
|2015-01-01|  Wednesday|     PF|          258|     N3HYAA|           SKT|                DSK|           -5.0| 748.3628336343568|         -9.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         4.0|
|2015-01-01|  Wednesday|     PA|          135|     N527AS|           DEA|                TUK|           -1.0|1001.6897451334537|        -21.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         3.0|
|2015-01-01|  Wednesday|     PA|          806|     N3730B|           KDU|                UET|           -5.0| 961.7355007023881|          8.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         3.0|
|2015-01-01|  Wednesday|     PF|          612|     N635NK|           GWD|                UET|           -6.0|  1043.91089704997|        -17.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         3.0|
|2015-01-01|  Wednesday|     PK|         2013|     N584UW|           SKT|                BNP|           14.0| 809.8526991772026|        -10.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         4.0|
|2015-01-01|  Wednesday|     PF|         1112|     N3LAAA|           KDU|                LHE|          -11.0| 997.1559301625526|        -13.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         3.0|
|2015-01-01|  Wednesday|     PA|         1173|     N826DN|           GWD|                KHI|            3.0| 916.9640778647401|        -15.0|       0|        0|               null|            null|          null|         null|               null|         null|             0|         3.0|
+----------+-----------+-------+-------------+-----------+--------------+-------------------+---------------+------------------+-------------+--------+---------+-------------------+----------------+--------------+-------------+-------------------+-------------+--------------+------------+
```
Количество записей в датафрейме **flights_pak** всего: 5819079. <br>
Количество записей в датафрейме всего без дубляжа: 5819079.
```
+----------+-----------+-------+-------------+-----------+--------------+-------------------+---------------+------------------+-------------+--------+---------+-------------------+----------------+--------------+-------------+-------------------+-------------+--------------+------------+
|      DATE|DAY_OF_WEEK|AIRLINE|FLIGHT_NUMBER|TAIL_NUMBER|ORIGIN_AIRPORT|DESTINATION_AIRPORT|DEPARTURE_DELAY|          DISTANCE|ARRIVAL_DELAY|DIVERTED|CANCELLED|CANCELLATION_REASON|AIR_SYSTEM_DELAY|SECURITY_DELAY|AIRLINE_DELAY|LATE_AIRCRAFT_DELAY|WEATHER_DELAY|DEPARTURE_HOUR|ARRIVAL_HOUR|
+----------+-----------+-------+-------------+-----------+--------------+-------------------+---------------+------------------+-------------+--------+---------+-------------------+----------------+--------------+-------------+-------------------+-------------+--------------+------------+
|2015-02-20|   Thursday|     PA|           65|     N703AS|           DEA|                GIL|           -1.0|1219.3128637367047|         null|       1|        0|               null|            null|          null|         null|               null|         null|             7|        null|
|2015-12-24|  Wednesday|     PF|          163|     N632NK|           LHE|                KHI|           -6.0|1204.8613285169577|         null|       1|        0|               null|            null|          null|         null|               null|         null|             7|        null|
|2015-07-23|  Wednesday|     PK|          169|     N366SW|           SYW|                KHI|           -2.0|1222.4298615291993|         null|       1|        0|               null|            null|          null|         null|               null|         null|            11|        null|
|2015-08-19|    Tuesday|     PF|          224|     N491AA|           GWD|                ISB|           -5.0| 982.9877583784869|         null|       1|        0|               null|            null|          null|         null|               null|         null|            15|        null|
|2015-06-29|     Sunday|     PK|          242|     N938WN|           LYP|                RYK|           -6.0| 973.9201284366848|         null|       1|        0|               null|            null|          null|         null|               null|         null|            18|        null|
+----------+-----------+-------+-------------+-----------+--------------+-------------------+---------------+------------------+-------------+--------+---------+-------------------+----------------+--------------+-------------+-------------------+-------------+--------------+------------+
```
Количество записей в датафрейме **flights_pak** всего без пустых записей и без дубляжа по столбцу **IATA CODE**: 5819079.
```
+----------+-----------+-------+-------------+-----------+--------------+-------------------+---------------+------------------+-------------+--------+---------+-------------------+----------------+--------------+-------------+-------------------+-------------+--------------+------------+
|      DATE|DAY_OF_WEEK|AIRLINE|FLIGHT_NUMBER|TAIL_NUMBER|ORIGIN_AIRPORT|DESTINATION_AIRPORT|DEPARTURE_DELAY|          DISTANCE|ARRIVAL_DELAY|DIVERTED|CANCELLED|CANCELLATION_REASON|AIR_SYSTEM_DELAY|SECURITY_DELAY|AIRLINE_DELAY|LATE_AIRCRAFT_DELAY|WEATHER_DELAY|DEPARTURE_HOUR|ARRIVAL_HOUR|
+----------+-----------+-------+-------------+-----------+--------------+-------------------+---------------+------------------+-------------+--------+---------+-------------------+----------------+--------------+-------------+-------------------+-------------+--------------+------------+
|2015-02-20|   Thursday|     PA|           65|     N703AS|           DEA|                GIL|           -1.0|1219.3128637367047|          0.0|       1|        0|               null|             0.0|           0.0|          0.0|                0.0|          0.0|             7|         0.0|
|2015-12-24|  Wednesday|     PF|          163|     N632NK|           LHE|                KHI|           -6.0|1204.8613285169577|          0.0|       1|        0|               null|             0.0|           0.0|          0.0|                0.0|          0.0|             7|         0.0|
|2015-07-23|  Wednesday|     PK|          169|     N366SW|           SYW|                KHI|           -2.0|1222.4298615291993|          0.0|       1|        0|               null|             0.0|           0.0|          0.0|                0.0|          0.0|            11|         0.0|
|2015-08-19|    Tuesday|     PF|          224|     N491AA|           GWD|                ISB|           -5.0| 982.9877583784869|          0.0|       1|        0|               null|             0.0|           0.0|          0.0|                0.0|          0.0|            15|         0.0|
|2015-06-29|     Sunday|     PK|          242|     N938WN|           LYP|                RYK|           -6.0| 973.9201284366848|          0.0|       1|        0|               null|             0.0|           0.0|          0.0|                0.0|          0.0|            18|         0.0|
+----------+-----------+-------+-------------+-----------+--------------+-------------------+---------------+------------------+-------------+--------+---------+-------------------+----------------+--------------+-------------+-------------------+-------------+--------------+------------+
```

```
 |-- DATE: date (nullable = true)
 |-- DAY_OF_WEEK: string (nullable = true)
 |-- AIRLINE: string (nullable = true)
 |-- FLIGHT_NUMBER: integer (nullable = true)
 |-- TAIL_NUMBER: string (nullable = true)
 |-- ORIGIN_AIRPORT: string (nullable = true)
 |-- DESTINATION_AIRPORT: string (nullable = true)
 |-- DEPARTURE_DELAY: double (nullable = false)
 |-- DISTANCE: double (nullable = false)
 |-- ARRIVAL_DELAY: double (nullable = false)
 |-- DIVERTED: integer (nullable = true)
 |-- CANCELLED: integer (nullable = true)
 |-- CANCELLATION_REASON: string (nullable = true)
 |-- AIR_SYSTEM_DELAY: double (nullable = false)
 |-- SECURITY_DELAY: double (nullable = false)
 |-- AIRLINE_DELAY: double (nullable = false)
 |-- LATE_AIRCRAFT_DELAY: double (nullable = false)
 |-- WEATHER_DELAY: double (nullable = false)
 |-- DEPARTURE_HOUR: integer (nullable = true)
 |-- ARRIVAL_HOUR: double (nullable = false)
```
Количество записей в датафрейме **df_flights_pak** всего: 5819079.
```
 |-- IATA_CODE: string (nullable = true)
 |-- AIRLINE: string (nullable = true)
 ```
Количество записей в датафрейме **df_airlines** всего: 4.
```
 |-- IATA_CODE: string (nullable = true)
 |-- Airport: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Latitude: double (nullable = true)
 |-- Longitude: double (nullable = true)
```
Количество записей в датафрейме **df_airports** всего: 48.<br>
Количество записей в новом датафрейме **df_flights** всего: 5819078. <br>
Количество записей на одну меньше, потому что в поле **DESTINATION_AIRPORT** указано некорректное значение = 10666. Оно не соответствует ни одному обозначению аэропорта.Запись некорректная, поэтому исключается из датафрейма. <br>
Топ 5 авиалиний с самым большим средним временем задержки рейса:
```
+--------------------+-------------------+
|        Airline_Name|avg_departure_delay|
+--------------------+-------------------+
|Pakistan Internat...|  9.466105826827128|
|           SereneAir|  9.228160771321443|
|             AirSial|  9.129130578365384|
|             Airblue|  9.008008316476188|
+--------------------+-------------------+
```
Процент отмененных рейсов для каждого аэропорта:
```
+--------------------+-------------+
| Origin_Airport_Name|per_cancelled|
+--------------------+-------------+
|  Bahawalpur Airport|         3.82|
|Islamabad Interna...|         2.63|
|  Abbottabad Airport|         2.59|
|      Chilas Airport|         2.42|
|        Zhob Airport|         2.42|
|Lahore Allama Iqb...|          2.4|
|Sehwan Sharif Air...|         2.36|
|       Sawan Airport|         2.22|
|     Chashma Airport|         2.17|
|Jacobabad Airport...|         2.15|
|   Rawalakot Airport|          2.1|
|  Moenjodaro Airport|         1.97|
|   Nawabshah Airport|         1.86|
|Skardu Internatio...|         1.83|
|Saidu Sharif Airport|         1.75|
|      Walton Airport|         1.71|
|      Gilgit Airport|         1.64|
|   Kadanwari Airport|          1.6|
|         Sui Airport|         1.54|
|Muzaffarabad Airport|         1.51|
+--------------------+-------------+
```
Дальнемагистральные рейсы (если расстояние больше 1000 миль):
```
+--------------------+-------------+------------------+--------------------+------------+
|        Airline_Name|FLIGHT_NUMBER|          DISTANCE| Origin_Airport_Name|IS_LONG_HAUL|
+--------------------+-------------+------------------+--------------------+------------+
|             AirSial|         1297|1406.0493678506923|  Abbottabad Airport|        true|
|             Airblue|           64|1403.2157334938793|  Bahawalpur Airport|        true|
|             Airblue|           65|1403.2157334938793|Begum Nusrat Bhut...|        true|
|             Airblue|          707|1401.7989163154725|Sialkot Internati...|        true|
|             Airblue|           77| 1400.382099137066|      Gilgit Airport|        true|
|             Airblue|           71| 1400.382099137066|   Kadanwari Airport|        true|
|           SereneAir|         4717|1398.1151916516155|Lahore Allama Iqb...|        true|
|Pakistan Internat...|         3925|1397.2651013445716|Peshawar Bacha Kh...|        true|
|Pakistan Internat...|         4992|1394.4314669877583|     Panjgur Airport|        true|
|           SereneAir|         2373|1394.4314669877583|Dera Ismail Khan ...|        true|
|           SereneAir|         5303|1393.0146498093518|Islamabad Interna...|        true|
|           SereneAir|         5377|1393.0146498093518|Sehwan Sharif Air...|        true|
|           SereneAir|         5419|1393.0146498093518|Sehwan Sharif Air...|        true|
|           SereneAir|         2825|1393.0146498093518|Sehwan Sharif Air...|        true|
|           SereneAir|         5413|1393.0146498093518|Sehwan Sharif Air...|        true|
|           SereneAir|         2637|1393.0146498093518|Islamabad Interna...|        true|
|           SereneAir|         5393|1393.0146498093518|Sehwan Sharif Air...|        true|
|           SereneAir|         2635|1393.0146498093518|Sehwan Sharif Air...|        true|
|           SereneAir|         2836|1393.0146498093518|Sehwan Sharif Air...|        true|
|           SereneAir|         5300|1393.0146498093518|Islamabad Interna...|        true|
+--------------------+-------------+------------------+--------------------+------------+
```
Добавили новое поле **DAY_PART** в датафрейм:
```
+--------------------+-------------+--------------+--------------------+--------+
|        Airline_Name|FLIGHT_NUMBER|DEPARTURE_HOUR| Origin_Airport_Name|DAY_PART|
+--------------------+-------------+--------------+--------------------+--------+
|             AirSial|         2604|            23|Lahore Allama Iqb...|   Night|
|             Airblue|          754|            23|Peshawar Bacha Kh...|   Night|
|             Airblue|         1043|            23|Faisalabad Intern...|   Night|
|             Airblue|          782|            23|Karachi Jinnah In...|   Night|
|             Airblue|          698|            23|Dera Ghazi Khan I...|   Night|
|             Airblue|          669|            23|Skardu Internatio...|   Night|
|             AirSial|          219|            23|Sialkot Internati...|   Night|
|             Airblue|         1460|            23|Gwadar Internatio...|   Night|
|             AirSial|          954|            23|Gwadar Internatio...|   Night|
|             Airblue|         1254|            23|Sialkot Internati...|   Night|
|             Airblue|         1261|            23|Turbat Internatio...|   Night|
|Pakistan Internat...|         2084|            23|Peshawar Bacha Kh...|   Night|
|           SereneAir|         2725|            23|Peshawar Bacha Kh...|   Night|
|Pakistan Internat...|         1931|            23|Peshawar Bacha Kh...|   Night|
|             Airblue|          658|            23|Peshawar Bacha Kh...|   Night|
|             AirSial|         3574|            23|Lahore Allama Iqb...|   Night|
|Pakistan Internat...|         4446|            23|Islamabad Interna...|   Night|
|           SereneAir|          745|            23|   Nawabshah Airport|   Night|
|             Airblue|         1730|            23|Skardu Internatio...|   Night|
|Pakistan Internat...|         1604|            23|     Chashma Airport|   Night|
+--------------------+-------------+--------------+--------------------+--------+
```

Определим, какое время суток (утро, день, вечер, ночь) чаще всего связано с задержками рейсов:
```
+--------+--------------+
|DAY_PART|count_day_part|
+--------+--------------+
| Daytime|       1200059|
| Evening|        966135|
| Morning|        927072|
|   Night|        184681|
+--------+--------------+
```
```
 |-- DATE: date (nullable = true)
 |-- DAY_OF_WEEK: string (nullable = true)
 |-- Airline_Code: string (nullable = true)
 |-- FLIGHT_NUMBER: integer (nullable = true)
 |-- TAIL_NUMBER: string (nullable = true)
 |-- ORIGIN_AIRPORT: string (nullable = true)
 |-- DESTINATION_AIRPORT: string (nullable = true)
 |-- DEPARTURE_DELAY: double (nullable = false)
 |-- DISTANCE: double (nullable = false)
 |-- ARRIVAL_DELAY: double (nullable = false)
 |-- DIVERTED: integer (nullable = true)
 |-- CANCELLED: integer (nullable = true)
 |-- CANCELLATION_REASON: string (nullable = true)
 |-- AIR_SYSTEM_DELAY: double (nullable = false)
 |-- SECURITY_DELAY: double (nullable = false)
 |-- AIRLINE_DELAY: double (nullable = false)
 |-- LATE_AIRCRAFT_DELAY: double (nullable = false)
 |-- WEATHER_DELAY: double (nullable = false)
 |-- DEPARTURE_HOUR: integer (nullable = true)
 |-- ARRIVAL_HOUR: double (nullable = false)
 |-- Origin_IATA_CODE: string (nullable = true)
 |-- Origin_Airport_Name: string (nullable = true)
 |-- Origin_City: string (nullable = true)
 |-- Origin_Latitude: double (nullable = true)
 |-- Origin_Longitude: double (nullable = true)
 |-- Destination_IATA_CODE: string (nullable = true)
 |-- Destination_Airport_Name: string (nullable = true)
 |-- Destination_City: string (nullable = true)
 |-- Destination_Latitude: double (nullable = true)
 |-- Destination_Longitude: double (nullable = true)
 |-- Airline_IATA_CODE: string (nullable = true)
 |-- Airline_Name: string (nullable = true)
 |-- IS_LONG_HAUL: boolean (nullable = true)
 |-- DAY_PART: string (nullable = false)
 ```

Выведем авиакомпании - общее время полетов ее самолетов для 10000 записей из таблицы **airlines_data** :
```
+--------------------+----------------+
|        Airline_Name|hours_in_flights|
+--------------------+----------------+
|Pakistan Internat...|          5514.0|
|             Airblue|          6699.0|
|           SereneAir|          3110.0|
|             AirSial|          4340.0|
+--------------------+----------------+
```
