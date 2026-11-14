# Поток Prefect со Spark на YARN кластера Team B

Домашка номер 5: в рамках потока Prefect запустить Spark под управлением нашего YARN, подключиться к нашему HDFS, прочитать данные, преобразовать их и сохранить как таблицу.

Как и третью с четвёртой, эту домашку мы оформили как описание: ниже все шаги, команды, конфиги и сам код потока по порядку. Всё работает поверх того, что уже развёрнуто: HDFS из первой домашки, YARN из второй, метастор Hive из третьей и Spark из четвёртой. Team A не трогаем.

## Что и как закрывает задание

Весь процесс - это один поток (flow) Prefect из пяти задач (task), по задаче на пункт задания:

1. Задача start_spark создаёт сессию Spark с master yarn, то есть приложение получает контейнеры у нашего ResourceManager. В лог пишутся master и id приложения в YARN, а само приложение видно в yarn application -list и в YARN UI.
2. Задача connect_hdfs через эту сессию подключается к нашему HDFS по адресу hdfs://team-28-nn:9100, проверяет, что входной каталог существует, и пишет в лог список файлов в нём. Если данных нет, поток падает на этом шаге с понятной ошибкой.
3. Задача read_events читает CSV, который заранее загружен в HDFS.
4. Задача transform делает несколько трансформаций: приведение типов, чистку значений, фильтр битых строк, новые колонки (дата и час события) и агрегацию по дате и часу.
5. Задача save_table сохраняет результат как таблицу team28b.events_hourly_stats в метасторе Hive, с партициями по дате, и сразу перечитывает её для проверки.

Сессия Spark останавливается в конце потока при любом исходе, даже если какая-то задача упала, чтобы приложение не висело в YARN.

## Почему Prefect, а не Airflow

В задании можно выбрать любой из двух. Мы взяли Prefect, потому что он легче. Airflow - это несколько постоянных сервисов (scheduler, webserver или api-server, dag-processor) и отдельная база для метаданных, вместе им нужно 1-2 ГБ памяти. А у нас на каждом узле около 2.5 ГБ, и они уже заняты DataNode, NodeManager, а на team-28-01 ещё PostgreSQL и Hive.

Prefect ставится одним пакетом pip, поток - это обычный Python-скрипт с декораторами @flow и @task. Сервер Prefect нужен только для истории запусков и UI, базу он хранит в одном файле SQLite.

Второй плюс: поток на Python, а Spark у нас уже на PySpark с четвёртой домашки. Сессию Spark и DataFrame'ы можно просто передавать из задачи в задачу внутри одного процесса.

## Раскладка по узлам

Всё ставим на team-28-01 (10.28.0.13), туда же, где Spark и Hive из прошлых домашек. На этом узле работают сервер Prefect и сам поток. Поток запускает драйвер Spark у себя в процессе, в режиме client, а executor'ы YARN, как и в четвёртой домашке, запускает в контейнерах на наших NodeManager'ах на всех трёх узлах.

На team-28-nn и team-28-00 ничего ставить не нужно.

Подключение как раньше: с ноутбука на edge team-28-en под team28b, оттуда на team-28-01 по ключу team28b_internal.

## Версии

Prefect 3.8.8. Ему нужен Python 3.10 или новее.

Spark 3.5.5 уже стоит с четвёртой домашки в ~/apps/spark, отдельно pyspark через pip не ставим. Модуль pyspark берём прямо из дистрибутива Spark через PYTHONPATH, чтобы версия Python-части точно совпадала с jar-файлами и чтобы использовались наши spark-defaults.conf и spark-env.sh.

Prefect ставим не в системный Python, а в отдельное окружение: sudo нет, а pip в системный Python на Ubuntu ставить нельзя. Окружение создаём через micromamba, который уже стоит с третьей домашки (через него ставили PostgreSQL). Python в окружении берём той же версии, что системный python3. Так у драйвера на team-28-01 и у executor'ов на других узлах одна и та же версия Python. Наш код использует только операции DataFrame, без своих Python-функций, поэтому executor'ы Python вообще не запускают. Но если кто-то потом добавит UDF, разные версии Python у драйвера и executor'ов сразу дадут ошибку.

## Где всё лежит

Окружение Python с Prefect в ~/apps/prefect-env. Код потока в ~/prefect/flows, база и настройки Prefect в ~/prefect/home, логи в ~/prefect/logs, pid сервера в ~/prefect/run.

В HDFS: входные данные в /user/team28b/spark/input/events (тот же каталог, что в четвёртой домашке), данные итоговой таблицы в /user/team28b/prefect/tables/events_hourly_stats.

## Порты

Новый постоянный порт один - сервер Prefect. По умолчанию это 4200, его может занять Team A, поэтому у нас 4300, как обычно со сдвигом +100.

Spark UI драйвера, пока работает поток, на 4140 (это уже задано в spark-defaults.conf из четвёртой домашки). Остальные порты наши из прошлых домашек: HDFS 9100, клиентский порт ResourceManager 9033, ResourceManager UI 9088, Metastore 9183, HiveServer2 10100.

Проверяем на team-28-01, что 4300 свободен:

```bash
ss -ltn | grep -E ':4300\b' || echo СВОБОДЕН
```

## Что должно работать перед стартом

Должны быть запущены HDFS и YARN, а из Hive - PostgreSQL и Metastore. HiveServer2 нужен только для последней проверки через beeline.

Spark должен быть установлен и настроен по четвёртой домашке: ~/apps/spark, spark-env.sh, spark-defaults.conf с master yarn и spark.driver.host, hive-site.xml для Spark, jar-файлы в HDFS. Ещё из четвёртой домашки нужен yarn.resourcemanager.address = 10.28.0.11:9033 в yarn-site.xml, без него Spark не достучится до ResourceManager.

Проверяем на team-28-01:

```bash
hdfs dfsadmin -report | grep -E 'Live datanodes'
yarn node -list
ss -ltn | grep -E ':(9183|10100)\b'
spark-submit --version
```

hdfs должен показать 3 живых DataNode, yarn - 3 ноды в состоянии RUNNING, должен слушаться 9183 (Metastore), а spark-submit показать версию 3.5.5.

## Входные данные в HDFS

По заданию данные должны быть загружены в HDFS заранее, до запуска потока. Мы используем тот же CSV, что в четвёртой домашке: 5000 событий за 5 дней, у 20 строк нет времени события. Смотрим, на месте ли он:

```bash
hdfs dfs -ls /user/team28b/spark/input/events
```

Если файла там нет, генерируем и кладём его заново, тем же awk, что и в прошлый раз:

```bash
mkdir -p ~/spark/data
awk -v n=5000 'BEGIN {
  srand(28)
  split("view,click,purchase,refund,VIEW, Click", t, ",")
  print "event_id,user_id,event_type,amount,event_time"
  for (i = 1; i <= n; i++) {
    ts = sprintf("2026-10-%02d %02d:%02d:%02d", 1+int(rand()*5), int(rand()*24), int(rand()*60), int(rand()*60))
    if (i % 250 == 0) ts = ""
    printf "%d,%d,%s,%.2f,%s\n", i, 1+int(rand()*200), t[1+int(rand()*6)], rand()*1000, ts
  }
}' > ~/spark/data/events.csv

hdfs dfs -mkdir -p /user/team28b/spark/input/events
hdfs dfs -put -f ~/spark/data/events.csv /user/team28b/spark/input/events/
```

## Окружение Python с Prefect

Смотрим версию системного Python:

```bash
PYV=$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')
echo $PYV
```

На Ubuntu 22.04 будет 3.10, на 24.04 - 3.12, обе подходят. Если вдруг окажется меньше 3.10, ставим PYV=3.11.

Создаём окружение с этой версией Python и ставим в него Prefect:

```bash
~/apps/bin/micromamba create -y -r ~/apps/mamba -p ~/apps/prefect-env -c conda-forge python=$PYV pip
~/apps/prefect-env/bin/pip install prefect==3.8.8
~/apps/prefect-env/bin/prefect version
```

Создаём каталоги:

```bash
mkdir -p ~/prefect/flows ~/prefect/home ~/prefect/logs ~/prefect/run
```

## Переменные окружения

SPARK_HOME уже прописан в ~/.bashrc с четвёртой домашки, добавляем остальное:

```bash
echo 'export PATH=$HOME/apps/prefect-env/bin:$PATH' >> ~/.bashrc
echo 'export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*-src.zip)' >> ~/.bashrc
echo 'export PREFECT_HOME=$HOME/prefect/home' >> ~/.bashrc
echo 'export PREFECT_API_URL=http://127.0.0.1:4300/api' >> ~/.bashrc
echo 'export PREFECT_SERVER_ANALYTICS_ENABLED=false' >> ~/.bashrc
source ~/.bashrc
```

Что тут зачем.

PATH ставит python и prefect из нашего окружения впереди системных. Через PYTHONPATH этот python находит модуль pyspark и библиотеку py4j внутри дистрибутива Spark. Имя zip-файла py4j зависит от версии Spark, поэтому ищем его через ls, а не пишем руками.

PREFECT_HOME держит базу и настройки Prefect в нашем каталоге. PREFECT_API_URL говорит потоку отправлять информацию о запусках нашему серверу на 4300. Если сервер не запущен, Prefect поднимет временный сервер на время одного запуска, но тогда история запусков потом не сохранится. Последняя переменная выключает отправку анонимной статистики Prefect в интернет.

Проверяем, что python видит pyspark:

```bash
python -c "import pyspark, prefect; print(pyspark.__version__, prefect.__version__)"
```

Должно напечатать 3.5.5 3.8.8.

## Сервер Prefect

Запускаем как сервисы Hive в третьей домашке, через nohup и с сохранением pid:

```bash
PREFECT_UI_API_URL=http://localhost:4300/api nohup prefect server start --host 0.0.0.0 --port 4300 > ~/prefect/logs/server.out 2>&1 &
echo $! > ~/prefect/run/server.pid
```

Ждём несколько секунд и проверяем:

```bash
curl -s http://127.0.0.1:4300/api/health
```

Должно вернуть true.

Сервер слушает на всех интерфейсах, чтобы до UI можно было дойти через SSH-туннель с edge. PREFECT_UI_API_URL нужен вот зачем: страница UI открывается у нас в браузере на ноутбуке и оттуда сама ходит в API сервера. Без этой переменной она пойдёт по адресу, который на ноутбуке не работает, и откроется пустой. С ней UI ходит в API через тот же туннель, по localhost:4300.

Остановка, только по своему pid:

```bash
kill $(cat ~/prefect/run/server.pid)
```

## Код потока

Сохраняем этот код в файл ~/prefect/flows/spark_events_flow.py:

```python
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://team-28-nn:9100"
INPUT_PATH = HDFS + "/user/team28b/spark/input/events"
TABLE = "team28b.events_hourly_stats"
TABLE_PATH = HDFS + "/user/team28b/prefect/tables/events_hourly_stats"


@task(cache_policy=NO_CACHE)
def start_spark():
    log = get_run_logger()
    spark = (
        SparkSession.builder
        .master("yarn")
        .appName("team28b-prefect-events")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
        .getOrCreate()
    )
    sc = spark.sparkContext
    log.info("Spark %s, master: %s, application id: %s", spark.version, sc.master, sc.applicationId)
    return spark


@task(cache_policy=NO_CACHE)
def connect_hdfs(spark):
    log = get_run_logger()
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(HDFS), conf)
    path = jvm.org.apache.hadoop.fs.Path(INPUT_PATH)
    if not fs.exists(path):
        raise FileNotFoundError(INPUT_PATH + " нет в HDFS, сначала загрузите данные")
    files = [st.getPath().getName() for st in fs.listStatus(path)]
    log.info("подключились к %s, во входном каталоге: %s", fs.getUri().toString(), files)
    return INPUT_PATH


@task(cache_policy=NO_CACHE)
def read_events(spark, path):
    log = get_run_logger()
    raw = spark.read.option("header", "true").csv(path)
    log.info("прочитано строк: %d, колонки: %s", raw.count(), raw.dtypes)
    return raw


@task(cache_policy=NO_CACHE)
def transform(raw):
    log = get_run_logger()
    # типы, чистка, фильтр битых строк, новые колонки
    events = (
        raw
        .withColumn("user_id", F.col("user_id").cast("int"))
        .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
        .filter(F.col("event_time").isNotNull())
        .withColumn("event_date", F.date_format("event_time", "yyyy-MM-dd"))
        .withColumn("event_hour", F.hour("event_time"))
    )
    log.info("после очистки: %d", events.count())

    # агрегация по дате и часу
    stats = (
        events
        .groupBy("event_date", "event_hour")
        .agg(
            F.count("*").alias("events_cnt"),
            F.countDistinct("user_id").alias("users_cnt"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases_cnt"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("amount"))).alias("revenue"),
        )
        .select(
            "event_hour",
            "events_cnt",
            "users_cnt",
            F.col("purchases_cnt").cast("bigint").alias("purchases_cnt"),
            F.coalesce(F.col("revenue"), F.lit(0)).cast("decimal(20,2)").alias("revenue"),
            "event_date",
        )
    )
    return stats


@task(cache_policy=NO_CACHE)
def save_table(spark, stats):
    log = get_run_logger()
    spark.sql("CREATE DATABASE IF NOT EXISTS team28b")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE} (
            event_hour INT,
            events_cnt BIGINT,
            users_cnt BIGINT,
            purchases_cnt BIGINT,
            revenue DECIMAL(20,2)
        )
        PARTITIONED BY (event_date STRING)
        STORED AS ORC
        LOCATION '{TABLE_PATH}'
    """)
    # колонка партиции в select последняя, insertInto сопоставляет по порядку
    stats.write.insertInto(TABLE, overwrite=True)

    saved = spark.table(TABLE)
    rows = saved.count()
    parts = [r[0] for r in spark.sql(f"SHOW PARTITIONS {TABLE}").collect()]
    total = saved.agg(F.sum("events_cnt")).first()[0]
    log.info("таблица %s: строк %d, сумма events_cnt %s, партиции %s", TABLE, rows, total, parts)
    return rows


@flow(name="team28b-spark-events", log_prints=True)
def spark_events_flow():
    spark = start_spark()
    try:
        path = connect_hdfs(spark)
        raw = read_events(spark, path)
        stats = transform(raw)
        return save_table(spark, stats)
    finally:
        spark.stop()


if __name__ == "__main__":
    spark_events_flow()
```

Несколько моментов в коде, которые стоит пояснить.

Задачи Prefect здесь вызываются напрямую, одна за другой, поэтому все они работают в одном процессе, и сессию Spark и DataFrame'ы можно передавать между ними как обычные объекты. У каждой задачи стоит cache_policy=NO_CACHE. По умолчанию Prefect пытается закешировать результат задачи по её входным данным, и тогда при повторном запуске с теми же входами задача может вообще не выполниться. Для сессии Spark и DataFrame это не имеет смысла, к тому же Prefect не умеет их хешировать и пишет об этом предупреждения.

Master yarn указан прямо в коде, хотя он есть и в spark-defaults.conf. Так в самом потоке явно видно, что сессия запускается под YARN. Всё остальное (spark.driver.host, память, jar-файлы в HDFS, адрес метастора) Spark берёт из конфигов четвёртой домашки: pyspark при старте запускает тот же spark-submit из ~/apps/spark.

В connect_hdfs мы обращаемся к HDFS через ту же библиотеку Hadoop, которой пользуется сама сессия Spark, и с её конфигурацией. Так задача проверяет именно то подключение, через которое Spark дальше будет читать и писать.

Spark вычисляет лениво: transform только описывает преобразования, а по-настоящему они выполняются, когда нужен результат. count() в read_events и transform запускает вычисление сразу, чтобы в логе каждой задачи были реальные числа. Основная работа, агрегация и запись, выполняется в save_table.

Таблицу создаём сами как EXTERNAL с явным LOCATION и пишем в неё через insertInto, а не saveAsTable, по тем же причинам, что в четвёртой домашке: повторный запуск не падает, а Hive 4 не переделывает таблицу и не переносит её данные. С overwrite=True перезаписываются только партиции, которые есть в новых данных, так что повторный запуск потока не создаёт дублей. Формат ORC, Hive его читает без сюрпризов.

Таблица отличается от четвёртой домашки: там была статистика по дням и типам событий, здесь по дням и часам, с отдельным подсчётом покупок и выручки.

## Запуск потока

Перед запуском лучше остановить HiveServer2, если он работает: потоку нужен только Metastore, а памяти на team-28-01 мало, на ней одновременно будут сервер Prefect, драйвер Spark, PostgreSQL и Metastore. Для проверки через beeline HiveServer2 запустим потом снова.

```bash
kill $(cat ~/hive/run/hiveserver2.pid)
```

Запускаем поток:

```bash
cd ~/prefect/flows
python spark_events_flow.py 2> ~/prefect/logs/spark_events_flow.err
```

Логи Prefect по задачам печатаются в терминал. Служебный лог Spark уходит в файл, чтобы не забивать вывод.

Первый запуск занимает пару минут: YARN выделяет контейнеры, executor'ы скачивают jar-файлы из HDFS.

Чтобы обработать новые данные, достаточно положить новый CSV во входной каталог в HDFS и запустить поток ещё раз.

## Проверка в Prefect UI

Туннель с ноутбука:

```bash
ssh -N -L 4300:team-28-01:4300 team28b@2.59.83.133
```

В браузере http://localhost:4300. В разделе Runs виден запуск потока team28b-spark-events в состоянии Completed. Если открыть его, видны пять задач по порядку и логи каждой из них.

То же самое из терминала:

```bash
prefect flow-run ls --limit 5
```

## Проверка в YARN

```bash
yarn application -list -appStates ALL | grep team28b-prefect-events
```

Там видно приложение с типом SPARK. Его id совпадает с тем, что задача start_spark написала в лог, а после завершения потока статус FINISHED и SUCCEEDED.

То же видно в YARN UI, туннель как в прошлых домашках:

```bash
ssh -N -L 9088:team-28-nn:9088 team28b@2.59.83.133
```

В браузере http://localhost:9088, приложение team28b-prefect-events будет в списке Applications.

## Как понять, что всё получилось

В логах потока, по задачам:

- start_spark: master: yarn и application id вида application_...;
- connect_hdfs: подключились к hdfs://team-28-nn:9100, во входном каталоге events.csv;
- read_events: прочитано 5000 строк, все колонки строкового типа (это до приведения типов);
- transform: после очистки 4980, то есть 20 строк без времени отфильтровались;
- save_table: 120 строк в таблице (5 дней на 24 часа), сумма events_cnt 4980, пять партиций с event_date=2026-10-01 по event_date=2026-10-05;
- в конце Flow run ... Finished in state Completed().

В HDFS:

```bash
hdfs dfs -ls /user/team28b/prefect/tables/events_hourly_stats
```

Там пять каталогов event_date=..., в каждом ORC-файл.

## Проверка через Hive

По заданию достаточно, чтобы данные были сохранены как таблица. Но раз таблица лежит в общем метасторе, убедимся, что её видит и обычный клиент Hive. Запускаем HiveServer2, как в третьей домашке:

```bash
nohup hive --service hiveserver2 > ~/hive/logs/hiveserver2.out 2>&1 &
echo $! > ~/hive/run/hiveserver2.pid
```

Ждём, пока появится порт 10100, и подключаемся:

```bash
beeline -u "jdbc:hive2://10.28.0.13:10100/default" -n team28b
```

Внутри:

```sql
SHOW TABLES IN team28b;
SHOW PARTITIONS team28b.events_hourly_stats;
SELECT * FROM team28b.events_hourly_stats WHERE event_date = '2026-10-03' ORDER BY event_hour LIMIT 5;
SELECT SUM(events_cnt), SUM(purchases_cnt) FROM team28b.events_hourly_stats;
```

В списке таблиц появится events_hourly_stats, партиций будет пять. Сумма events_cnt должна быть 4980, как в логе задачи save_table.

## Логи

Логи Prefect по задачам есть в терминале и в UI у каждого запуска. Лог сервера Prefect - ~/prefect/logs/server.out. Служебный лог драйвера Spark - ~/prefect/logs/spark_events_flow.err. Логи executor'ов лежат у NodeManager'ов, их удобнее всего смотреть через YARN UI.

Ошибки считаем как в прошлых домашках:

```bash
grep -icE "error|fatal|exception" ~/prefect/logs/server.out ~/prefect/logs/spark_events_flow.err
```

Предупреждения о том, что не загрузилась native-hadoop library, и про SLF4J - нормально, на работу не влияют.

## Изоляция от Team A

Prefect и окружение Python ставятся в /home/team28b, sudo не нужен. Сервер Prefect на 4300, стандартный 4200 не занимаем, его база в нашем ~/prefect/home. Spark-приложение идёт в наш YARN, читает и пишет в наш HDFS на 9100, таблица регистрируется в нашем метасторе на 9183. Сервер Prefect останавливаем только по своему pid. После потока приложение Spark в YARN завершается и контейнеры освобождаются.

## Куда смотреть, если не работает

Если ModuleNotFoundError: No module named 'pyspark', значит не применился PYTHONPATH. Делаем source ~/.bashrc и проверяем, что python - это ~/apps/prefect-env/bin/python (команда which python).

Если поток висит на задаче start_spark, а в логе Spark Retrying connect to server на порту 8032, значит не сделан шаг из четвёртой домашки с yarn.resourcemanager.address. Проверяем, что в yarn-site.xml на team-28-01 есть 10.28.0.11:9033.

Если start_spark висит, а приложение в YARN на ACCEPTED, YARN не может выделить контейнеры. Проверяем yarn node -list и свободную память на узлах (free -m). Если памяти не хватает на team-28-01, останавливаем HiveServer2, как написано в разделе про запуск.

Если поток пишет, что не может достучаться до API Prefect, значит сервер не запущен. Проверяем curl http://127.0.0.1:4300/api/health и ~/prefect/logs/server.out.

Если UI в браузере открывается, но пустой или с ошибкой подключения, сервер запустили без PREFECT_UI_API_URL. Останавливаем его и запускаем командой из раздела про сервер.

Если задача connect_hdfs падает с FileNotFoundError, входных данных нет в HDFS. Кладём CSV по разделу про входные данные.

Если появится ошибка Python in worker has different version, значит у executor'ов другая версия Python, чем у драйвера. В нашем коде такого быть не должно, Python на executor'ах не запускается. Но если добавить свою Python-функцию, нужно, чтобы версия Python в ~/apps/prefect-env совпадала с системным python3 на всех узлах.

## Как повторить с нуля

Всё на team-28-01 под team28b. HDFS, YARN, PostgreSQL и Metastore должны быть запущены, Spark настроен по четвёртой домашке. Проверяем, что входной CSV лежит в HDFS, если нет - кладём. Создаём окружение Python той же версии, что системный python3, и ставим в него Prefect. Прописываем переменные в ~/.bashrc и проверяем импорт pyspark и prefect. Запускаем сервер Prefect на 4300. Сохраняем код в ~/prefect/flows/spark_events_flow.py и запускаем его. Проверяем запуск в Prefect UI, приложение в YARN, партиции в HDFS и таблицу через beeline