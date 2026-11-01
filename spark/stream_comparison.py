import glob
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, Window

Path('/app/results/spark-events').mkdir(parents=True, exist_ok=True)
spark = SparkSession.builder.appName('NYC Taxi Replay Comparison').master('local[*]').getOrCreate()
files = glob.glob('/app/results/lake/ingest_date=*/*.parquet')
if not files:
    raise SystemExit('No hay eventos persistidos; ejecute lake-writer y producer primero')
zones = glob.glob('/app/data/reference/*.csv')
if not zones:
    raise SystemExit('Falta Taxi Zone Lookup CSV')
lake = spark.read.parquet(*files)
# Spark lee el JSON original, conservando todas las columnas originales.
events = spark.read.json(lake.rdd.map(lambda row: row.payload_json))
if 'event_id' not in events.columns:
    raise SystemExit('Los eventos no contienen event_id; no es posible deduplicarlos')
events = events.dropDuplicates(['event_id']).filter(
    F.col('PULocationID').between(1, 263) & (F.col('total_amount') >= 0)
)

replay_run_id = os.getenv('REPLAY_RUN_ID')
if replay_run_id:
    events = events.filter(F.col('replay_run_id') == replay_run_id)

z = spark.read.option('header', True).option('inferSchema', True).csv(zones[0])
enriched = events.join(
    F.broadcast(z.select(F.col('LocationID').alias('PULocationID'), 'Borough', 'Zone')),
    'PULocationID',
    'left',
)
agg = enriched.groupBy('Borough', 'Zone', 'PULocationID').agg(
    F.count('*').alias('replayed_trips')
)
rank = agg.withColumn(
    'rank',
    F.row_number().over(
        Window.partitionBy('Borough').orderBy(
            F.desc('replayed_trips'), F.asc('PULocationID')
        )
    ),
)
top3 = rank.filter('rank <= 3')
top3.orderBy('Borough', 'rank').coalesce(1).write.mode('overwrite').option(
    'header', True
).csv('/app/results/comparison/replayed_top3')
top3.orderBy('Borough', 'rank').show(30, False)

hist = spark.read.option('header', True).option('inferSchema', True).csv(
    '/app/results/batch/p7_top3'
)
current = top3.select(
    'Borough', 'PULocationID', 'Zone', 'replayed_trips', F.col('rank').alias('replay_rank')
)
hist3 = hist.select(
    'Borough', 'PULocationID', F.col('Zone').alias('historical_zone'),
    'trips', F.col('rank').alias('historical_rank')
)
comparison = current.join(hist3, ['Borough', 'PULocationID'], 'full_outer')
comparison.coalesce(1).write.mode('overwrite').option('header', True).csv(
    '/app/results/comparison/rank_overlap'
)
comparison.show(30, False)
print('ADVERTENCIA: ranking de eventos reproducidos, no equivale a una ventana de Flink.')
spark.stop()
