import glob
import json
import math
import os
import time
import uuid
from datetime import datetime, timezone

import pyarrow.parquet as pq
from confluent_kafka import Producer

TOPIC = 'nyc.taxi.trips'
REQUIRED = [
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID',
    'DOLocationID', 'total_amount', 'trip_distance'
]


def main():
    bootstrap = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
    rate = max(float(os.getenv('EVENTS_PER_SECOND', '20')), 0.1)
    limit = int(os.getenv('MAX_EVENTS', '5000'))
    if limit <= 0:
        print('MAX_EVENTS <= 0; no se publicaron eventos.')
        return

    files = sorted(glob.glob('/app/data/historical/*.parquet'))
    if not files:
        raise SystemExit('Faltan archivos .parquet en data/historical/')

    replay_run_id = os.getenv('REPLAY_RUN_ID') or str(uuid.uuid4())
    producer = Producer({'bootstrap.servers': bootstrap, 'client.id': 'nyc-taxi-replay'})
    delivery_failures = []

    def delivered(error, message):
        if error is not None:
            delivery_failures.append(str(error))
            print(f'Error de entrega Kafka: {error}', flush=True)

    sent = 0
    finished = False
    print(f'replay_run_id={replay_run_id}', flush=True)
    for file in files:
        source_file = os.path.basename(file)
        source_row = 0
        parquet_file = pq.ParquetFile(file)
        for batch in parquet_file.iter_batches(batch_size=1024):
            for row in batch.to_pylist():
                current_row = source_row
                source_row += 1
                if any(row.get(key) is None for key in REQUIRED):
                    continue
                pickup = row['tpep_pickup_datetime']
                if not isinstance(pickup, datetime):
                    continue

                # El identificador determinista permite deduplicar reintentos y replays repetidos.
                event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f'nyc-taxi/{source_file}/{current_row}'))
                event = {
                    key: (
                        value.isoformat() if isinstance(value, datetime)
                        else None if isinstance(value, float) and not math.isfinite(value)
                        else value
                    )
                    for key, value in row.items()
                }
                event.update(
                    event_id=event_id,
                    replay_run_id=replay_run_id,
                    ingested_at=datetime.now(timezone.utc).isoformat(),
                    source_file=source_file,
                    source_row=current_row,
                    replay=True,
                )
                payload = json.dumps(event, default=str, allow_nan=False).encode('utf-8')

                while True:
                    try:
                        producer.produce(
                            TOPIC,
                            key=str(row['PULocationID']).encode(),
                            value=payload,
                            on_delivery=delivered,
                        )
                        break
                    except BufferError:
                        producer.poll(1)
                producer.poll(0)
                sent += 1
                if sent % 500 == 0:
                    print(f'Eventos encolados: {sent}', flush=True)
                if sent >= limit:
                    finished = True
                    break
                time.sleep(1 / rate)
            if finished:
                break
        if finished:
            break

    pending = producer.flush(30)
    if pending or delivery_failures:
        raise SystemExit(
            f'Publicación incompleta: {pending} pendientes, '
            f'{len(delivery_failures)} entregas fallidas'
        )
    print(f'Finalizado: {sent} eventos entregados; replay_run_id={replay_run_id}')


if __name__ == '__main__':
    main()
