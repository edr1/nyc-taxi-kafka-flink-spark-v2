# Evidencia de ejecución validada

Fecha: 31 de octubre de 2026  
Replay: `informe-final-20261031`  
Tasa: 100 eventos/s  
Eventos entregados: 5000

## Infraestructura

- Kafka 3.9.1 en modo KRaft: saludable.
- Topic `nyc.taxi.trips`: 3 particiones, factor de replicación 1.
- Flink 1.20.1: job `5296e2c6ee3be2703e9022fd3b425a60` en estado RUNNING,
  cinco tareas activas y cero tareas fallidas.
- Grupos `taxi-flink-v2` y `taxi-lake-v1`: lag final cero en las tres particiones.
- Spark 3.5.5: análisis histórico y comparación finalizados correctamente.
- Kafka UI y Spark History Server: iniciados correctamente.

## Calidad y volúmenes

| Indicador | Resultado |
|---|---:|
| Registros históricos brutos | 9.554.778 |
| Registros históricos válidos | 9.402.640 |
| Eventos Kafka entregados | 5.000 |
| Eventos válidos procesados por Flink | 4.921 |
| Ingresos de la ventana Flink | USD 133.103,13 |

## Resultados principales

- P1: Manhattan concentró 8.463.211 viajes (90,01 %); Queens, 817.312
  (8,69 %). Midtown Center lideró por viajes con 448.514. JFK Airport registró
  USD 33.877.907,57 de ingresos.
- P2: la hora de mayor demanda fue las 18:00 (680.861 viajes); el jueves fue el
  día de mayor demanda (1.520.793). La combinación máxima fue jueves a las 18:00
  con 119.136 viajes.
- Flink: en la ventana 21:29–21:30 lideraron East Village (283), Upper East Side
  South (228) y Lincoln Square East (223).
- P7: el replay coincidió con cinco de las doce posiciones Top 3 evaluadas:
  Queens 3/3, Manhattan 1/3, Brooklyn 1/3 y Bronx 0/3.
- Clima: sin lluvia se observaron 7.892.759 viajes, distancia media 4,10 millas
  y propina media USD 3,33; con lluvia, 1.509.881 viajes, 3,83 millas y USD 3,17.

## Correcciones descubiertas durante la ejecución

1. El replay Spark ahora aplica los mismos filtros de zona e importe que Flink y
   el histórico; se excluyen las zonas especiales 264 y 265.
2. El join meteorológico utiliza una clave horaria textual para evitar que el
   cambio al horario de verano duplique registros de las 03:00.
3. Los IDs deterministas y la deduplicación evitan dobles conteos por reintentos.

