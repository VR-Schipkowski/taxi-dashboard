# Kafka Contract & Architecture Overview

This file explains the architecture of the pipeline, the Kafka topic contracts,
the WebSocket protocol, and who is responsible for what.
Read this before starting any Stage 2 feature work.

---

## Architecture (Stage 2 — Kappa)

```
Data Provider → Kafka (taxi-locations) → Flink ──→ Redis (Store Information, every event)
                                               │
                                               ├──→ Kafka (taxi-processed, throttled 5s) ──→ Backend → WebSocket → Frontend
                                               ├──→ Kafka (taxi-speeding, immediate)     ──→ Backend → WebSocket → Frontend
                                               └──→ Kafka (taxi-area-violations, immediate) → Backend → WebSocket → Frontend
```

**Key decisions:**

| Decision | Reason |
|---|---|
| Flink writes every event to Redis | Professor's topology — "Store Information" operator |
| `taxi-processed` is throttled to 1 update per taxi per 5s | Professor's topology — "Propagate location information every 5 seconds" |
| `taxi-speeding` and `taxi-area-violations` are immediate | Alerts must not be delayed by the throttle window |
| Backend does NOT write to Redis | Flink is the single writer — avoids race conditions |
| Backend reads Redis only when a new client connects | Snapshot so the map is not empty on first load |
| Backend broadcasts single-taxi events over WebSocket | Avoids full Redis scan on every Kafka event (OOM fix) |

---

## Kafka Topics

### `taxi-locations`
**Producer:** Data provider
**Consumer:** Flink

Raw GPS data replayed from the T-drive dataset in timestamp order.

```json
{
  "taxiId": 31,
  "timestamp": "2008-02-02 13:59:00",
  "latitude": 39.9163,
  "longitude": 116.3972
}
```

---

### `taxi-processed`
**Producer:** Flink (throttled — one update per taxi per 5-second window)
**Consumer:** Backend

The main data stream. Published after speed, distance, and parking state are calculated.
The 5s throttle means the latest event per taxi per window is forwarded — not every raw GPS point.

```json
{
  "taxiId": 31,
  "timestamp": "2008-02-02 13:59:00",
  "latitude": 39.9163,
  "longitude": 116.3972,
  "speed": 45.2,
  "averageSpeed": 38.1,
  "totalDistance": 12.4,
  "isSpeeding": false,
  "isOutOfArea": false,
  "isParking": false,
  "lastMoved": "2008-02-02 13:55:00"
}
```

Field notes:
- `speed` — instantaneous speed for this event (km/h)
- `averageSpeed` — rolling average speed for this taxi (km/h)
- `totalDistance` — cumulative distance traveled (km)
- `isSpeeding` — true if speed > 50 km/h (Mykola's threshold)
- `isOutOfArea` — true if outside 10 km radius of Forbidden City (Muhammad's geofence)
- `isParking` — true if the taxi has not moved for > 300 seconds
- `lastMoved` — timestamp of last non-parked position, empty string if never parked

---

### `taxi-speeding`
**Producer:** Flink (immediate — side output, bypasses 5s throttle)
**Consumer:** Backend

Published the moment a taxi exceeds **60 km/h**. Same full schema as `taxi-processed`.

```json
{
  "taxiId": 42,
  "timestamp": "2008-02-02 14:01:00",
  "latitude": 39.9200,
  "longitude": 116.4100,
  "speed": 67.3,
  "averageSpeed": 51.0,
  "totalDistance": 5.1,
  "isSpeeding": true,
  "isOutOfArea": false,
  "isParking": false,
  "lastMoved": ""
}
```

---

### `taxi-area-violations`
**Producer:** Flink (immediate — side output, bypasses 5s throttle)
**Consumer:** Backend

Published when a taxi leaves the **10 km radius** around the Forbidden City (39.9163°N, 116.3972°E).
Same full schema as `taxi-processed`.

```json
{
  "taxiId": 815,
  "timestamp": "2008-02-02 14:05:00",
  "latitude": 40.0500,
  "longitude": 116.1200,
  "speed": 38.0,
  "averageSpeed": 29.4,
  "totalDistance": 22.7,
  "isSpeeding": false,
  "isOutOfArea": true,
  "isParking": false,
  "lastMoved": ""
}
```

---

## WebSocket Protocol (Frontend Contract)

The backend sends **one of four message types** over WebSocket.
The frontend maintains a local map of taxis keyed by `taxi_id` and merges updates.

---

### `snapshot` — sent once when a new client connects

Full current state read from Redis. Use this to populate the initial map.

```json
{
  "type": "snapshot",
  "taxis": [
    {
      "taxi_id": "31",
      "latitude": 39.9163,
      "longitude": 116.3972,
      "speed": 45.2,
      "averageSpeed": 38.1,
      "distance": 12.4,
      "totalDistance": 12.4,
      "timestamp": "2008-02-02 13:59:00",
      "isSpeeding": false,
      "isParking": false,
      "lastMoved": ""
    }
  ],
  "stats": {
    "activeTaxiCount": 150,
    "totalDistance": 9823.4
  },
  "speedingIncidents": [
    { "taxiId": 42, "speed": 67.3, "timestamp": "2008-02-02 14:01:00" }
  ],
  "areaViolations": [
    { "taxiId": 815, "timestamp": "2008-02-02 14:05:00" }
  ]
}
```

---

### `taxiUpdate` — sent every ~5 seconds per taxi

Single taxi update from `taxi-processed`. Update (or insert) this taxi in your local map by `taxi_id`.

```json
{
  "type": "taxiUpdate",
  "taxi": {
    "taxi_id": "31",
    "latitude": 39.9163,
    "longitude": 116.3972,
    "speed": 45.2,
    "averageSpeed": 38.1,
    "distance": 12.4,
    "totalDistance": 12.4,
    "timestamp": "2008-02-02 13:59:00",
    "isSpeeding": false,
    "isParking": false,
    "lastMoved": ""
  }
}
```

---

### `speedingAlert` — sent immediately when a speeding event is detected

Use `incident` for the new event. Use `speedingIncidents` to replace the full list in the UI.

```json
{
  "type": "speedingAlert",
  "incident": {
    "taxiId": 42,
    "speed": 67.3,
    "timestamp": "2008-02-02 14:01:00"
  },
  "speedingIncidents": [
    { "taxiId": 42, "speed": 67.3, "timestamp": "2008-02-02 14:01:00" }
  ]
}
```

---

### `areaViolation` — sent immediately when a taxi leaves the geofence

Use `violation` for the new event. Use `areaViolations` to replace the full list in the UI.

```json
{
  "type": "areaViolation",
  "violation": {
    "taxiId": 815,
    "timestamp": "2008-02-02 14:05:00"
  },
  "areaViolations": [
    { "taxiId": 815, "timestamp": "2008-02-02 14:05:00" }
  ]
}
```

---

## Frontend Field Guide

| Field | Source message | Use for |
|---|---|---|
| `taxi.taxi_id` | snapshot, taxiUpdate | Map key, display ID |
| `taxi.latitude` / `taxi.longitude` | snapshot, taxiUpdate | Leaflet Marker position |
| `taxi.speed` | snapshot, taxiUpdate | Current speed display |
| `taxi.averageSpeed` | snapshot, taxiUpdate | Average speed display |
| `taxi.totalDistance` | snapshot, taxiUpdate | Total distance display |
| `taxi.isSpeeding` | snapshot, taxiUpdate | Speeding marker color |
| `taxi.isParking` | snapshot, taxiUpdate | Parking marker icon |
| `taxi.isOutOfArea` | snapshot | Area violation marker (not in taxiUpdate) |
| `speedingIncidents[]` | snapshot, speedingAlert | Speeding panel list |
| `areaViolations[]` | snapshot, areaViolation | Area violation panel list |

---

## Redis Role

Flink is the **only writer**. The backend only reads Redis.

| Scenario | What happens |
|---|---|
| Live update for connected clients | Kafka → Backend → WebSocket (instant, no Redis involved) |
| New browser connects | Backend reads all `taxi:speed:*` keys from Redis → sends `snapshot` |
| Backend restarts | Same as above — Redis is the recovery point |

Redis keys expire after **60 seconds**. A taxi that stops sending GPS events disappears from the map naturally.

Key format: `taxi:speed:<taxiId>` (Redis hash)

Fields stored per taxi: `latitude`, `longitude`, `speed`, `averageSpeed`, `distance`, `totalDistance`, `timestamp`, `isSpeeding`, `isOutOfArea`, `isParking`, `lastMoved`

---
