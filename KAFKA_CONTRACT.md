# Kafka Contract & Architecture Overview

This file explains the old and new architecture of the pipeline, the Kafka topic contracts,
and who is responsible for what. Read this before starting any Stage 2 work.

---

## Old Architecture (Stage 1)

```
Data Provider → Kafka (taxi-locations) → Flink → Redis → Backend (polls every 5s) → WebSocket → Frontend
```

**Problems with this:**
- Backend polls Redis every 5 seconds — not truly real-time
- Flink writes directly to Redis — tightly coupled, hard to extend
- Only one consumer of processed data (Redis) — nothing else can listen
- No historical data stored anywhere

---

## New Architecture (Stage 2)

```
Data Provider → Kafka (taxi-locations) → Flink ──→ Kafka (taxi-processed) ──→ Backend ──→ Redis (current state)
                                               │                                      └──→ WebSocket → Frontend
                                               └──→ Kafka (taxi-speeding)  ──→ Backend
                                               │
                                               └──→ Kafka (taxi-area-violations) ──→ Backend
                                               │
                                               └──→ Kafka (taxi-processed) ──→ Database (historical data)
```

**What changed:**
- Flink no longer writes to Redis directly — it publishes to Kafka topics only
- Backend consumes from Kafka → writes latest state to Redis → pushes WebSocket instantly
- Database also consumes from `taxi-processed` Kafka topic — stores every event as historical data
- New Kafka topics carry speeding and area violation events separately

**Why each component exists:**

| Component | Role |
|---|---|
| Kafka | Backbone — Flink publishes here, all consumers read from here |
| Backend | Consumes Kafka events → updates Redis + pushes WebSocket to frontend |
| Redis | Current state cache — backend reads this when a new browser connects or backend restarts |
| Database | Historical data — consumed directly from Kafka, used for leaderboards and stats |

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
**Producer:** Flink 
**Consumers:** Backend, Database 

Published for every taxi update after speed and distance are calculated.
This is the main data highway — all downstream consumers read from here.

```json
{
  "taxiId": 31,
  "timestamp": "2008-02-02 13:59:00",
  "latitude": 39.9163,
  "longitude": 116.3972,
  "speed": 45.2,
  "totalDistance": 12.4,
  "isSpeeding": false,
  "isOutOfArea": false
}
```

---

### `taxi-speeding`
**Producer:** Flink 
**Consumer:** Backend 

Published when a taxi exceeds **50 km/h**. Same fields as `taxi-processed`.

```json
{
  "taxiId": 42,
  "timestamp": "2008-02-02 14:01:00",
  "latitude": 39.9200,
  "longitude": 116.4100,
  "speed": 67.3,
  "totalDistance": 5.1,
  "isSpeeding": true,
  "isOutOfArea": false
}
```

---

### `taxi-area-violations`
**Producer:** Flink
**Consumer:** Backend

Published when a taxi leaves the **10 km radius** around the Forbidden City (39.9163°N, 116.3972°E).
Taxis beyond **15 km** are discarded from the pipeline entirely.

```json
{
  "taxiId": 815,
  "timestamp": "2008-02-02 14:05:00",
  "latitude": 40.0500,
  "longitude": 116.1200,
  "speed": 38.0,
  "totalDistance": 22.7,
  "isSpeeding": false,
  "isOutOfArea": true
}
```

---

## WebSocket Payload (Frontend Contract)

The backend sends this to the React frontend every time new data arrives.

```json
{
  "taxis": [
    {
      "taxi_id": "31",
      "latitude": 39.9163,
      "longitude": 116.3972,
      "speed": 45.2,
      "distance": 12.4,
      "timestamp": "2008-02-02 13:59:00",
      "isSpeeding": false
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

## Redis Role After Refactor

Redis is **kept** but its writer changes — `SpeedRedisProcessor.java` in Flink is replaced. The backend now writes to Redis after consuming from Kafka.

| Scenario | Data source |
|---|---|
| Live updates for connected clients | Kafka → Backend → WebSocket (instant) |
| New browser connects / backend restarts | Backend reads Redis snapshot → sends full current state immediately |
| Historical data / leaderboards | Database (consumed directly from Kafka) |

Without Redis, a new browser would see an empty map and wait for taxis to appear one by one as Kafka events trickle in. Redis gives new clients the full picture immediately.
