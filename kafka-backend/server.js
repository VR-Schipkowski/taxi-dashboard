const express = require("express");
const { WebSocketServer } = require("ws");
const cors = require("cors");
const Redis = require("ioredis");
const { Kafka } = require("kafkajs");
const TAXI_API_URL = process.env.TAXI_API_URL || "http://taxi-api:8000";

const app = express();
app.use(cors());

const redis = new Redis({ host: "redis", port: 6379 });
const server = app.listen(5001, () =>
  console.log("Backend running on port 5001"),
);
const wss = new WebSocketServer({ server });

const kafka = new Kafka({ brokers: ["kafka:9092"] });

let speedingIncidents = [];
let areaViolations = [];
const areaViolationIndex = new Set();
// taxi_Id -> violation

// Rolling window of recent end-to-end latencies (ms) for the dashboard health panel.
// Each taxi-processed event carries ingested_at (provider publish time); latency is
// now - ingested_at, measured the moment the backend receives the event from Kafka.
const recentLatencies = [];
const LATENCY_WINDOW = 500; // keep the last N samples
function recordLatency(ms) {
  if (!Number.isFinite(ms) || ms < 0) return;
  recentLatencies.push(ms);
  if (recentLatencies.length > LATENCY_WINDOW) recentLatencies.shift();
}
function latencyStats() {
  if (recentLatencies.length === 0)
    return { avgLatencyMs: null, p95LatencyMs: null };
  const sum = recentLatencies.reduce((a, b) => a + b, 0);
  const avg = Math.round(sum / recentLatencies.length);
  const sorted = [...recentLatencies].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95));
  return { avgLatencyMs: avg, p95LatencyMs: sorted[idx] };
}
// Broadcast aggregated latency to all clients every 5s.
setInterval(() => {
  const stats = latencyStats();
  if (stats.avgLatencyMs !== null) {
    broadcast({ type: "latencyStats", stats });
    console.log(
      `[Latency] avg ${stats.avgLatencyMs}ms p95 ${stats.p95LatencyMs}ms (n=${recentLatencies.length})`,
    );
  }
}, 5000);
// DEBUG ENDPOINT - remove before production
app.get("/debug", async (req, res) => {
  const keys = await redis.keys("taxi:speed:*");
  const result = {};
  for (const key of keys) {
    result[key] = await redis.hgetall(key);
  }
  res.json(result);
});
// Wrapper for taxi-api
app.get("/taxis/:id/locations", async (req, res) => {
  const { id } = req.params;

  const timeInterval = Number(req.query.time_interval) || 15;
  const number = Number(req.query.number) || 10;

  try {
    const upstream = await fetch(
      `${TAXI_API_URL}/taxis/${encodeURIComponent(id)}/times?time_interval=${encodeURIComponent(timeInterval)}&number=${encodeURIComponent(number)}`,
    );

    const body = await upstream.json().catch(() => null);

    if (!upstream.ok) {
      return res.status(upstream.status).json(
        body || {
          error: "taxi-api error",
        },
      );
    }

    res.json(body);
  } catch (err) {
    console.error("Error calling taxi-api:", err);
    res.status(502).json({
      error: "taxi-api unreachable",
    });
  }
});

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(msg);
  });
}

// Read full state from Redis — only called when a new client connects
//TODO: add a redis for taxi-heatmap and taxi-area-violations so we can send a snapshot of those too also figure out better parameter for the heatmap component ocpancy also make it togabl.
async function buildSnapshot() {
  const keys = await redis.keys("taxi:speed:*");
  const taxis = [];
  let totalDistanceAll = 0;
  for (const key of keys) {
    const data = await redis.hgetall(key);
    if (data && data.latitude && data.longitude) {
      taxis.push({
        taxi_id: key.split(":")[2],
        latitude: parseFloat(data.latitude),
        longitude: parseFloat(data.longitude),
        speed: parseFloat(data.speed),
        distance: parseFloat(data.distance),
        timestamp: data.timestamp,
        isSpeeding: data.isSpeeding === "true",
        averageSpeed: parseFloat(data.averageSpeed),
        totalDistance: parseFloat(data.totalDistance),
        lastMoved:
          data.lastMoved && data.lastMoved !== "null" ? data.lastMoved : "",
        isParking: data.isParking === "true",
      });
      totalDistanceAll += parseFloat(data.totalDistance) || 0;
    }
  }
  return { taxis, totalDistanceAll };
}

// New client connects — send full snapshot from Redis so map is not empty
wss.on("connection", async (ws) => {
  const { taxis, totalDistanceAll } = await buildSnapshot();
  ws.send(
    JSON.stringify({
      type: "snapshot",
      taxis,
      stats: {
        activeTaxiCount: taxis.length,
        totalDistanceAll,
        ...latencyStats(),
      },
      speedingIncidents,
      areaViolations,
    }),
  );
});

// Start Kafka consumers for taxi-processed, taxi-speeding, and taxi-area-violations topics
// TODO why not use redice for taxi data?? isnt is double work to have both kafka and redis?
async function startConsumers() {
  // Consumer for taxi-processed — broadcast single taxi update, no Redis scan
  const processedConsumer = kafka.consumer({ groupId: "backend-processed" });
  await processedConsumer.connect();
  await processedConsumer.subscribe({
    topic: "taxi-processed",
    fromBeginning: false,
  });
  await processedConsumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      // End-to-end latency: now minus the provider ingestion timestamp.
      if (event.ingested_at) recordLatency(Date.now() - event.ingested_at);
      broadcast({
        type: "taxiUpdate",
        taxi: {
          taxi_id: String(event.taxi_id),
          latitude: event.latitude,
          longitude: event.longitude,
          speed: event.speed,
          distance: event.totalDistance,
          timestamp: event.timestamp,
          isSpeeding: event.isSpeeding,
          averageSpeed: event.averageSpeed,
          totalDistance: event.totalDistance,
          speedingStateChanged: event.speedingStateChanged,
          lastMoved: event.lastMoved ?? "",
          isParking: event.isParking ?? false,
        },
      });
    },
  });

  // Consumer for taxi-speeding — immediate alert broadcast
  const speedingConsumer = kafka.consumer({ groupId: "backend-speeding" });
  await speedingConsumer.connect();
  await speedingConsumer.subscribe({
    topic: "taxi-speeding",
    fromBeginning: false,
  });
  await speedingConsumer.run({
    eachMessage: async ({ message }) => {
      speedingIncidents = JSON.parse(message.value.toString());
      broadcast({ type: "speedingAlert", speedingIncidents });
    },
  });

  // Consumer for taxi-area-violations — immediate alert broadcast
  const violationsConsumer = kafka.consumer({ groupId: "backend-violations" });
  await violationsConsumer.connect();
  await violationsConsumer.subscribe({
    topic: "taxi-area-violations",
    fromBeginning: false,
  });
  violationsConsumer.run({
    eachMessage: async ({ message }) => {
      const newList = JSON.parse(message.value.toString());
      const newIds = new Set(newList.map(v => String(v.taxi_id)));

      // detect transitions by diffing
      for (const id of newIds) {
        if (!areaViolationIndex.has(id)) {
          broadcast({ type: 'ooaNotification', trigger: 'entered', taxiId: id });
        }
      }
      for (const id of areaViolationIndex) {
        if (!newIds.has(id)) {
          broadcast({ type: 'ooaNotification', trigger: 'returned', taxiId: id });
        }
      }

      areaViolations = newList;
      areaViolationIndex.clear();
      newIds.forEach(id => areaViolationIndex.add(id));

      broadcast({ type: 'areaViolation', areaViolations });
    }
  });
  const cellConsumer = kafka.consumer({ groupId: "backend-cell" });
  await cellConsumer.connect();
  await cellConsumer.subscribe({
    topic: "taxi-heatmap",
    fromBeginning: false,
  });
  await cellConsumer.run({
    eachMessage: async ({ message }) => {
      const cellData = JSON.parse(message.value.toString());
      broadcast({ type: "heatmapUpdate", cellData });
    },
  });
}

startConsumers().catch(console.error);
