const express = require('express');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const Redis = require('ioredis');
const { Kafka } = require('kafkajs');
const TAXI_API_URL = process.env.TAXI_API_URL || 'http://taxi-api:8000';


const app = express();
app.use(cors());

const redis = new Redis({ host: 'redis', port: 6379 });
const server = app.listen(5001, () => console.log('Backend running on port 5001'));
const wss = new WebSocketServer({ server });

const kafka = new Kafka({ brokers: ['kafka:9092'] });

const ALARM_TTL_MS = 5 * 60 * 1000; // 5 minutes

const speedingIncidents = new Map(); // taxiId -> incident
const areaViolations = new Map();    // taxiId -> violation

// Rolling window of recent end-to-end latencies (ms) for the dashboard health panel.
// Each taxi-processed event carries ingestedAt (provider publish time); latency is
// now - ingestedAt, measured the moment the backend receives the event from Kafka.
const recentLatencies = [];
const LATENCY_WINDOW = 500; // keep the last N samples
function recordLatency(ms) {
    if (!Number.isFinite(ms) || ms < 0) return;
    recentLatencies.push(ms);
    if (recentLatencies.length > LATENCY_WINDOW) recentLatencies.shift();
}
function latencyStats() {
    if (recentLatencies.length === 0) return { avgLatencyMs: null, p95LatencyMs: null };
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
        broadcast({ type: 'latencyStats', stats });
        console.log(`[Latency] avg ${stats.avgLatencyMs}ms p95 ${stats.p95LatencyMs}ms (n=${recentLatencies.length})`);
    }
}, 5000);

function pruneExpired(map, ttl) {
    const cutoff = Date.now() - ttl;
    let changed = false;
    for (const [taxiId, entry] of map) {
        if (entry.receivedAt < cutoff) {
            map.delete(taxiId);
            changed = true;
        }
    }
    return changed;
}
setInterval(() => {
    const speedingChanged = pruneExpired(speedingIncidents, ALARM_TTL_MS);
    const violationsChanged = pruneExpired(areaViolations, ALARM_TTL_MS);
    if (speedingChanged) broadcast({ type: 'alarmsSync', kind: 'speeding', speedingIncidents: Array.from(speedingIncidents.values()) });
    if (violationsChanged) broadcast({ type: 'alarmsSync', kind: 'area', areaViolations: Array.from(areaViolations.values()) });
}, 30_000);
// DEBUG ENDPOINT - remove before production
app.get('/debug', async (req, res) => {
    const keys = await redis.keys('taxi:speed:*');
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
            `${TAXI_API_URL}/taxis/${encodeURIComponent(id)}/times?time_interval=${encodeURIComponent(timeInterval)}&number=${encodeURIComponent(number)}`
        );

        const body = await upstream.json().catch(() => null);

        if (!upstream.ok) {
            return res.status(upstream.status).json(
                body || {
                    error: "taxi-api error",
                }
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
    wss.clients.forEach(client => {
        if (client.readyState === 1) client.send(msg);
    });
}

// Read full state from Redis — only called when a new client connects
async function buildSnapshot() {
    const keys = await redis.keys('taxi:speed:*');
    const taxis = [];
    let totalDistance = 0;
    for (const key of keys) {
        const data = await redis.hgetall(key);
        if (data && data.latitude && data.longitude) {
            taxis.push({
                taxi_id: key.split(':')[2],
                latitude: parseFloat(data.latitude),
                longitude: parseFloat(data.longitude),
                speed: parseFloat(data.speed),
                distance: parseFloat(data.distance),
                timestamp: data.timestamp,
                isSpeeding: data.isSpeeding === 'true',
                averageSpeed: parseFloat(data.averageSpeed),
                totalDistance: parseFloat(data.totalDistance),
                lastMoved: data.lastMoved && data.lastMoved !== 'null' ? data.lastMoved : '',
                isParking: data.isParking === 'true'
            });
            totalDistance += parseFloat(data.distance) || 0;
        }
    }
    return { taxis, totalDistance };
}

// New client connects — send full snapshot from Redis so map is not empty
wss.on('connection', async (ws) => {
    const { taxis, totalDistance } = await buildSnapshot();
    ws.send(JSON.stringify({
        type: 'snapshot',
        taxis,
        stats: { activeTaxiCount: taxis.length, totalDistance, ...latencyStats() },
        speedingIncidents: Array.from(speedingIncidents.values()),
        areaViolations: Array.from(areaViolations.values())
    }));
});

async function startConsumers() {
    // Consumer for taxi-processed — broadcast single taxi update, no Redis scan
    const processedConsumer = kafka.consumer({ groupId: 'backend-processed' });
    await processedConsumer.connect();
    await processedConsumer.subscribe({ topic: 'taxi-processed', fromBeginning: false });
    await processedConsumer.run({
        eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value.toString());
            // End-to-end latency: now minus the provider ingestion timestamp.
            if (event.ingestedAt) recordLatency(Date.now() - event.ingestedAt);
            broadcast({
                type: 'taxiUpdate',
                taxi: {
                    taxi_id: String(event.taxiId),
                    latitude: event.latitude,
                    longitude: event.longitude,
                    speed: event.speed,
                    distance: event.totalDistance,
                    timestamp: event.timestamp,
                    isSpeeding: event.isSpeeding,
                    averageSpeed: event.averageSpeed,
                    totalDistance: event.totalDistance,
                    lastMoved: event.lastMoved ?? '',
                    isParking: event.isParking ?? false
                }
            });
        }
    });

    // Consumer for taxi-speeding — immediate alert broadcast
    const speedingConsumer = kafka.consumer({ groupId: 'backend-speeding' });
    await speedingConsumer.connect();
    await speedingConsumer.subscribe({ topic: 'taxi-speeding', fromBeginning: false });
    await speedingConsumer.run({
        eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value.toString());
            const incident = { taxiId: event.taxiId, speed: event.speed, timestamp: event.timestamp, receivedAt: Date.now() };
            speedingIncidents.set(event.taxiId, incident);
            broadcast({
                type: 'speedingAlert',
                incident,
                speedingIncidents: Array.from(speedingIncidents.values())
            });
        }
    });


    // Consumer for taxi-area-violations — immediate alert broadcast
    const violationsConsumer = kafka.consumer({ groupId: 'backend-violations' });
    await violationsConsumer.connect();
    await violationsConsumer.subscribe({ topic: 'taxi-area-violations', fromBeginning: false });
    await violationsConsumer.run({
        eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value.toString());
            const violation = {
                taxiId: event.taxiId,
                timestamp: event.timestamp,
                receivedAt: Date.now()
            };
            areaViolations.set(event.taxiId, violation);
            broadcast({
                type: 'areaViolation',
                violation,
                areaViolations: Array.from(areaViolations.values())
            });
        }
    });
}

startConsumers().catch(console.error);
