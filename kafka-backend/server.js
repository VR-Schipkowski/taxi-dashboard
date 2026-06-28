const express = require('express');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const Redis = require('ioredis');
const { Kafka } = require('kafkajs');

const app = express();
app.use(cors());

const redis = new Redis({ host: 'redis', port: 6379 });
const server = app.listen(5001, () => console.log('Backend running on port 5001'));
const wss = new WebSocketServer({ server });

const kafka = new Kafka({ brokers: ['kafka:9092'] });

let speedingIncidents = [];
let areaViolations = [];

// DEBUG ENDPOINT - remove before production
app.get('/debug', async (req, res) => {
    const keys = await redis.keys('taxi:speed:*');
    const result = {};
    for (const key of keys) {
        result[key] = await redis.hgetall(key);
    }
    res.json(result);
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
        if (data && data.latitude && data.longitude && data.isOutOfArea !== 'true') {
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
        stats: { activeTaxiCount: taxis.length, totalDistance },
        speedingIncidents,
        areaViolations
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
            speedingIncidents = JSON.parse(message.value.toString());
            broadcast({ type: 'speedingAlert', speedingIncidents });
        }
    });

    // Consumer for taxi-area-violations — immediate alert broadcast
    const violationsConsumer = kafka.consumer({ groupId: 'backend-violations' });
    await violationsConsumer.connect();
    await violationsConsumer.subscribe({ topic: 'taxi-area-violations', fromBeginning: false });
    await violationsConsumer.run({
        eachMessage: async ({ message }) => {
            areaViolations = JSON.parse(message.value.toString());
            broadcast({ type: 'areaViolation', areaViolations });
        }
    });
}

startConsumers().catch(console.error);
