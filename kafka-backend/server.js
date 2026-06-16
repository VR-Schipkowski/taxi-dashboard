const express = require('express');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const Redis = require('ioredis');

const app = express();
app.use(cors());
const redis = new Redis({ host: 'redis', port: 6379 });


// DEBUG ENDPOINT - remove before production
app.get('/debug', async (req, res) => {
    const keys = await redis.keys('taxi:speed:*');
    const result = {};
    for (const key of keys) {
        result[key] = await redis.hgetall(key);
    }
    res.json(result);
});

// Connect to Redis where Flink stores all processed taxi data
const server = app.listen(5001, () => console.log('Backend running on port 5001'));
const wss = new WebSocketServer({ server });

// Every 5 seconds, read all processed taxi data from Redis and broadcast to connected dashboard clients.
// Fulfils two pipeline operators:
//   - "Propagate location information to dashboard": sends each taxi's current position, speed and distance
//   - "Propagate information to dashboard": sends fleet-wide stats (active taxi count, total distance)
setInterval(async () => {
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
                timestamp: data.timestamp,
                speed: parseFloat(data.speed),
                distance: parseFloat(data.distance),
                isSpeeding: data.isSpeeding === 'true',
                averageSpeed: parseFloat(data.averageSpeed),
                totalDistance: parseFloat(data.totalDistance),
                lastMoved: data.lastMoved && data.lastMoved !== 'null' ? data.lastMoved : '',
                isParking: data.isParking === 'true'
            });
            totalDistance += parseFloat(data.distance) || 0;
        }
    }

    // Payload structure sent to frontend via WebSocket:
    // taxis: array of individual taxi location + speed + distance (for map markers)
    // stats: aggregated fleet summary (for dashboard info panel)
    const payload = {
        taxis,
        stats: {
            activeTaxiCount: taxis.length,
            totalDistance
        }
    };

    wss.clients.forEach(client => {
        if (client.readyState === 1) client.send(JSON.stringify(payload));
    });

    console.log(`[Interval] ${taxis.length} taxis sent, total distance: ${totalDistance.toFixed(2)} km`);

}, 5000);

// for debug 
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "dashboard",
    brokers: ["kafka:9092"],
});

const consumer = kafka.consumer({ groupId: "dashboard-debug" });

const alerts = {
    speeding: [],
    outOfArea: []
};

async function startKafka() {
    await consumer.connect();

    await consumer.subscribe({
        topics: ["taxi-speeding-alerts", "taxi-out-of-area-alerts"],
        fromBeginning: true
    });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            const value = message.value.toString();
            console.log("UPDATING ALERTS:", topic, value);

            if (topic === "taxi-speeding-alerts") {
                alerts.speeding.push(value);
                if (alerts.speeding.length > 100) alerts.speeding.shift();
            }
            if (topic === "taxi-out-of-area-alerts") {
                alerts.outOfArea.push(value);
                if (alerts.outOfArea.length > 100) alerts.outOfArea.shift();
            }
        }
    });
}

startKafka().catch(console.error);
console.log("Kafka consumer starting...");

app.get("/debug/alerts", (req, res) => {
    res.json(alerts);
});
