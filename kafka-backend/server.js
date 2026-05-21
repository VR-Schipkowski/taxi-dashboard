const express = require('express');
const { Kafka } = require('kafkajs');
const { WebSocketServer } = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());

const server = app.listen(5001, () => console.log('Backend läuft auf Port 5001'));
const wss = new WebSocketServer({ server });

// Verbindung zu Docker-Kafka (Nutze 'kafka:9092' statt 'localhost:9092' im Docker-Netzwerk)
const kafka = new Kafka({
    clientId: 'react-middleware',
    brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});
const consumer = kafka.consumer({ groupId: 'react-web-group' });

//TODO this has later to be moved to flink as the backend schould only pass trough data to websocker
let latestTaxiPositions = {};

async function startKafka() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'taxi-locations', fromBeginning: false });

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const rawPayload = message.value.toString();
                const data = JSON.parse(rawPayload);

                // Prüfen, ob es sich um ein echtes Positions-Update handelt
                if (data.taxi_id && data.latitude && data.longitude) {
                    latestTaxiPositions[data.taxi_id] = {
                        taxi_id: data.taxi_id,
                        longitude: data.longitude,
                        latitude: data.latitude,
                        timestamp: data.timestamp
                    };
                }
            } catch (err) {
                // Ignoriere korrupte oder nicht-JSON Nachrichten im Stream
            }
        },
    });
}
setInterval(() => {
    const positionsArray = Object.values(latestTaxiPositions);

    if (positionsArray.length > 0) {
        const payloadString = JSON.stringify(positionsArray);

        wss.clients.forEach(client => {
            if (client.readyState === 1) { // 1 = OPEN
                client.send(payloadString);
            }
        });

        console.log(`[Interval] ${positionsArray.length} Taxi-Positionen an Frontend gesendet.`);
    }
}, 5000); // 5000 Millisekunden = 5 Sekunden

startKafka().catch(console.error);
