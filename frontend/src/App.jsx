import { useState, useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import L from 'leaflet';
import './App.css';

// Wichtig: Leaflet-CSS-Styles importieren (sonst zerreißt es die Karte)
import 'leaflet/dist/leaflet.css';

// Fix für Standard-Marker-Icons in Leaflet bei der Nutzung mit Build-Tools wie Vite
import markerIconPng from 'leaflet/dist/images/marker-icon.png';
import markerShadowPng from 'leaflet/dist/images/marker-shadow.png';

//TODO better logo, logo is to big, propabiil change on zoom level
const defaultIcon = L.icon({
  iconUrl: markerIconPng,
  shadowUrl: markerShadowPng,
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
});

const parkingIcon = L.icon({
  iconUrl: markerIconPng,
  shadowUrl: markerShadowPng,
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  className: 'parking-marker',
});


function App() {
  const [taxis, setTaxis] = useState([]);
  const [status, setStatus] = useState('Connecting...');

  useEffect(() => {
    // Verbindung zum Node.js-Backend herstellen
    const socket = new WebSocket('ws://localhost:5001');

    socket.onopen = () => {
      setStatus('Connected – Live-Stream active');
    };

    socket.onmessage = (event) => {
      try {
        // backend sends taxi object every 5 seconds
        const payload = JSON.parse(event.data);
        setTaxis(payload.taxis || []);
      } catch (error) {
        console.error("Error parsing the WebSocket data:", error);
      }
    };

    socket.onclose = () => {
      setStatus('Lost connection to backend');
    };

    return () => socket.close();
  }, []);

  return (
    <div style={{ padding: '20px', fontFamily: 'sans-serif', height: '100vh', display: 'flex', flexDirection: 'column' }}>
      <header style={{ marginBottom: '10px' }}>
        <h1 style={{ margin: 0 }}>🚖 Taxi Live-Tracker</h1>
        <p style={{ margin: '5px 0' }}>
          <strong>Status:</strong> <span style={{ color: status.includes('active') || status.includes('aktiv') ? 'green' : 'red' }}>{status}</span> |
          <strong> Active taxis on the map:</strong> {taxis.length}
        </p>
      </header>

      {/* Karten-Container */}
      <div style={{ flex: 1, borderRadius: '8px', overflow: 'hidden', boxShadow: '0 4px 12px rgba(0,0,0,0.1)' }}>
        <MapContainer
          center={[39.9042, 116.4074]}
          zoom={12}
          style={{ height: '100%', width: '100%' }}
        >
          {/* OpenStreetMap Kacheln laden */}
          <TileLayer
            attribution='&copy; <a href="https://openstreetmap.org">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          {/* Alle Taxis aus dem State als Marker rendern */}
          {taxis.map((taxi) => (
            <Marker
              key={taxi.taxi_id}
              position={[taxi.latitude, taxi.longitude]}
              icon={taxi.isParking ? parkingIcon : defaultIcon}
            >
              <Popup>
                <div style={{ fontSize: '14px' }}>
                  <strong>Taxi ID:</strong> {taxi.taxi_id}<br />
                  <strong>Timestamp:</strong> {taxi.timestamp}<br />
                  <strong>Average Speed:</strong> {taxi.averageSpeed?.toFixed(1)} km/h<br />
                  <strong>Speed:</strong> {taxi.speed?.toFixed(1)} km/h
                  {taxi.isSpeeding ? ' ⚠️ Speeding!' : ''}<br />
                  <strong>Distance:</strong> {taxi.distance?.toFixed(2)} km<br />
                  {taxi.isOutOfArea ? ' ⚠️ Out of Area!' : ''}<br />
                </div>
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    </div>
  );
}

export default App;
