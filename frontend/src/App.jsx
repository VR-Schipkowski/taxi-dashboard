import { useState, useEffect, useRef } from 'react';
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet';
import L from 'leaflet';
import './App.css';
import 'leaflet/dist/leaflet.css';
import markerIconPng from 'leaflet/dist/images/marker-icon.png';
import markerShadowPng from 'leaflet/dist/images/marker-shadow.png';
// Todo: Refactor panels in seperate components
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
  const [taxiMap, setTaxiMap] = useState({});
  const [speedingIncidents, setSpeedingIncidents] = useState([]);
  const [areaViolations, setAreaViolations] = useState([]);
  const [status, setStatus] = useState('Connecting...');
  const [selectedTaxiId, setSelectedTaxiId] = useState(null);

  // Debug alerts state
  const [debugEntries, setDebugEntries] = useState([]);
  const [debugCounts, setDebugCounts] = useState({ speeding: 0, area: 0, taxiUpdate: 0 });
  const [debugFilters, setDebugFilters] = useState({ speeding: true, area: true, taxiUpdate: true });

  function addDebugEntry(type, text, taxiId = null) {
    const now = new Date();
    const ts = now.toTimeString().slice(0, 8) + '.' + String(now.getMilliseconds()).padStart(3, '0');
    setDebugEntries(prev => [{ type, text, ts, taxiId }, ...prev].slice(0, 300));
    setDebugCounts(prev => ({ ...prev, [type]: prev[type] + 1 }));
  }

  function toggleFilter(type) {
    setDebugFilters(prev => ({ ...prev, [type]: !prev[type] }));
  }

  function clearDebug() {
    setDebugEntries([]);
    setDebugCounts({ speeding: 0, area: 0, taxiUpdate: 0 });
  }

  function selectTaxiFromAlert(taxiId) {
    setSelectedTaxiId(prev => (String(prev) === String(taxiId) ? null : taxiId));
  }

  useEffect(() => {
    const socket = new WebSocket('ws://34.32.19.27:5001');

    socket.onopen = () => setStatus('Connected – Live-Stream active');

    socket.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        if (data.type === 'snapshot') {
          const map = {};
          (data.taxis || []).forEach(t => { map[t.taxi_id] = t; });
          setTaxiMap(map);
          setSpeedingIncidents(data.speedingIncidents || []);
          setAreaViolations(data.areaViolations || []);
          addDebugEntry('taxiUpdate', `snapshot — ${data.taxis.length} taxis loaded`);

        } else if (data.type === 'taxiUpdate') {
          const t = data.taxi;
          setTaxiMap(prev => ({ ...prev, [t.taxi_id]: t }));
          addDebugEntry(
            'taxiUpdate',
            `taxi ${t.taxi_id} → (${t.latitude.toFixed(4)}, ${t.longitude.toFixed(4)}) ${t.speed.toFixed(1)} km/h${t.isSpeeding ? ' ⚡' : ''}${t.isParking ? ' 🅿' : ''}`,
            t.taxi_id
          );

        } else if (data.type === 'speedingAlert') {
          const i = data.incident;
          setSpeedingIncidents(data.speedingIncidents || []);
          addDebugEntry('speeding', `taxi ${i.taxiId} — ${i.speed.toFixed(1)} km/h`, i.taxiId);

        } else if (data.type === 'areaViolation') {
          const v = data.violation;
          setAreaViolations(data.areaViolations || []);
          addDebugEntry('area', `taxi ${v.taxiId} outside permitted area`, v.taxiId);
        }

      } catch (error) {
        console.error('Error parsing WebSocket data:', error);
      }
    };

    socket.onclose = () => setStatus('Lost connection to backend');

    return () => socket.close();
  }, []);

  const allTaxis = Object.values(taxiMap);
  const taxis = selectedTaxiId === null
    ? allTaxis
    : allTaxis.filter(t => String(t.taxi_id) === String(selectedTaxiId));
  const isConnected = status.includes('active') || status.includes('aktiv');

  return (
    <div style={{ padding: 0, fontFamily: 'sans-serif', height: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <header style={{
        padding: '10px 16px', display: 'flex', alignItems: 'center',
        gap: 12, borderBottom: '1px solid #e5e7eb', flexShrink: 0,
        background: '#fff',
      }}>
        <h1 style={{ margin: 0, fontSize: 18 }}>🚖 Taxi Live-Tracker</h1>
        <span style={{ fontSize: 13, color: '#555' }}>
          <strong>Status:</strong>{' '}
          <span style={{ color: isConnected ? '#16a34a' : '#dc2626' }}>{status}</span>
        </span>
        <span style={{ fontSize: 13, color: '#555' }}>
          <strong>Active:</strong> {allTaxis.length}
        </span>
        {selectedTaxiId !== null && (
          <span style={{ fontSize: 12, background: '#EFF6FF', color: '#1D4ED8', padding: '2px 8px', borderRadius: 4, fontWeight: 500 }}>
            focused taxi: {selectedTaxiId}
          </span>
        )}
        {speedingIncidents.length > 0 && (
          <span style={{
            fontSize: 12, background: '#FAECE7', color: '#993C1D',
            padding: '2px 8px', borderRadius: 4, fontWeight: 500,
          }}>
            ⚠️ {speedingIncidents.length} speeding
          </span>
        )}
        {areaViolations.length > 0 && (
          <span style={{
            fontSize: 12, background: '#FAEEDA', color: '#854F0B',
            padding: '2px 8px', borderRadius: 4, fontWeight: 500,
          }}>
            🗺️ {areaViolations.length} area violations
          </span>
        )}

      </header>

      {/* Main content */}
      <div style={{ flex: 1, display: 'flex', overflow: 'hidden' }}>
        {/* Map */}
        <div style={{ flex: 1, overflow: 'hidden' }}>
          <MapContainer
            center={[39.9042, 116.4074]}
            zoom={12}
            style={{ height: '100%', width: '100%' }}
          >
            <RecenterMap selectedTaxi={selectedTaxiId === null ? null : taxiMap[selectedTaxiId]} />
            <TileLayer
              attribution='&copy; <a href="https://openstreetmap.org">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            {taxis.map((taxi) => (
              <Marker
                key={taxi.taxi_id}
                position={[taxi.latitude, taxi.longitude]}
                icon={selectedTaxiId !== null && String(taxi.taxi_id) === String(selectedTaxiId)
                  ? selectedIcon
                  : (taxi.isParking ? parkingIcon : defaultIcon)}
              >
                <Popup>
                  <div style={{ fontSize: 14 }}>
                    <strong>Taxi ID:</strong> {taxi.taxi_id}<br />
                    <strong>Timestamp:</strong> {taxi.timestamp}<br />
                    <strong>Average Speed:</strong> {taxi.averageSpeed?.toFixed(1)} km/h<br />
                    <strong>Speed:</strong> {taxi.speed?.toFixed(1)} km/h
                    {taxi.isSpeeding ? ' ⚠️ Speeding!' : ''}<br />
                    <strong>Distance:</strong> {taxi.distance?.toFixed(2)} km<br />
                    {taxi.isOutOfArea ? ' ⚠️ Out of Area!' : ''}
                  </div>
                </Popup>
              </Marker>
            ))}
          </MapContainer>
        </div>

        {/* Debug panel */}
        <DebugAlerts
          entries={debugEntries}
          counts={debugCounts}
          filters={debugFilters}
          onToggleFilter={toggleFilter}
          onClear={clearDebug}
          selectedTaxiId={selectedTaxiId}
          onSelectTaxi={selectTaxiFromAlert}
        />
      </div>
    </div>
  );
}

export default App;