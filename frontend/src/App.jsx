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

const speedingIcon = L.icon({
  iconUrl: markerIconPng,
  shadowUrl: markerShadowPng,
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  className: 'speeding-marker',
});

const ooaIcon = L.icon({
  iconUrl: markerIconPng,
  shadowUrl: markerShadowPng,
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  className: 'area-marker',
});

const parkingIcon = L.icon({
  iconUrl: markerIconPng,
  shadowUrl: markerShadowPng,
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  className: 'parking-marker',
});

const selectedIcon = L.icon({
  iconUrl: markerIconPng,
  shadowUrl: markerShadowPng,
  iconSize: [32, 52],
  iconAnchor: [16, 52],
  popupAnchor: [1, -44],
  className: 'selected-marker',
});

const TAG_STYLES = {
  speeding: { bg: '#FAECE7', color: '#993C1D', dot: '#D85A30' },
  area: { bg: '#FAEEDA', color: '#854F0B', dot: '#BA7517' },
  taxiUpdate: { bg: '#E6F1FB', color: '#185FA5', dot: '#378ADD' },
};

const STALE_AFTER_MS = 30 * 1000;

function getOpacity(lastSeenTime, now) {
  if (!lastSeenTime) return 1;
  const age = now - lastSeenTime;
  if (age >= STALE_AFTER_MS) return 0;
  return 1 - age / STALE_AFTER_MS;
}

function RecenterMap({ selectedTaxi }) {
  const map = useMap();

  useEffect(() => {
    if (!selectedTaxi) return;

    map.setView([selectedTaxi.latitude, selectedTaxi.longitude], map.getZoom(), {
      animate: true,
    });
  }, [map, selectedTaxi]);

  return null;
}

function DebugAlerts({ entries, counts, filters, onToggleFilter, onClear, selectedTaxiId, onSelectTaxi }) {
  const logRef = useRef(null);

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = 0;
  }, [entries.length]);

  const visible = entries.filter(e => filters[e.type]);

  return (
    <div style={{
      width: 320,
      flexShrink: 0,
      display: 'flex',
      flexDirection: 'column',
      background: '#fff',
      borderLeft: '1px solid #e5e7eb',
      fontFamily: 'monospace',
      fontSize: 12,
    }}>
      {/* Header */}
      <div style={{ padding: '10px 12px', borderBottom: '1px solid #e5e7eb' }}>
        <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 8, color: '#111' }}>
          Alerts
        </div>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
          {['speeding', 'area', 'taxiUpdate'].map(type => {
            const s = TAG_STYLES[type];
            const label = type === 'taxiUpdate' ? 'updates' : type;
            return (
              <button
                key={type}
                onClick={() => onToggleFilter(type)}
                style={{
                  display: 'inline-flex', alignItems: 'center', gap: 4,
                  padding: '2px 8px', borderRadius: 4, fontSize: 11, fontWeight: 500,
                  cursor: 'pointer', border: `1px solid ${s.dot}`,
                  background: filters[type] ? s.bg : '#f3f4f6',
                  color: filters[type] ? s.color : '#888',
                  opacity: filters[type] ? 1 : 0.6,
                  transition: 'all 0.15s',
                }}
              >
                {label}
                <span style={{
                  background: 'rgba(0,0,0,0.12)', borderRadius: 999,
                  padding: '0 5px', fontSize: 10,
                }}>
                  {counts[type]}
                </span>
              </button>
            );
          })}
          <button
            onClick={onClear}
            style={{
              marginLeft: 'auto', padding: '2px 8px', fontSize: 11,
              border: '1px solid #d1d5db', borderRadius: 4,
              background: 'transparent', cursor: 'pointer', color: '#555',
            }}
          >
            clear
          </button>
        </div>
      </div>

      {/* Log */}
      <div ref={logRef} style={{ flex: 1, overflowY: 'auto', padding: '4px 0' }}>
        {visible.length === 0 ? (
          <div style={{ padding: '2rem 1rem', textAlign: 'center', color: '#aaa' }}>
            waiting for events…
          </div>
        ) : (
          visible.map((entry, i) => {
            const s = TAG_STYLES[entry.type];
            const clickable = entry.taxiId !== null && entry.taxiId !== undefined;
            const selected = clickable && String(entry.taxiId) === String(selectedTaxiId);
            return (
              <div
                key={i}
                onClick={() => clickable && onSelectTaxi(entry.taxiId)}
                style={{
                  display: 'flex', gap: 8, padding: '5px 12px',
                  borderBottom: '1px solid #f3f4f6', alignItems: 'flex-start',
                  cursor: clickable ? 'pointer' : 'default',
                  background: selected ? '#EFF6FF' : 'transparent',
                }}
              >
                <div style={{
                  width: 7, height: 7, borderRadius: '50%',
                  background: s.dot, marginTop: 4, flexShrink: 0,
                }} />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div>
                    <span style={{
                      background: s.bg, color: s.color, fontSize: 10,
                      padding: '1px 5px', borderRadius: 3, marginRight: 5, fontWeight: 600,
                    }}>
                      {entry.type}
                    </span>
                    <span style={{ color: '#222', wordBreak: 'break-all' }}>{entry.text}</span>
                  </div>
                  <div style={{ color: '#aaa', fontSize: 10, marginTop: 2 }}>{entry.ts}</div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

function App() {
  const [taxiMap, setTaxiMap] = useState({});
  const [speedingIncidents, setSpeedingIncidents] = useState([]);
  const [areaViolations, setAreaViolations] = useState([]);
  const [status, setStatus] = useState('Connecting...');
  const [selectedTaxiId, setSelectedTaxiId] = useState(null);

  const [lastSeen, setLastSeen] = useState({});
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const interval = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(interval);
  }, []);

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
    // const socket = new WebSocket('ws://localhost:5001');
    const socket = new WebSocket('ws://34.28.224.202:5001');

    socket.onopen = () => setStatus('Connected – Live-Stream active');

    socket.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        if (data.type === 'snapshot') {
          const map = {};
          const seen = {};
          const receivedAt = Date.now();
          (data.taxis || []).forEach(t => { map[t.taxi_id] = t; seen[t.taxi_id] = receivedAt; });
          setTaxiMap(map);
          setLastSeen(seen);
          setSpeedingIncidents(data.speedingIncidents || []);
          setAreaViolations(data.areaViolations || []);
          addDebugEntry('taxiUpdate', `snapshot — ${data.taxis.length} taxis loaded`);

        } else if (data.type === 'taxiUpdate') {
          const t = data.taxi;
          setTaxiMap(prev => ({ ...prev, [t.taxi_id]: t }));
          setLastSeen(prev => ({ ...prev, [t.taxi_id]: Date.now() }));
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

  const violatingTaxiIds = new Set(areaViolations.map(v => String(v.taxiId)));

  const visibleTaxis = allTaxis
      .map(t => ({ ...t, _opacity: getOpacity(lastSeen[t.taxi_id], now) }))
      .filter(t => t._opacity > 0);

  const taxis = selectedTaxiId === null
      ? visibleTaxis
      : visibleTaxis.filter(t => String(t.taxi_id) === String(selectedTaxiId));
  const isConnected = status.includes('active') || status.includes('aktiv');

  return (
    <div style={{ padding: 0, fontFamily: 'sans-serif', height: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <header style={{
        padding: '10px 16px', display: 'flex', alignItems: 'center',
        gap: 12, borderBottom: '1px solid #e5e7eb', flexShrink: 0,
        background: '#fff',
      }}>
        <h1 style={{margin: 0, fontSize: 18}}>🚖 Taxi Live-Tracker</h1>
        <span style={{fontSize: 13, color: '#555'}}>
          <strong>Status:</strong>{' '}
          <span style={{color: isConnected ? '#16a34a' : '#dc2626'}}>{status}</span>
        </span>
        <span style={{fontSize: 13, color: '#555'}}>
          <strong>Active:</strong> {visibleTaxis.length}
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
      <div style={{flex: 1, display: 'flex', overflow: 'hidden'}}>
        {/* Map */}
        <div style={{flex: 1, overflow: 'hidden' }}>
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
            {taxis.map((taxi) => {
              const isSpeeding = taxi.isSpeeding;
              const isOutOfArea = violatingTaxiIds.has(String(taxi.taxi_id));

              let icon;
              if (selectedTaxiId !== null && String(taxi.taxi_id) === String(selectedTaxiId)) {
                icon = selectedIcon;
              } else if (isSpeeding) {
                icon = speedingIcon;
              } else if (isOutOfArea) {
                icon = ooaIcon;
              } else if (taxi.isParking) {
                icon = parkingIcon;
              } else {
                icon = defaultIcon;
              }

              return (
                  <Marker
                      key={taxi.taxi_id}
                      position={[taxi.latitude, taxi.longitude]}
                      opacity={taxi._opacity}
                      icon={icon}
                  >
                    <Popup>
                      <div style={{ fontSize: 14 }}>
                        <strong>Taxi ID:</strong> {taxi.taxi_id}<br />
                        <strong>Timestamp:</strong> {taxi.timestamp}<br />
                        <strong>Average Speed:</strong> {taxi.averageSpeed?.toFixed(1)} km/h<br />
                        <strong>Speed:</strong> {taxi.speed?.toFixed(1)} km/h
                        {isSpeeding ? ' ⚠️ Speeding!' : ''}<br />
                        <strong>Distance:</strong> {taxi.distance?.toFixed(2)} km<br />
                        {isOutOfArea ? ' ⚠️ Out of Area!' : ''}
                      </div>
                    </Popup>
                  </Marker>
              );
            })}
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