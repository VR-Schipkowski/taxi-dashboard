import { useEffect, useRef, useMemo, useState } from "react";

import {
  MapContainer,
  TileLayer,
  Marker,
  Popup,
  Polyline,
  CircleMarker,
  useMap,
} from "react-leaflet";
import L from "leaflet";
import "./App.css";
import "leaflet/dist/leaflet.css";
import MarkerClusterGroup from "react-leaflet-cluster";

// Todo: Refactor panels in seperate components
import {
  BACKEND,
  PATH_LOCATIONS_LIMIT,
  PATH_TIME_INTERVAL,
  STALE_AFTER_MS,
  DEBUG_LOG_MAX_ENTRIES,
  NOW_UPDATE_INTERVAL_MS,
  TAXI_UPDATE_FLUSH_INTERVAL_MS,
} from "./config.js";
import { useDebugLog } from "./hooks/UseDebugLog.js";
import { useNow } from "./hooks/UseNow";
import { useTaxiPath } from "./hooks/UseTaxiPath";
import { useTaxiSocket } from "./hooks/UseTaxiSocket";

// create icons per taxi
function createDotIcon({
  color,
  size = 14,
  variant = "default",
  ring = false,
  ringColor,
}) {
  return L.divIcon({
    className: `taxi-dot-icon dot-${variant}`,
    html: `<div class="taxi-dot" style="width:${size}px;height:${size}px;background:${color};${ring ? `box-shadow:0 0 0 3px ${ringColor}, 0 1px 3px rgba(0,0,0,0.4);` : ""}"></div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    popupAnchor: [0, -size / 2 - 6],
  });
}

const defaultIcon = createDotIcon({ color: "#378ADD", variant: "default" });
const speedingIcon = createDotIcon({
  color: "#E2462F",
  size: 16,
  variant: "speeding",
});
const ooaIcon = createDotIcon({ color: "#1F9D55", size: 16, variant: "area" });
const parkingIcon = createDotIcon({ color: "#9CA3AF", variant: "parking" });
const selectedIcon = createDotIcon({
  color: "#1D4ED8",
  size: 20,
  variant: "selected",
  ring: true,
  ringColor: "rgba(29,78,216,0.35)",
});

const TAG_STYLES = {
  speeding: { bg: "#FAECE7", color: "#993C1D", dot: "#D85A30" },
  area: { bg: "#E6F6ED", color: "#1F7A43", dot: "#1F9D55" },
  taxiUpdate: { bg: "#E6F1FB", color: "#185FA5", dot: "#378ADD" },
};

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

    map.setView(
      [selectedTaxi.latitude, selectedTaxi.longitude],
      map.getZoom(),
      {
        animate: true,
      },
    );
  }, [map, selectedTaxi]);

  return null;
}
// Taxi search box to  display taxi path
function TaxiSearchBox({ onSelect, onClear, selectedTaxiId }) {
  const [value, setValue] = useState("");

  function handleSubmit(e) {
    e.preventDefault();
    const trimmed = value.trim();
    if (trimmed === "") return;
    onSelect(trimmed);
  }
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === "Escape" && selectedTaxiId !== null) {
        setValue("");
        onClear();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [selectedTaxiId, setValue, onClear]);

  return (
    <form
      onSubmit={handleSubmit}
      style={{ display: "flex", gap: 6, alignItems: "center" }}
    >
      <input
        type="text"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        placeholder="select taxi"
        style={{
          padding: "4px 8px",
          fontSize: 13,
          border: "1px solid #d1d5db",
          borderRadius: 4,
          width: 120,
        }}
      />
      <button
        type="submit"
        style={{
          padding: "4px 10px",
          fontSize: 13,
          border: "1px solid #d1d5db",
          borderRadius: 4,
          background: "#fff",
          cursor: "pointer",
        }}
      >
        Search
      </button>
      {selectedTaxiId !== null && (
        <button
          type="button"
          onClick={() => {
            setValue("");
            onClear();
          }}
          style={{
            padding: "4px 10px",
            fontSize: 13,
            border: "1px solid #d1d5db",
            borderRadius: 4,
            background: "#fff",
            cursor: "pointer",
            color: "#555",
          }}
        >
          deselect
        </button>
      )}
    </form>
  );
}

function DebugAlerts({
  entries,
  counts,
  filters,
  onToggleFilter,
  onClear,
  selectedTaxiId,
  onSelectTaxi,
  activeFilters,
  speedingIncidents,
  areaViolations,
}) {
  const logRef = useRef(null);

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = 0;
  }, [entries.length]);

  // Only show selected taxi allerts
  const visible = entries.filter((e) => {
    if (!filters[e.type]) return false;
    if (selectedTaxiId !== null && String(e.taxiId) !== String(selectedTaxiId))
      return false;
    return true;
  });

  return (
    <div
      style={{
        width: 320,
        flexShrink: 0,
        display: "flex",
        flexDirection: "column",
        background: "#fff",
        borderLeft: "1px solid #e5e7eb",
        fontFamily: "monospace",
        fontSize: 12,
      }}
    >
      {(activeFilters.speeding || activeFilters.area) && (
        <div
          style={{
            padding: "8px 12px",
            borderBottom: "1px solid #e5e7eb",
            background: "#FAFAFA",
          }}
        >
          <div style={{ fontWeight: 600, fontSize: 12, marginBottom: 6 }}>
            Current Violators
          </div>
          {activeFilters.speeding &&
            speedingIncidents.map((i) => (
              <div
                key={`s-${i.taxiId}`}
                onClick={() => onSelectTaxi(i.taxiId)}
                style={{ cursor: "pointer", fontSize: 11, padding: "2px 0" }}
              >
                🚖 {i.taxiId} — {i.speed?.toFixed(1)} km/h
              </div>
            ))}
          {activeFilters.area &&
            areaViolations.map((v) => (
              <div
                key={`a-${v.taxiId}`}
                onClick={() => onSelectTaxi(v.taxiId)}
                style={{ cursor: "pointer", fontSize: 11, padding: "2px 0" }}
              >
                🚖 {v.taxiId} — outside permitted area
              </div>
            ))}
        </div>
      )}
      {/* Header */}
      <div style={{ padding: "10px 12px", borderBottom: "1px solid #e5e7eb" }}>
        <div
          style={{
            fontWeight: 600,
            fontSize: 13,
            marginBottom: 8,
            color: "#111",
          }}
        >
          Alerts
        </div>
        <div
          style={{
            display: "flex",
            gap: 6,
            flexWrap: "wrap",
            alignItems: "center",
          }}
        >
          {["speeding", "area", "taxiUpdate"].map((type) => {
            const s = TAG_STYLES[type];
            const label = type === "taxiUpdate" ? "updates" : type;
            return (
              <button
                key={type}
                onClick={() => onToggleFilter(type)}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 4,
                  padding: "2px 8px",
                  borderRadius: 4,
                  fontSize: 11,
                  fontWeight: 500,
                  cursor: "pointer",
                  border: `1px solid ${s.dot}`,
                  background: filters[type] ? s.bg : "#f3f4f6",
                  color: filters[type] ? s.color : "#888",
                  opacity: filters[type] ? 1 : 0.6,
                  transition: "all 0.15s",
                }}
              >
                {label}
                <span
                  style={{
                    background: "rgba(0,0,0,0.12)",
                    borderRadius: 999,
                    padding: "0 5px",
                    fontSize: 10,
                  }}
                >
                  {counts[type]}
                </span>
              </button>
            );
          })}
          <button
            onClick={onClear}
            style={{
              marginLeft: "auto",
              padding: "2px 8px",
              fontSize: 11,
              border: "1px solid #d1d5db",
              borderRadius: 4,
              background: "transparent",
              cursor: "pointer",
              color: "#555",
            }}
          >
            clear
          </button>
        </div>
      </div>

      {/* Log */}
      <div
        ref={logRef}
        style={{ flex: 1, overflowY: "auto", padding: "4px 0" }}
      >
        {visible.length === 0 ? (
          <div
            style={{ padding: "2rem 1rem", textAlign: "center", color: "#aaa" }}
          >
            waiting for events…
          </div>
        ) : (
          visible.map((entry, i) => {
            const s = TAG_STYLES[entry.type];
            const clickable =
              entry.taxiId !== null && entry.taxiId !== undefined;
            const selected =
              clickable && String(entry.taxiId) === String(selectedTaxiId);
            return (
              <div
                key={i}
                onClick={() => clickable && onSelectTaxi(entry.taxiId)}
                style={{
                  display: "flex",
                  gap: 8,
                  padding: "5px 12px",
                  borderBottom: "1px solid #f3f4f6",
                  alignItems: "flex-start",
                  cursor: clickable ? "pointer" : "default",
                  background: selected ? "#EFF6FF" : "transparent",
                }}
              >
                <div
                  style={{
                    width: 7,
                    height: 7,
                    borderRadius: "50%",
                    background: s.dot,
                    marginTop: 4,
                    flexShrink: 0,
                  }}
                />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div>
                    <span
                      style={{
                        background: s.bg,
                        color: s.color,
                        fontSize: 10,
                        padding: "1px 5px",
                        borderRadius: 3,
                        marginRight: 5,
                        fontWeight: 600,
                      }}
                    >
                      {entry.type}
                    </span>
                    <span style={{ color: "#222", wordBreak: "break-all" }}>
                      {entry.text}
                    </span>
                  </div>
                  <div style={{ color: "#aaa", fontSize: 10, marginTop: 2 }}>
                    {entry.ts}
                  </div>
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
  const now = useNow();
  const debugLog = useDebugLog();
  const [selectedTaxiId, setSelectedTaxiId] = useState(null);
  const { pathLocations, pathError, appendLiveUpdate } =
    useTaxiPath(selectedTaxiId);

  // Wire socket events -> debug log + path trail. useTaxiSocket only owns
  // its own state; side effects like these are handled by the callbacks.
  const {
    taxiMap,
    lastSeen,
    speedingIncidents,
    areaViolations,
    status,
    latency,
    latencyTrend,
  } = useTaxiSocket(BACKEND, {
    onSnapshot: (data) => {
      debugLog.addEntry(
        "taxiUpdate",
        `snapshot — ${data.taxis.length} taxis loaded`,
      );
    },
    onTaxiUpdate: (t) => {
      debugLog.addEntry(
        "taxiUpdate",
        `taxi ${t.taxi_id} → (${t.latitude.toFixed(4)}, ${t.longitude.toFixed(4)}) ${t.speed.toFixed(1)} km/h${t.isSpeeding ? " ⚡" : ""}${t.isParking ? " 🅿" : ""}`,
        t.taxi_id,
      );
      appendLiveUpdate(t);
    },
    onSpeedingAlert: (incidents) => {
      incidents.forEach((i) => {
        debugLog.addEntry(
          "speeding",
          `taxi ${i.taxiId} — ${i.speed.toFixed(1)} km/h`,
          i.taxiId,
        );
      });
    },
    onAreaViolation: (violations) => {
      violations.forEach((v) => {
        debugLog.addEntry(
          "area",
          `taxi ${v.taxiId} outside permitted area`,
          v.taxiId,
        );
      });
    },
  });

  const totalDistanceAll = useMemo(
    () =>
      Object.values(taxiMap).reduce(
        (sum, taxi) => sum + (taxi.totalDistance || 0),
        0,
      ),
    [taxiMap],
  );

  // filtering
  const [activeFilters, setActiveFilters] = useState({
    speeding: false,
    area: false,
  });

  function toggleViolationFilter(type) {
    setActiveFilters((prev) => ({ ...prev, [type]: !prev[type] }));
  }

  function selectTaxiFromAlert(taxiId) {
    setSelectedTaxiId((prev) =>
      String(prev) === String(taxiId) ? null : taxiId,
    );
  }

  // Used both by the search box and by clicking a marker/alert.
  function selectTaxi(taxiId) {
    setSelectedTaxiId(taxiId);
  }

  function clearSelection() {
    setSelectedTaxiId(null);
  }

  // clusters for better overview
  function createClusterIcon(cluster) {
    const childMarkers = cluster.getAllChildMarkers();
    const count = childMarkers.length;

    let hasSpeeding = false;
    let hasArea = false;
    childMarkers.forEach((m) => {
      const cls = m.options.icon?.options?.className || "";
      if (cls.includes("dot-speeding")) hasSpeeding = true;
      else if (cls.includes("dot-area")) hasArea = true;
    });

    let variant = "normal";
    if (hasSpeeding) variant = "speeding";
    else if (hasArea) variant = "area";

    let size, sizeTier;
    if (count < 10) {
      size = 36;
      sizeTier = "sm";
    } else if (count < 50) {
      size = 44;
      sizeTier = "md";
    } else {
      size = 52;
      sizeTier = "lg";
    }

    return L.divIcon({
      html: `<div class="cluster-inner">${count}</div>`,
      className: `taxi-cluster cluster-${variant} cluster-${sizeTier}`,
      iconSize: L.point(size, size),
    });
  }

  const allTaxis = Object.values(taxiMap);

  const violatingTaxiIds = useMemo(
    () => new Set(areaViolations.map((v) => String(v.taxiId))),
    [areaViolations],
  );

  const visibleTaxis = useMemo(
    () =>
      allTaxis
        .map((t) => ({ ...t, _opacity: getOpacity(lastSeen[t.taxi_id], now) }))
        .filter((t) => t._opacity > 0),
    [allTaxis, lastSeen, now],
  );

  const hasActiveFilter = activeFilters.speeding || activeFilters.area;

  const filteredByViolation = useMemo(() => {
    if (!hasActiveFilter) return visibleTaxis;
    const violatorMap = {};
    if (activeFilters.speeding) {
      speedingIncidents.forEach((i) => {
        violatorMap[i.taxiId] = normalizeAlarmTaxi(i);
      });
    }
    if (activeFilters.area) {
      areaViolations.forEach((v) => {
        violatorMap[v.taxiId] = {
          ...violatorMap[v.taxiId],
          ...normalizeAlarmTaxi(v),
        };
      });
    }
    return Object.values(violatorMap);
  }, [
    hasActiveFilter,
    activeFilters,
    visibleTaxis,
    speedingIncidents,
    areaViolations,
  ]);

  const taxis = useMemo(
    () =>
      selectedTaxiId === null
        ? filteredByViolation
        : filteredByViolation.filter(
            (t) => String(t.taxi_id) === String(selectedTaxiId),
          ),
    [selectedTaxiId, filteredByViolation],
  );

  const isConnected = status.includes("active") || status.includes("aktiv");

  const pathPositions = useMemo(
    () =>
      pathLocations
        .map((p) => [Number(p.latitude), Number(p.longitude)])
        .filter(([lat, lng]) => Number.isFinite(lat) && Number.isFinite(lng)),
    [pathLocations],
  );

  return (
    <div
      style={{
        padding: 0,
        fontFamily: "sans-serif",
        height: "100vh",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* Header */}
      <header
        style={{
          padding: "10px 16px",
          display: "flex",
          alignItems: "center",
          gap: 12,
          borderBottom: "1px solid #e5e7eb",
          flexShrink: 0,
          background: "#fff",
        }}
      >
        <h1 style={{ margin: 0, fontSize: 18 }}>🚖 Taxi Live-Tracker</h1>
        <span style={{ fontSize: 13, color: "#555" }}>
          <strong>Status:</strong>{" "}
          <span style={{ color: isConnected ? "#16a34a" : "#dc2626" }}>
            {status}
          </span>
        </span>
        <span style={{ fontSize: 13, color: "#555" }}>
          <strong>Active:</strong> {visibleTaxis.length}
        </span>
        <span style={{ fontSize: 13, color: "#555" }}>
          <strong>Total distance travelled:</strong>{" "}
          {totalDistanceAll != null
            ? `${totalDistanceAll.toFixed(1)} km`
            : "N/A"}
        </span>

        <TaxiSearchBox
          onSelect={selectTaxi}
          onClear={clearSelection}
          selectedTaxiId={selectedTaxiId}
        />

        {selectedTaxiId !== null && (
          <span
            style={{
              fontSize: 12,
              background: "#EFF6FF",
              color: "#1D4ED8",
              padding: "2px 8px",
              borderRadius: 4,
              fontWeight: 500,
            }}
          >
            focused taxi: {selectedTaxiId}
          </span>
        )}
        {speedingIncidents.length > 0 && (
          <button
            onClick={() => toggleViolationFilter("speeding")}
            style={{
              fontSize: 12,
              fontWeight: 500,
              borderRadius: 4,
              cursor: "pointer",
              border: "1px solid #1F9D55",
              padding: "2px 8px",
              background: activeFilters.speeding ? "#993C1D" : "#FAECE7",
              color: activeFilters.speeding ? "#fff" : "#993C1D",
            }}
          >
            ⚠️ {speedingIncidents.length} speeding
          </button>
        )}
        {areaViolations.length > 0 && (
          <button
            onClick={() => toggleViolationFilter("area")}
            style={{
              fontSize: 12,
              fontWeight: 500,
              borderRadius: 4,
              cursor: "pointer",
              border: "1px solid #1F9D55",
              padding: "2px 8px",
              background: activeFilters.area ? "#1F7A43" : "#E6F6ED",
              color: activeFilters.area ? "#fff" : "#1F7A43",
            }}
          >
            🗺️ {areaViolations.length} area violations
          </button>
        )}
        <span style={{ fontSize: 13, color: "#555" }}>
          <strong>Latency:</strong>{" "}
          {latency !== null ? `${latency.toFixed(2)}s` : "N/A"}
          {latencyTrend === "up" && (
            <span style={{ color: "#dc2626", marginLeft: 4 }}>↑</span>
          )}
          {latencyTrend === "down" && (
            <span style={{ color: "#16a34a", marginLeft: 4 }}>↓</span>
          )}
        </span>
        {pathError && (
          <span
            style={{
              fontSize: 12,
              background: "#FAECE7",
              color: "#993C1D",
              padding: "2px 8px",
              borderRadius: 4,
            }}
          >
            {pathError}
          </span>
        )}
      </header>

      {/* Main content */}
      <div style={{ flex: 1, display: "flex", overflow: "hidden" }}>
        {/* Map */}
        <div style={{ flex: 1, overflow: "hidden" }}>
          <MapContainer
            center={[39.9042, 116.4074]}
            zoom={12}
            style={{ height: "100%", width: "100%" }}
          >
            <RecenterMap
              selectedTaxi={
                selectedTaxiId === null ? null : taxiMap[selectedTaxiId]
              }
            />
            <TileLayer
              attribution='&copy; <a href="https://openstreetmap.org">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            <MarkerClusterGroup
              chunkedLoading
              iconCreateFunction={createClusterIcon}
              maxClusterRadius={60}
              spiderfyOnMaxZoom={true}
              disableClusteringAtZoom={16}
              showCoverageOnHover={false}
            >
              {selectedTaxiId !== null && pathPositions.length > 1 && (
                <Polyline
                  key={`poly-${selectedTaxiId}`}
                  positions={pathPositions}
                  pathOptions={{ color: "#1D4ED8", weight: 3, opacity: 0.7 }}
                />
              )}

              {taxis.map((taxi) => {
                const isSpeeding = taxi.isSpeeding;
                const isOutOfArea = violatingTaxiIds.has(String(taxi.taxi_id));

                let icon;
                if (
                  selectedTaxiId !== null &&
                  String(taxi.taxi_id) === String(selectedTaxiId)
                ) {
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
                    eventHandlers={{ click: () => selectTaxi(taxi.taxi_id) }}
                  >
                    <Popup>
                      <div style={{ fontSize: 14 }}>
                        <strong>Taxi ID:</strong> {taxi.taxi_id}
                        <br />
                        <strong>Timestamp:</strong> {taxi.timestamp}
                        <br />
                        <strong>Average Speed:</strong>{" "}
                        {taxi.averageSpeed?.toFixed(1)} km/h
                        <br />
                        <strong>Speed:</strong> {taxi.speed?.toFixed(1)} km/h
                        {isSpeeding ? " ⚠️ Speeding!" : ""}
                        <br />
                        <strong>Distance:</strong> {taxi.distance?.toFixed(2)}{" "}
                        km
                        <br />
                        {isOutOfArea ? " ⚠️ Out of Area!" : ""}
                      </div>
                    </Popup>
                  </Marker>
                );
              })}
            </MarkerClusterGroup>
          </MapContainer>
        </div>

        {/* Debug panel */}
        <DebugAlerts
          entries={debugLog.entries}
          counts={debugLog.counts}
          filters={debugLog.filters}
          onToggleFilter={debugLog.toggleFilter}
          onClear={debugLog.clear}
          selectedTaxiId={selectedTaxiId}
          onSelectTaxi={selectTaxiFromAlert}
          activeFilters={activeFilters}
          speedingIncidents={speedingIncidents}
          areaViolations={areaViolations}
        />
      </div>
    </div>
  );
}

export default App;
