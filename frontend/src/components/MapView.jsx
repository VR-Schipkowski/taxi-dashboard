import { useEffect } from "react";
import {
  MapContainer,
  TileLayer,
  Marker,
  Popup,
  Polyline,
  useMap,
} from "react-leaflet";
import MarkerClusterGroup from "react-leaflet-cluster";

import { pickTaxiIcon, createClusterIcon } from "../utils/taxiIcons.js";

const BEIJING_CENTER = [39.9042, 116.4074];
const DEFAULT_ZOOM = 12;

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

// Renders the full live map: base tiles, clustered taxi markers, the
// selected taxi's recent path, and auto-recentering on selection.
export function MapView({
  taxiMap,
  taxis,
  selectedTaxiId,
  violatingTaxiIds,
  pathPositions,
  onSelectTaxi,
}) {
  return (
    <MapContainer
      center={BEIJING_CENTER}
      zoom={DEFAULT_ZOOM}
      style={{ height: "100%", width: "100%" }}
    >
      <RecenterMap
        selectedTaxi={selectedTaxiId === null ? null : taxiMap[selectedTaxiId]}
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
          const icon = pickTaxiIcon({
            isSelected:
              selectedTaxiId !== null &&
              String(taxi.taxi_id) === String(selectedTaxiId),
            isSpeeding,
            isOutOfArea,
            isParking: taxi.isParking,
          });

          return (
            <Marker
              key={taxi.taxi_id}
              position={[taxi.latitude, taxi.longitude]}
              opacity={taxi._opacity}
              icon={icon}
              eventHandlers={{ click: () => onSelectTaxi(taxi.taxi_id) }}
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
                  <strong>Distance:</strong> {taxi.distance?.toFixed(2)} km
                  <br />
                  {isOutOfArea ? " ⚠️ Out of Area!" : ""}
                </div>
              </Popup>
            </Marker>
          );
        })}
      </MarkerClusterGroup>
    </MapContainer>
  );
}
