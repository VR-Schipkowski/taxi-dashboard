import { STALE_AFTER_MS } from "../config.js";

// Fades a marker out over STALE_AFTER_MS after its last update.
export function getOpacity(lastSeenTime, now) {
  if (!lastSeenTime) return 1;
  const age = now - lastSeenTime;
  if (age >= STALE_AFTER_MS) return 0;
  return 1 - age / STALE_AFTER_MS;
}

// Converts a speeding-incident / area-violation record (taxi_id, ...) into
// the same shape as a regular taxi record (taxi_id, ...) so both can be
// rendered by the same marker list.
export function normalizeAlarmTaxi(a) {
  return {
    taxi_id: String(a.taxi_id),
    latitude: a.latitude,
    longitude: a.longitude,
    speed: a.speed,
    averageSpeed: a.averageSpeed,
    distance: a.totalDistance,
    timestamp: a.timestamp,
    isSpeeding: a.isSpeeding,
    isParking: a.isParking,
    _opacity: 1,
  };
}
