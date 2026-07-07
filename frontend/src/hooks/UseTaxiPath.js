import { useCallback, useEffect, useRef, useState } from 'react';
import { BACKEND, PATH_LOCATIONS_LIMIT, PATH_TIME_INTERVAL } from '../config';

/**
 * Owns the recent-path trail for whichever taxi is currently selected.
 *
 * - Loads the last `limit` locations from the REST API whenever
 *   `selectedTaxiId` changes.
 * - Exposes `appendLiveUpdate(taxi)` so a live WebSocket taxiUpdate can be
 *   appended to the trail. It only appends when the update belongs to the
 *   currently selected taxi (checked via a ref so callers don't need to
 *   worry about stale closures).
 */
export function useTaxiPath(
  selectedTaxiId,
  { apiBase = BACKEND, timeIntervalMin = PATH_TIME_INTERVAL, limit = PATH_LOCATIONS_LIMIT } = {}
) {
  const [pathLocations, setPathLocations] = useState([]);
  const [pathError, setPathError] = useState(null);

  const selectedTaxiIdRef = useRef(selectedTaxiId);
  useEffect(() => {
    selectedTaxiIdRef.current = selectedTaxiId;
  }, [selectedTaxiId]);

  useEffect(() => {
    if (selectedTaxiId === null) {
      setPathLocations([]);
      setPathError(null);
      return;
    }

    let cancelled = false;
    setPathError(null);

    fetch(`${apiBase}/taxis/${selectedTaxiId}/locations?time_interval=${timeIntervalMin}&number=${limit}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => {
        if (cancelled) return;
        setPathLocations([...data]);
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('error loading path:', err);
        setPathLocations([]);
        setPathError('could not load');
      });

    return () => { cancelled = true; };
  }, [selectedTaxiId, apiBase, timeIntervalMin, limit]);

  const appendLiveUpdate = useCallback((taxi) => {
    if (String(selectedTaxiIdRef.current) !== String(taxi.taxi_id)) return;

    setPathLocations((prev) => {
      const updated = [
        ...prev,
        {
          latitude: taxi.latitude,
          longitude: taxi.longitude,
          timestamp: taxi.timestamp,
          speed: taxi.speed,
          averageSpeed: taxi.averageSpeed,
          totalDistance: taxi.totalDistance,
          isSpeeding: taxi.isSpeeding,
          isParking: taxi.isParking,
        },
      ];
      return updated.slice(-limit);
    });
  }, [limit]);

  return { pathLocations, pathError, appendLiveUpdate };
}