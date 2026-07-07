import { useCallback, useState } from 'react';
import { DEBUG_LOG_MAX_ENTRIES } from '../config';

/**
 * Owns the debug/alerts log: entries, per-type counts, and per-type
 * visibility filters. Pure UI-side bookkeeping, no socket knowledge.
 */
export function useDebugLog(maxEntries = DEBUG_LOG_MAX_ENTRIES) {
  const [entries, setEntries] = useState([]);
  const [counts, setCounts] = useState({ speeding: 0, area: 0, taxiUpdate: 0 });
  const [filters, setFilters] = useState({ speeding: true, area: true, taxiUpdate: true });

  const addEntry = useCallback((type, text, taxiId = null) => {
    const now = new Date();
    const ts = now.toTimeString().slice(0, 8) + '.' + String(now.getMilliseconds()).padStart(3, '0');
    setEntries(prev => [{ type, text, ts, taxiId }, ...prev].slice(0, maxEntries));
    setCounts(prev => ({ ...prev, [type]: prev[type] + 1 }));
  }, [maxEntries]);

  const toggleFilter = useCallback((type) => {
    setFilters(prev => ({ ...prev, [type]: !prev[type] }));
  }, []);

  const clear = useCallback(() => {
    setEntries([]);
    setCounts({ speeding: 0, area: 0, taxiUpdate: 0 });
  }, []);

  return { entries, counts, filters, addEntry, toggleFilter, clear };
}