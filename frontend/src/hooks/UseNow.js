import { useEffect, useState } from "react";
import { NOW_UPDATE_INTERVAL_MS } from "../config";

/**
 * Returns the current timestamp (ms), re-rendering every `intervalMs`.
 * Used to drive time-based effects like marker fade-out.
 */
export function useNow(intervalMs = NOW_UPDATE_INTERVAL_MS) {
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const interval = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(interval);
  }, [intervalMs]);

  return now;
}
