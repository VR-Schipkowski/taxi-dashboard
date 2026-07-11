import { useEffect, useRef } from "react";
import { TAG_STYLES } from "../utils/taxiIcons.js";

export function DebugAlerts({
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

  // Only show selected taxi alerts
  const visible = entries.filter((e) => {
    if (!filters[e.type]) return false;
    if (selectedTaxiId !== null && String(e.taxi_id) !== String(selectedTaxiId))
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
                key={`s-${i.taxi_id}`}
                onClick={() => onSelectTaxi(i.taxi_id)}
                style={{ cursor: "pointer", fontSize: 11, padding: "2px 0" }}
              >
                🚖 {i.taxi_id} — {i.speed?.toFixed(1)} km/h
              </div>
            ))}
          {activeFilters.area &&
            areaViolations.map((v) => (
              <div
                key={`a-${v.taxi_id}`}
                onClick={() => onSelectTaxi(v.taxi_id)}
                style={{ cursor: "pointer", fontSize: 11, padding: "2px 0" }}
              >
                🚖 {v.taxi_id} — outside permitted area
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
          Alerts{selectedTaxiId !== null ? ` — Taxi ${selectedTaxiId}` : ""}
        </div>
        <div
          style={{
            display: "flex",
            gap: 6,
            flexWrap: "wrap",
            alignItems: "center",
          }}
        >
          {["speeding", "area"].map((type) => {
            const s = TAG_STYLES[type];
            const active = filters[type];
            return (
              <button
                key={type}
                onClick={() => onToggleFilter(type)}
                title={active ? `Hide ${type} alerts` : `Show ${type} alerts`}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 4,
                  padding: "2px 8px",
                  borderRadius: 4,
                  fontSize: 11,
                  fontWeight: 600,
                  cursor: "pointer",
                  // Active filter reads as "focused": solid status colour, filled
                  // background. Inactive is a plain outline — no greying-out.
                  border: `1px solid ${s.dot}`,
                  background: active ? s.bg : "#fff",
                  color: active ? s.color : "#374151",
                  boxShadow: active ? `inset 0 0 0 1px ${s.dot}` : "none",
                  transition: "all 0.15s",
                }}
              >
                {type}
                <span
                  style={{
                    background: active ? s.dot : "#e5e7eb",
                    color: active ? "#fff" : "#6b7280",
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
      <div ref={logRef} style={{ flex: 1, overflowY: "auto", padding: "4px 0" }}>
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
              entry.taxi_id !== null && entry.taxi_id !== undefined;
            const selected =
              clickable && String(entry.taxi_id) === String(selectedTaxiId);
            return (
              <div
                key={i}
                onClick={() => clickable && onSelectTaxi(entry.taxi_id)}
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
