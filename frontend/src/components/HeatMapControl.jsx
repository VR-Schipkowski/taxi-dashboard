import { useEffect, useRef, useState } from "react";
import L from "leaflet";
import { HeatMapLayer } from "./HeatMapLayer.jsx";

const DEFAULTS = { radius: 70, blur: 45, maxZoom: 5, max: 600 };

export function HeatMapControl({ cells }) {
  const [enabled, setEnabled] = useState(true);
  const [open, setOpen] = useState(false);
  const [radius, setRadius] = useState(DEFAULTS.radius);
  const [blur, setBlur] = useState(DEFAULTS.blur);
  const [maxZoom, setMaxZoom] = useState(DEFAULTS.maxZoom);
  const [max, setMax] = useState(DEFAULTS.max);
  const controlRef = useRef(null);

  useEffect(() => {
    if (!controlRef.current) return;

    L.DomEvent.disableClickPropagation(controlRef.current);
    L.DomEvent.disableScrollPropagation(controlRef.current);
  }, []);

  return (
    <>
      {enabled && (
        <HeatMapLayer
          cells={cells}
          radius={radius}
          blur={blur}
          maxZoom={maxZoom}
          max={max}
        />
      )}

      <div ref={controlRef} style={wrapperStyle}>
        <button
          onClick={() => setOpen((o) => !o)}
          style={buttonStyle}
          title="Heatmap settings"
        >
          🔥
        </button>

        {open && (
          <div style={panelStyle}>
            <label
              style={{
                ...rowStyle,
                display: "flex",
                alignItems: "center",
                gap: 6,
              }}
            >
              <input
                type="checkbox"
                checked={enabled}
                onChange={(e) => setEnabled(e.target.checked)}
              />
              Show heatmap
            </label>

            <SliderRow
              label="Radius"
              value={radius}
              min={50}
              max={150}
              onChange={setRadius}
            />
            <SliderRow
              label="Blur"
              value={blur}
              min={0}
              max={100}
              onChange={setBlur}
            />
            <SliderRow
              label="Max zoom"
              value={maxZoom}
              min={1}
              max={20}
              onChange={setMaxZoom}
            />
            <SliderRow
              label="Max occupancy"
              value={max}
              min={1}
              max={500}
              onChange={setMax}
            />
          </div>
        )}
      </div>
    </>
  );
}

function SliderRow({ label, value, min, max, onChange }) {
  return (
    <div style={rowStyle}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          fontSize: 12,
        }}
      >
        <span>{label}</span>
        <span>{value}</span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        style={{ width: "100%" }}
      />
    </div>
  );
}

const wrapperStyle = {
  position: "absolute",
  bottom: 20,
  left: 10,
  zIndex: 1000,
  display: "flex",
  flexDirection: "column",
  alignItems: "flex-start",
};

const buttonStyle = {
  width: 36,
  height: 36,
  borderRadius: "50%",
  border: "none",
  background: "#fff",
  boxShadow: "0 1px 4px rgba(0,0,0,0.4)",
  cursor: "pointer",
  fontSize: 18,
};

const panelStyle = {
  marginTop: 8,
  background: "#fff",
  borderRadius: 8,
  padding: 12,
  boxShadow: "0 1px 4px rgba(0,0,0,0.4)",
  width: 200,
};

const rowStyle = { marginBottom: 10 };
