import { useEffect, useRef } from "react";
import { useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet.heat";
import { decodeCellId } from "../utils/heatmap_helper.js";

export function HeatMapLayer({
  cells,
  radius = 70,
  blur = 45,
  maxZoom = 5,
  max = 600,
  opacity = 0.1,
}) {
  const map = useMap();
  const layerRef = useRef(null);

  // create the layer once
  useEffect(() => {
    const heatLayer = L.heatLayer([], {
      radius,
      blur,
      maxZoom,
      max,
      opacity,
    }).addTo(map);

    layerRef.current = heatLayer;

    return () => {
      map.removeLayer(heatLayer);
      layerRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [map]);

  // update points in place (no teardown -> no canvas artifacts)
  useEffect(() => {
    if (!layerRef.current) return;
    const points = Object.values(cells).map((cell) => {
      const [lat, lon] = decodeCellId(cell.cellId);
      return [lat, lon, cell.taxiCount];
    });
    layerRef.current.setLatLngs(points);
  }, [cells]);

  // update options in place
  useEffect(() => {
    if (!layerRef.current) return;
    layerRef.current.setOptions({ radius, blur, maxZoom, max, opacity });
    layerRef.current.redraw();
  }, [radius, blur, maxZoom, max, opacity]);

  return null;
}
