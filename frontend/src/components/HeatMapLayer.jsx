import { useEffect } from "react";
import { useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet.heat";
import { decodeCellId } from "../utils/heatmap_helper.js";

export function HeatMapLayer({
  cells,
  radius = 90,
  blur = 70,
  maxZoom = 15,
  max = 50,
  opacity = 0.2,
}) {
  const map = useMap();

  useEffect(() => {
    const points = Object.values(cells).map((cell) => {
      const [lat, lon] = decodeCellId(cell.cellId);
      return [lat, lon, cell.taxiCount];
    });

    const heatLayer = L.heatLayer(points, {
      radius,
      blur,
      maxZoom,
      max,
      opacity,
    }).addTo(map);

    return () => {
      map.removeLayer(heatLayer);
    };
  }, [map, cells, radius, blur, maxZoom, max, opacity]);

  return null;
}
