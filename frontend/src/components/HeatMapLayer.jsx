import { useEffect } from "react";
import { useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet.heat";
import { decodeCellId } from "../utils/heatmap_helper.js";
//TODO: heatmap only grows, need to remove cells that are no longer present in the latest update
export function HeatMapLayer({ cells }) {
  const map = useMap();

  useEffect(() => {
    const points = Object.values(cells).map((cell) => {
      const [lat, lon] = decodeCellId(cell.cellId);
      return [lat, lon, cell.taxiCount];
    });

    const heatLayer = L.heatLayer(points, {
      radius: 80,
      blur: 35,
      maxZoom: 5,
      // max: 40.0, //TODO: this is a magic number, should be configurable
      max: 100.0,
    }).addTo(map);

    return () => {
      map.removeLayer(heatLayer);
    };
  }, [map, cells]);

  return null;
}
