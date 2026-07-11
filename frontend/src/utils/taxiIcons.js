import L from "leaflet";

// create icons per taxi
function createDotIcon({
  color,
  size = 14,
  variant = "default",
  ring = false,
  ringColor,
}) {
  return L.divIcon({
    className: `taxi-dot-icon dot-${variant}`,
    html: `<div class="taxi-dot" style="width:${size}px;height:${size}px;background:${color};${ring ? `box-shadow:0 0 0 3px ${ringColor}, 0 1px 3px rgba(0,0,0,0.4);` : ""}"></div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    popupAnchor: [0, -size / 2 - 6],
  });
}

// Status colours. Out-of-area is orange (per the professor's suggestion),
// distinct from the red used for speeding.
const COLORS = {
  default: "#378ADD",
  speeding: "#E2462F",
  area: "#F59E0B", // orange (was green #1F9D55)
  parking: "#9CA3AF",
};

// Selection ring colour — drawn around whatever status colour the taxi has,
// so a selected taxi keeps its speeding/area/parking colour instead of turning
// blue.
const SELECTION_RING = "rgba(29,78,216,0.55)";

export const defaultIcon = createDotIcon({
  color: COLORS.default,
  variant: "default",
});
export const speedingIcon = createDotIcon({
  color: COLORS.speeding,
  size: 16,
  variant: "speeding",
});
export const ooaIcon = createDotIcon({
  color: COLORS.area,
  size: 16,
  variant: "area",
});
export const parkingIcon = createDotIcon({
  color: COLORS.parking,
  variant: "parking",
});

// Selected variants: same status colour, but larger with a selection ring.
// Keeps the taxi's status recognisable while marking it as focused.
const selectedIcons = {
  default: createDotIcon({ color: COLORS.default, size: 20, variant: "selected default", ring: true, ringColor: SELECTION_RING }),
  speeding: createDotIcon({ color: COLORS.speeding, size: 20, variant: "selected speeding", ring: true, ringColor: SELECTION_RING }),
  area: createDotIcon({ color: COLORS.area, size: 20, variant: "selected area", ring: true, ringColor: SELECTION_RING }),
  parking: createDotIcon({ color: COLORS.parking, size: 20, variant: "selected parking", ring: true, ringColor: SELECTION_RING }),
};

export const TAG_STYLES = {
  speeding: { bg: "#FAECE7", color: "#993C1D", dot: "#D85A30" },
  area: { bg: "#FEF3E2", color: "#B45309", dot: "#F59E0B" }, // orange to match OOA marker
};

// Picks which pre-built dot icon a taxi marker should use. A selected taxi keeps
// its status colour (speeding/area/parking/default) and gains a selection ring,
// rather than being recoloured blue.
export function pickTaxiIcon({ isSelected, isSpeeding, isOutOfArea, isParking }) {
  let status = "default";
  if (isSpeeding) status = "speeding";
  else if (isOutOfArea) status = "area";
  else if (isParking) status = "parking";

  if (isSelected) return selectedIcons[status];

  if (status === "speeding") return speedingIcon;
  if (status === "area") return ooaIcon;
  if (status === "parking") return parkingIcon;
  return defaultIcon;
}

// Builds the cluster bubble icon: color reflects the worst violation among
// its child markers, size reflects how many markers are clustered.
export function createClusterIcon(cluster) {
  const childMarkers = cluster.getAllChildMarkers();
  const count = childMarkers.length;

  let hasSpeeding = false;
  let hasArea = false;
  childMarkers.forEach((m) => {
    const cls = m.options.icon?.options?.className || "";
    if (cls.includes("dot-speeding")) hasSpeeding = true;
    else if (cls.includes("dot-area")) hasArea = true;
  });

  let variant = "normal";
  if (hasSpeeding) variant = "speeding";
  else if (hasArea) variant = "area";

  let size, sizeTier;
  if (count < 10) {
    size = 36;
    sizeTier = "sm";
  } else if (count < 50) {
    size = 44;
    sizeTier = "md";
  } else {
    size = 52;
    sizeTier = "lg";
  }

  return L.divIcon({
    html: `<div class="cluster-inner">${count}</div>`,
    className: `taxi-cluster cluster-${variant} cluster-${sizeTier}`,
    iconSize: L.point(size, size),
  });
}
