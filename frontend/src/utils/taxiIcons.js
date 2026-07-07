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

export const defaultIcon = createDotIcon({
  color: "#378ADD",
  variant: "default",
});
export const speedingIcon = createDotIcon({
  color: "#E2462F",
  size: 16,
  variant: "speeding",
});
export const ooaIcon = createDotIcon({
  color: "#1F9D55",
  size: 16,
  variant: "area",
});
export const parkingIcon = createDotIcon({
  color: "#9CA3AF",
  variant: "parking",
});
export const selectedIcon = createDotIcon({
  color: "#1D4ED8",
  size: 20,
  variant: "selected",
  ring: true,
  ringColor: "rgba(29,78,216,0.35)",
});

export const TAG_STYLES = {
  speeding: { bg: "#FAECE7", color: "#993C1D", dot: "#D85A30" },
  area: { bg: "#E6F6ED", color: "#1F7A43", dot: "#1F9D55" },
  taxiUpdate: { bg: "#E6F1FB", color: "#185FA5", dot: "#378ADD" },
};

// Picks which pre-built dot icon a taxi marker should use, in priority order.
export function pickTaxiIcon({
  isSelected,
  isSpeeding,
  isOutOfArea,
  isParking,
}) {
  if (isSelected) return selectedIcon;
  if (isSpeeding) return speedingIcon;
  if (isOutOfArea) return ooaIcon;
  if (isParking) return parkingIcon;
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
