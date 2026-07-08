import { useEffect, useState } from "react";

// Taxi search box to display taxi path
export function TaxiSearchBox({ onSelect, onClear, selectedTaxiId }) {
  const [value, setValue] = useState("");

  function handleSubmit(e) {
    e.preventDefault();
    const trimmed = value.trim();
    if (trimmed === "") return;
    onSelect(trimmed);
  }

  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === "Escape" && selectedTaxiId !== null) {
        setValue("");
        onClear();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [selectedTaxiId, setValue, onClear]);

  return (
    <form
      onSubmit={handleSubmit}
      style={{ display: "flex", gap: 6, alignItems: "center" }}
    >
      <input
        type="text"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        placeholder="select taxi"
        style={{
          padding: "4px 8px",
          fontSize: 13,
          border: "1px solid #d1d5db",
          borderRadius: 4,
          width: 120,
        }}
      />
      <button
        type="submit"
        style={{
          padding: "4px 10px",
          fontSize: 13,
          border: "1px solid #d1d5db",
          borderRadius: 4,
          background: "#fff",
          cursor: "pointer",
        }}
      >
        Search
      </button>
      {selectedTaxiId !== null && (
        <button
          type="button"
          onClick={() => {
            setValue("");
            onClear();
          }}
          style={{
            padding: "4px 10px",
            fontSize: 13,
            border: "1px solid #d1d5db",
            borderRadius: 4,
            background: "#fff",
            cursor: "pointer",
            color: "#555",
          }}
        >
          deselect
        </button>
      )}
    </form>
  );
}
