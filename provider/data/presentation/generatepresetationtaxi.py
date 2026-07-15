from pathlib import Path
from datetime import datetime

# -----------------------
# Configuration
# -----------------------
INPUT_FOLDER = Path("provider/data/release/taxi_log_2008_by_id")
OUTPUT_FOLDER = Path("/home/vincent/Dokumente/studium/BigData/projekt/bd26_project_w4_b/provider/data/presentation/taxi-data")
NUM_FILES = 700

OUTPUT_FOLDER.mkdir(exist_ok=True)

# Current start time for all taxis
new_start = datetime.now().replace(microsecond=0)

# -----------------------
# Find the largest files
# -----------------------
files = []

for file in INPUT_FOLDER.glob("*.txt"):
    with file.open("r") as f:
        line_count = sum(1 for _ in f)
    files.append((line_count, file))

# Sort descending by number of lines
files.sort(reverse=True)

selected_files = [f for _, f in files[:NUM_FILES]]

print(f"Processing {len(selected_files)} files...")

# -----------------------
# Process each file
# -----------------------
for file in selected_files:
    with file.open("r") as f:
        lines = [line.strip() for line in f if line.strip()]

    if not lines:
        continue

    # Parse first timestamp
    first_parts = lines[0].split(",")
    taxi_id = first_parts[0]
    first_time = datetime.strptime(first_parts[1], "%Y-%m-%d %H:%M:%S")

    output_lines = []

    for line in lines:
        taxi_id, timestamp, lon, lat = line.split(",")

        old_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

        # Time relative to first point
        delta = old_time - first_time

        # New timestamp
        new_time = new_start + delta

        output_lines.append(
            f"{taxi_id},{new_time.strftime('%Y-%m-%d %H:%M:%S')},{lon},{lat}"
        )

    out_file = OUTPUT_FOLDER / f"{taxi_id}.txt"

    with out_file.open("w") as f:
        f.write("\n".join(output_lines))

print("Done.")