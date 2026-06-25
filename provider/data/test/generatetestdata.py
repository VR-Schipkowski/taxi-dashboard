import math
from datetime import datetime, timedelta


EARTH_RADIUS = 6371000  # Meter

CENTER_LAT = 39.9163
CENTER_LON = 116.3972

RADIUS_M = 5000
SEND_INTERVAL = 5  # Sekunden

folder = "provider/data/test/taxi-data"


def destination_point(lat, lon, distance_m, bearing_deg):
    """
    Berechnet neuen Punkt aus Startpunkt, Distanz und Richtung.
    """
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)
    bearing = math.radians(bearing_deg)

    lat2 = math.asin(
        math.sin(lat1) * math.cos(distance_m / EARTH_RADIUS)
        + math.cos(lat1) * math.sin(distance_m / EARTH_RADIUS) * math.cos(bearing)
    )

    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(distance_m / EARTH_RADIUS) * math.cos(lat1),
        math.cos(distance_m / EARTH_RADIUS) - math.sin(lat1) * math.sin(lat2)
    )

    return math.degrees(lat2), math.degrees(lon2)


def generate_circle_taxi(
        taxi_id,
        speed_kmh,
        duration_minutes,
        center_lat=CENTER_LAT,
        center_lon=CENTER_LON,
        radius_m=RADIUS_M,
        start_time=datetime(2008, 2, 2, 12, 0, 0)
):
    """
    Taxi fährt auf einem Kreis.
    Startposition = 12 Uhr.
    """

    circumference = 2 * math.pi * radius_m
    speed_ms = speed_kmh / 3.6

    current_time = start_time
    elapsed = 0

    records = []

    while elapsed <= duration_minutes * 60:

        travelled = speed_ms * elapsed

        #angle on circle
        angle_deg = (travelled / circumference) * 360

        
        bearing = angle_deg

        lat, lon = destination_point(
            center_lat,
            center_lon,
            radius_m,
            bearing
        )

        records.append(
            {
                "taxi_id": taxi_id,
                "timestamp": current_time,
                "lon": lon,
                "lat": lat,
            }
        )

        current_time += timedelta(seconds=SEND_INTERVAL)
        elapsed += SEND_INTERVAL

    return records

def write_taxi_file(filename, records):
    with open(folder + "/" + filename, "w") as f:
        for record in records:
            f.write(
                f"{record['taxi_id']},{record['timestamp']:%Y-%m-%d %H:%M:%S},{record['lon']:.5f},{record['lat']:.5f}\n"
            )




parked = generate_circle_taxi(
    taxi_id=0,
    speed_kmh=0,
    duration_minutes=30
)

normal = generate_circle_taxi(
    taxi_id=45,
    speed_kmh=45,
    duration_minutes=30
)

speeding = generate_circle_taxi(
    taxi_id=65,
    speed_kmh=65,
    duration_minutes=30
)

speedingfaster = generate_circle_taxi(
    taxi_id=120,
    speed_kmh=120,
    duration_minutes=30)
speedingfastest = generate_circle_taxi(
    taxi_id=200,
    speed_kmh=200,
    duration_minutes=30)

norma_out_of_area = generate_circle_taxi(
    taxi_id=16000,
    speed_kmh=45,
    duration_minutes=30,
    center_lat=CENTER_LAT,  
    center_lon=CENTER_LON,
    radius_m=16000
)


write_taxi_file("taxi_parked.txt", parked)
write_taxi_file("taxi_45.txt", normal)
write_taxi_file("taxi_65.txt", speeding)    
write_taxi_file("taxi_120.txt", speedingfaster)    
write_taxi_file("taxi_200.txt", speedingfastest)
write_taxi_file("taxi_45_out_of_area.txt", norma_out_of_area)



#lost signal:
taxi = generate_circle_taxi(
    taxi_id=2020,
    speed_kmh=45,
    duration_minutes=30,
    center_lat=CENTER_LAT,  
    center_lon=CENTER_LON,
    radius_m=7000
)
stop_and_go = []
start_time = taxi[0]["timestamp"]
sending = 20
not_sending = 20
for i in taxi: 
    if (i["timestamp"] - start_time).total_seconds() % (sending + not_sending) < sending:
        stop_and_go.append(i)


write_taxi_file("taxi_45_stop_and_go.txt", stop_and_go)

#stop sending
stop_sending = generate_circle_taxi(
    taxi_id=3030,
    speed_kmh=20,
    duration_minutes=0.5,
    center_lat=CENTER_LAT,      
    center_lon=CENTER_LON,
    radius_m=7000)

    
write_taxi_file("taxi_20_stop_sending.txt", stop_sending)
