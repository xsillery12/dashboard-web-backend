import json
import psycopg2
from shapely.geometry import shape
from dotenv import load_dotenv
import os
import re

load_dotenv()

# Load GeoJSON level 2 (Kabupaten/Kota)
print("Loading GeoJSON...")
with open("gadm41_IDN_3.json", "r", encoding="utf-8") as f:
    geojson = json.load(f)

print(f"Total features: {len(geojson['features'])}")

# Ekstrak centroid
print("Extracting centroids...")
data = []
for feature in geojson['features']:
    props = feature['properties']
    geom = shape(feature['geometry'])
    centroid = geom.centroid

    data.append({
    'nama_provinsi':  props.get('NAME_1', ''),
    'nama_kabupaten': props.get('NAME_2', '').upper(),
    'nama_kecamatan': props.get('NAME_3', '').upper(),
    'lat':            centroid.y,
    'lon':            centroid.x
})

print(f"Total kabupaten: {len(data)}")
print("Sample:", data[0])

# Simpan ke PostgreSQL
print("Saving to database...")
db_url = os.getenv("DATABASE_URL")

match = re.match(r'postgresql://(.+):(.+)@(.+):(\d+)/(.+)', db_url)
user, password, host, port, dbname = match.groups()

conn = psycopg2.connect(
    host=host, port=port,
    dbname=dbname, user=user, password=password
)
cur = conn.cursor()

# Truncate dulu
cur.execute("TRUNCATE TABLE kecamatan_koordinat RESTART IDENTITY")

# Insert batch
batch_size = 100
for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    cur.executemany("""
        INSERT INTO kecamatan_koordinat 
        (nama_provinsi, nama_kabupaten, nama_kecamatan, lat, lon)
        VALUES (%(nama_provinsi)s, %(nama_kabupaten)s, %(nama_kecamatan)s, %(lat)s, %(lon)s)
    """, batch)
    conn.commit()
    print(f"Inserted {min(i+batch_size, len(data))}/{len(data)}")

cur.close()
conn.close()
print("✅ Selesai!")