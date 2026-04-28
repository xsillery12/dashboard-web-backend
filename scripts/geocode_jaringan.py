import pandas as pd
import psycopg2
import time
import re
import requests
from dotenv import load_dotenv
import os

load_dotenv()

df = pd.read_excel("Mapping_Data_Jaringan_BSI_Agen_2026.xlsx")

def geocode(kecamatan, kabupaten):
    headers = {"User-Agent": "BSI-Agen-Dashboard/1.0"}
    
    # Coba beberapa variasi query
    queries = [
        f"Kecamatan {kecamatan}, {kabupaten}, Indonesia",
        f"{kecamatan}, {kabupaten}, Indonesia",
        f"{kabupaten}, Indonesia",  # fallback ke kabupaten
    ]
    
    for query in queries:
        try:
            res = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": query, "format": "json", "limit": 1, "countrycodes": "id"},
                headers=headers,
                timeout=10
            )
            data = res.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
        except:
            pass
        time.sleep(1)
    
    return None, None

# Koneksi DB
db_url = os.getenv("DATABASE_URL")
match = re.match(r'postgresql://(.+):(.+)@(.+):(\d+)/(.+)', db_url)
user, password, host, port, dbname = match.groups()
conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
cur = conn.cursor()

# Buat tabel kalau belum ada
cur.execute("""
    CREATE TABLE IF NOT EXISTS cabang_koordinat (
        id           SERIAL PRIMARY KEY,
        kode_cabang  VARCHAR(50) UNIQUE,
        kecamatan    VARCHAR(255),
        kabupaten    VARCHAR(255),
        lat          FLOAT,
        lon          FLOAT
    )
""")
conn.commit()

# Cek yang sudah ada
cur.execute("SELECT kode_cabang FROM cabang_koordinat WHERE lat IS NOT NULL")
existing = {row[0] for row in cur.fetchall()}
print(f"Sudah ada koordinat: {len(existing)} cabang")

# Geocode yang belum ada
pending = df[~df["OUTLET CODE BSI"].astype(str).isin(existing)]
print(f"Perlu di-geocode: {len(pending)} cabang")

for i, (_, row) in enumerate(pending.iterrows()):
    kode      = str(row.get("OUTLET CODE BSI", "")).strip()
    kecamatan = str(row.get("KECAMATAN", "")).strip()
    kabupaten = str(row.get("KOTA/KAB", "")).strip()

    lat, lon = geocode(kecamatan, kabupaten)

    if lat and lon:
        cur.execute("""
            INSERT INTO cabang_koordinat (kode_cabang, kecamatan, kabupaten, lat, lon)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (kode_cabang) DO UPDATE 
            SET lat = EXCLUDED.lat, lon = EXCLUDED.lon
        """, (kode, kecamatan, kabupaten, lat, lon))
        conn.commit()
        print(f"[{i+1}/{len(pending)}] ✅ {kecamatan}, {kabupaten} → {lat:.4f}, {lon:.4f}")
    else:
        print(f"[{i+1}/{len(pending)}] ❌ {kecamatan}, {kabupaten} → tidak ditemukan")

cur.close()
conn.close()

# Hitung total
conn2 = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
cur2 = conn2.cursor()
cur2.execute("SELECT COUNT(*) FROM cabang_koordinat WHERE lat IS NOT NULL")
total = cur2.fetchone()[0]
cur2.close()
conn2.close()
print(f"✅ Total cabang dengan koordinat: {total}/{len(df)}")