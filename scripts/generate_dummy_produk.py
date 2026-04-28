import psycopg2
import random
import re
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

# Daftar produk BSI Agen
PRODUK_LIST = [
    "OVO",
    "Transfer Sesama BSI",
    "Transfer Antar Bank",
    "Pembayaran PLN",
    "Pembayaran PDAM",
    "Pembayaran BPJS",
    "Pembayaran Telkom",
    "Pembayaran ZISWAF",
    "Pembayaran Lainnya",
    "Tarik Tunai"
]

# Bobot produk (simulasi distribusi pareto)
BOBOT = [30, 25, 18, 12, 6, 3, 2, 2, 1, 1]

db_url = os.getenv("DATABASE_URL")
match = re.match(r'postgresql://(.+):(.+)@(.+):(\d+)/(.+)', db_url)
user, password, host, port, dbname = match.groups()

conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
cur = conn.cursor()

# Ambil data agen + bulan yang ada (hanya Bulanan)
print("Mengambil data agen...")
cur.execute("""
    SELECT DISTINCT kode_agen, bulan, bulan_date
    FROM bsi_agen_monitoring
    WHERE tipe_bulan = 'Bulanan'
    AND jumlah_transaksi > 0
    AND bulan_date IS NOT NULL
    LIMIT 5000
""")
rows = cur.fetchall()
print(f"Total kombinasi agen-bulan: {len(rows)}")

# Truncate dulu
cur.execute("TRUNCATE TABLE agen_produk RESTART IDENTITY")
conn.commit()

# Generate dummy data
print("Generating dummy data...")
batch = []
for i, (kode_agen, bulan, bulan_date) in enumerate(rows):
    # Ambil total volume dari data asli
    cur.execute("""
        SELECT SUM(volume_transaksi), SUM(jumlah_transaksi)
        FROM bsi_agen_monitoring
        WHERE kode_agen = %s AND bulan = %s
    """, (kode_agen, bulan))
    result = cur.fetchone()
    total_volume  = result[0] or 0
    total_jumlah  = result[1] or 0

    if total_volume == 0:
        continue

    # Distribusikan ke produk berdasarkan bobot
    total_bobot = sum(BOBOT)
    for j, produk in enumerate(PRODUK_LIST):
        bobot = BOBOT[j]
        # Tambah random noise ±20%
        noise = random.uniform(0.8, 1.2)
        vol   = round(total_volume * (bobot / total_bobot) * noise)
        jml   = round(total_jumlah * (bobot / total_bobot) * noise)

        if vol > 0:
            batch.append((kode_agen, bulan, bulan_date, produk, vol, jml))

    # Insert per 1000
    if len(batch) >= 1000:
        cur.executemany("""
            INSERT INTO agen_produk (kode_agen, bulan, bulan_date, produk, volume_transaksi, jumlah_transaksi)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, batch)
        conn.commit()
        print(f"Inserted {i+1}/{len(rows)} agen-bulan...")
        batch = []

# Insert sisa
if batch:
    cur.executemany("""
        INSERT INTO agen_produk (kode_agen, bulan, bulan_date, produk, volume_transaksi, jumlah_transaksi)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, batch)
    conn.commit()

cur.close()
conn.close()
print("✅ Selesai!")