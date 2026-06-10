import pandas as pd
import psycopg2
import re
from io import StringIO
from transform import transform_excel
from dotenv import load_dotenv
import os

load_dotenv()

print("Transforming Excel...")
df = transform_excel(
    r"D:\website_bsi_agen\backend\data1.xlsx",
    r"D:\website_bsi_agen\backend\data2.xlsx",
)
print(f"Total rows: {len(df)}")

db_url = os.getenv("DATABASE_URL")
match = re.match(r"postgresql://(.+):(.+)@(.+):(\d+)/(.+)", db_url)
user, password, host, port, dbname = match.groups()

conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password,
    sslmode="require",  # ← tambah ini
    keepalives=1,  # ← tambah ini
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=5,
)
cur = conn.cursor()

print("Truncating table...")
cur.execute("TRUNCATE TABLE bsi_agen_monitoring RESTART IDENTITY")
conn.commit()

# Upload per batch 10.000 rows
BATCH_SIZE = 10000
total = len(df)

for i in range(0, total, BATCH_SIZE):
    batch = df.iloc[i : i + BATCH_SIZE]
    output = StringIO()
    batch.to_csv(output, sep="\t", header=False, index=False, na_rep="\\N")
    output.seek(0)
    cur.copy_from(output, "bsi_agen_monitoring", null="\\N", columns=list(df.columns))
    conn.commit()
    print(f"Uploaded {min(i+BATCH_SIZE, total)}/{total} rows...")

cur.close()
conn.close()
print("✅ Upload ke Railway selesai!")
