"""
Script standalone untuk upload data mapping cabang dari Excel data jaringan
ke tabel cabang_region.

Cara pakai:
    python upload_cabang_region.py path/to/data_jaringan.xlsx
"""

import pandas as pd
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


def upload_cabang_region(file_path: str):
    print(f"Membaca file: {file_path}")
    df = pd.read_excel(file_path)
    df.columns = [str(c).strip() for c in df.columns]

    required = [
        "ID KANTOR",
        "OUTLET CODE BSI",
        "NAMA OUTLET",
        "REGION",
        "AREA",
        "KELURAHAN",
        "KECAMATAN",
        "KOTA/KAB",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"❌ Kolom tidak ditemukan: {missing}")
        print(f"Kolom yang ada: {list(df.columns)}")
        return

    df = df[required].copy()
    df.columns = [
        "kode_cabang",
        "outlet_code_bsi",
        "nama_outlet",
        "region",
        "area",
        "kelurahan",
        "kecamatan",
        "kab_kota",
    ]

    # Clean data
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": None, "NaN": None, "": None})

    # Wajib ada kode_cabang
    df = df.dropna(subset=["kode_cabang"])
    df = df.drop_duplicates(subset=["kode_cabang"], keep="first")

    print(f"Total mapping siap upload: {len(df)} baris")

    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE cabang_region RESTART IDENTITY"))
        conn.commit()
        df.to_sql(
            "cabang_region", conn, if_exists="append", index=False, method="multi"
        )
        conn.commit()

    print(f"✅ Berhasil upload {len(df)} baris mapping cabang_region")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python upload_cabang_region.py path/to/data_jaringan.xlsx")
        sys.exit(1)
    upload_cabang_region(sys.argv[1])
