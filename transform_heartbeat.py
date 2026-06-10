import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


def transform_heartbeat(file) -> pd.DataFrame:
    # Header detail di baris ke-4
    df = pd.read_excel(file, header=3)
    df.columns = [str(c).strip() for c in df.columns]

    # ── Rename kolom B (index 1) dan C (index 2) ──
    new_cols = list(df.columns)
    new_cols[1] = "Location"
    new_cols[2] = "Client Group"
    df.columns = new_cols

    print("✅ Kolom setelah rename:", list(df.columns))

    # Mapping kolom Excel ke nama database
    col_map = {
        "MID (Kode Agen)": "kode_agen",
        "Location": "location",
        "Client Group": "client_group",
        "Kode Cabang": "kode_cabang",
        "Alamat": "alamat",
        "Kabupaten/Kota": "kabupaten_kota",
        "Wilayah": "wilayah",
        "Contact Person Agen": "contact_person",
        "Telp Agen": "telp_agen",
        "PIC Cabang": "pic_cabang",
        "Phone PIC Cabang": "phone_pic",
        "Serial Number": "serial_number",
        "IMEI": "imei",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
        "UPDATE TIME": "update_time",
        "LAST HEARTBEAT": "last_heartbeat",
        "SYNCED AT": "synced_at",
    }

    df = df.rename(columns=col_map)

    keep_cols = list(col_map.values())
    df = df[[c for c in keep_cols if c in df.columns]]

    df = df.dropna(subset=["kode_agen"])

    # Fix string columns
    str_cols = [
        "kode_agen",
        "location",
        "client_group",
        "kode_cabang",
        "alamat",
        "kabupaten_kota",
        "wilayah",
        "contact_person",
        "telp_agen",
        "pic_cabang",
        "phone_pic",
        "serial_number",
        "imei",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"nan": None, "NaN": None, "": None})

    # Fix tanggal
    date_cols = ["update_time", "last_heartbeat", "synced_at"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce").dt.date

    # Fix latitude longitude
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # JOIN dengan cabang_region untuk dapat region, area, nama_outlet
    with engine.connect() as conn:
        mapping_df = pd.read_sql(
            text("SELECT kode_cabang, region, area, nama_outlet FROM cabang_region"),
            conn,
        )

    df = df.merge(mapping_df, on="kode_cabang", how="left")

    # Hitung selisih hari
    today = datetime.today().date()
    df["selisih_hari"] = df["last_heartbeat"].apply(
        lambda d: (today - d).days if pd.notna(d) else None
    )
    df["selisih_hari"] = df["selisih_hari"].astype("Int64")

    def kategorisasi(hari):
        if pd.isna(hari):
            return "Tidak diketahui"
        if hari < 30:
            return "Aktif"
        elif hari < 90:
            return "1-3 bulan"
        elif hari < 365:
            return "3-12 bulan"
        else:
            return "> 1 tahun"

    df["kategori"] = df["selisih_hari"].apply(kategorisasi)

    df = df.reset_index(drop=True)
    return df
