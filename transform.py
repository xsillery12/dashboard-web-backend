import pandas as pd
from datetime import datetime


def load_jaringan_mapping(jaringan_path: str) -> dict:
    """Load mapping kode_cabang -> (kelurahan, kecamatan, kab_kota) dari file jaringan BSI"""
    df_jaringan = pd.read_excel(jaringan_path)
    mapping = {}
    for _, row in df_jaringan.iterrows():
        kode = str(row.get("OUTLET CODE BSI", "")).strip()
        if kode:
            mapping[kode] = {
                "kelurahan": str(row.get("KELURAHAN", "")).strip(),
                "kecamatan": str(row.get("KECAMATAN", "")).strip(),
                "kab_kota": str(row.get("KOTA/KAB", "")).strip(),
            }
    return mapping


def transform_excel(
    file, jaringan_path: str = "Mapping_Data_Jaringan_BSI_Agen_2026.xlsx"
) -> pd.DataFrame:
    df_raw = pd.read_excel(file, header=None)

    header_main = df_raw.iloc[3].tolist()
    header_sub = df_raw.iloc[4].tolist()

    col_names = []
    current_group = None
    for main, sub in zip(header_main, header_sub):
        if pd.notna(main) and main not in [None, ""]:
            current_group = str(main).strip()
        if pd.notna(sub) and sub not in [None, ""]:
            if isinstance(sub, datetime):
                label = sub.strftime("%b-%y")
            else:
                label = str(sub).strip()
            col_names.append(f"{current_group}__{label}")
        else:
            col_names.append(
                current_group if current_group else f"col_{len(col_names)}"
            )

    df = df_raw.iloc[5:].copy()
    df.columns = col_names
    df = df.reset_index(drop=True)

    identity_map = {
        "TGL DAFTAR": "tgl_daftar",
        "KODE AGEN": "kode_agen",
        "NAMA AGEN": "nama_agen",
        "NAMA TOKO": "nama_toko",
        "KODE CABANG": "kode_cabang",
        "NAMA CABANG": "nama_cabang",
        "AREA": "area",
        "REGION": "region",
        "DEVICE": "device",
    }

    id_cols_available = {}
    for col in df.columns:
        for key, new_name in identity_map.items():
            if col == key:
                id_cols_available[col] = new_name

    df_id = df[list(id_cols_available.keys())].rename(columns=id_cols_available)

    jumlah_cols = {
        c: c.replace("Jumlah Transaksi__", "")
        for c in df.columns
        if c.startswith("Jumlah Transaksi__")
    }
    volume_cols = {
        c: c.replace("Volume Transaksi__", "")
        for c in df.columns
        if c.startswith("Volume Transaksi__")
    }
    fee_bank_cols = {
        c: c.replace("Fee Bank__", "") for c in df.columns if c.startswith("Fee Bank__")
    }
    fee_agen_cols = {
        c: c.replace("Fee Agen__", "") for c in df.columns if c.startswith("Fee Agen__")
    }

    df_jumlah = pd.concat(
        [df_id, df[list(jumlah_cols.keys())].rename(columns=jumlah_cols)], axis=1
    )
    df_jumlah = df_jumlah.melt(
        id_vars=list(identity_map.values()),
        var_name="bulan",
        value_name="jumlah_transaksi",
    )

    df_volume = pd.concat(
        [df_id, df[list(volume_cols.keys())].rename(columns=volume_cols)], axis=1
    )
    df_volume = df_volume.melt(
        id_vars=list(identity_map.values()),
        var_name="bulan",
        value_name="volume_transaksi",
    )

    df_fee_bank = pd.concat(
        [df_id, df[list(fee_bank_cols.keys())].rename(columns=fee_bank_cols)], axis=1
    )
    df_fee_bank = df_fee_bank.melt(
        id_vars=list(identity_map.values()), var_name="bulan", value_name="fee_bank"
    )

    df_fee_agen = pd.concat(
        [df_id, df[list(fee_agen_cols.keys())].rename(columns=fee_agen_cols)], axis=1
    )
    df_fee_agen = df_fee_agen.melt(
        id_vars=list(identity_map.values()), var_name="bulan", value_name="fee_agen"
    )

    df_long = (
        df_jumlah.merge(
            df_volume[list(identity_map.values()) + ["bulan", "volume_transaksi"]],
            on=list(identity_map.values()) + ["bulan"],
            how="left",
        )
        .merge(
            df_fee_bank[list(identity_map.values()) + ["bulan", "fee_bank"]],
            on=list(identity_map.values()) + ["bulan"],
            how="left",
        )
        .merge(
            df_fee_agen[list(identity_map.values()) + ["bulan", "fee_agen"]],
            on=list(identity_map.values()) + ["bulan"],
            how="left",
        )
    )

    df_long = df_long[
        df_long["jumlah_transaksi"].notna() | df_long["volume_transaksi"].notna()
    ]

    # Tipe bulan
    df_long["tipe_bulan"] = df_long["bulan"].apply(
        lambda x: "Total Tahunan" if str(x).replace(".0", "").isdigit() else "Bulanan"
    )

    # Konversi bulan ke date
    df_long["bulan_date"] = pd.to_datetime(
        df_long["bulan"].where(df_long["tipe_bulan"] == "Bulanan"),
        format="%b-%y",
        errors="coerce",
    )

    # ── Mapping kelurahan, kecamatan, kab_kota dari data jaringan ─────────────
    jaringan_map = load_jaringan_mapping(jaringan_path)
    df_long["kelurahan"] = df_long["kode_cabang"].map(
        lambda x: jaringan_map.get(str(x).strip(), {}).get("kelurahan", "")
    )
    df_long["kecamatan"] = df_long["kode_cabang"].map(
        lambda x: jaringan_map.get(str(x).strip(), {}).get("kecamatan", "")
    )
    df_long["kab_kota"] = df_long["kode_cabang"].map(
        lambda x: jaringan_map.get(str(x).strip(), {}).get("kab_kota", "")
    )

    # Fix tipe data
    str_cols = [
        "kode_agen",
        "nama_agen",
        "nama_toko",
        "kode_cabang",
        "nama_cabang",
        "area",
        "region",
        "device",
        "bulan",
        "tipe_bulan",
        "kelurahan",
        "kecamatan",
        "kab_kota",
    ]
    for col in str_cols:
        df_long[col] = df_long[col].astype(str)

    df_long["jumlah_transaksi"] = pd.to_numeric(
        df_long["jumlah_transaksi"], errors="coerce"
    ).fillna(0)
    df_long["volume_transaksi"] = pd.to_numeric(
        df_long["volume_transaksi"], errors="coerce"
    ).fillna(0)
    df_long["fee_bank"] = pd.to_numeric(df_long["fee_bank"], errors="coerce").fillna(0)
    df_long["fee_agen"] = pd.to_numeric(df_long["fee_agen"], errors="coerce").fillna(0)

    # Urutan kolom sesuai PostgreSQL
    df_long = df_long[
        [
            "kode_agen",
            "nama_agen",
            "nama_toko",
            "kode_cabang",
            "nama_cabang",
            "area",
            "region",
            "device",
            "tgl_daftar",
            "bulan",
            "tipe_bulan",
            "jumlah_transaksi",
            "volume_transaksi",
            "bulan_date",
            "kelurahan",
            "kecamatan",
            "kab_kota",
            "fee_bank",
            "fee_agen",
        ]
    ].reset_index(drop=True)

    return df_long
