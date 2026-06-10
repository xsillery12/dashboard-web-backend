import pandas as pd


def transform_transaksi(file) -> pd.DataFrame:
    """Transform Excel transaksi BSI Agen format baru"""
    df = pd.read_excel(file)
    df.columns = [str(c).strip() for c in df.columns]

    # Mapping kolom Excel ke nama database
    col_map = {
        "No": "nomor",
        "Produk": "produk",
        "Produk Detail": "produk_detail",
        "Agent Phone": "agent_phone",
        "Agent Name": "agent_name",
        "Agent Code": "agent_code",
        "Agent Branch": "agent_branch",
        "Reference": "reference",
        "Type Transaction": "type_transaction",
        "Source Account": "source_account",
        "Destination Account": "destination_account",
        "Amount": "amount",
        "Transaction Date": "transaction_date_raw",
        "Device Type": "device_type",
    }

    df = df.rename(columns=col_map)
    keep_cols = list(col_map.values())
    df = df[[c for c in keep_cols if c in df.columns]]

    df = df.dropna(subset=["agent_code"])

    # Fix string columns
    str_cols = [
        "nomor",
        "produk",
        "produk_detail",
        "agent_phone",
        "agent_name",
        "agent_code",
        "agent_branch",
        "reference",
        "type_transaction",
        "source_account",
        "destination_account",
        "device_type",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"nan": None, "NaN": None, "": None})

    # Amount → numeric
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    # Transaction Date → split jadi date dan time
    dt = pd.to_datetime(df["transaction_date_raw"], dayfirst=True, errors="coerce")
    df["transaction_date"] = dt.dt.date
    df["transaction_time"] = dt.dt.time
    df = df.drop(columns=["transaction_date_raw"])

    # Urutan kolom sesuai tabel DB (tanpa id, created_at)
    df = df[
        [
            "nomor",
            "produk",
            "produk_detail",
            "agent_phone",
            "agent_name",
            "agent_code",
            "agent_branch",
            "reference",
            "type_transaction",
            "source_account",
            "destination_account",
            "amount",
            "transaction_date",
            "transaction_time",
            "device_type",
        ]
    ].reset_index(drop=True)

    return df
