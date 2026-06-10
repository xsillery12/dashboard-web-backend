from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
import io
from io import StringIO

from database import get_db
from transform import transform_excel
from routers.auth import get_current_user

router = APIRouter(prefix="/upload", tags=["Upload"])


# Endpoint for Upload Data Monitoring Agen
@router.post("/")
async def upload_excel(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="File harus format .xlsx")

    try:
        contents = await file.read()
        file_like = io.BytesIO(contents)

        df = transform_excel(file_like)

        # Hapus data lama
        db.execute(text("TRUNCATE TABLE bsi_agen_monitoring RESTART IDENTITY"))
        db.commit()

        conn = db.get_bind().raw_connection()
        cursor = conn.cursor()

        # Drop kolom id, biarkan DB auto-increment
        df = df.drop(columns=["id"], errors="ignore")

        # Convert df ke CSV di memory
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # Sebutkan kolom secara eksplisit tanpa id
        columns = ", ".join(df.columns.tolist())
        cursor.copy_expert(
            f"COPY bsi_agen_monitoring ({columns}) FROM STDIN WITH CSV", buffer
        )
        conn.commit()
        cursor.close()
        conn.close()

        return {
            "status": "success",
            "message": "Data berhasil diupload",
            "total_rows": len(df),
            "total_agen": df["kode_agen"].nunique(),
            "total_periode": df["bulan"].nunique(),
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


#  Endpoint for Upload Data Transaksi Agen
@router.post("/transaksi")
async def upload_transaksi(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="File harus format .xlsx atau .xls")

    try:
        from transform_transaksi import transform_transaksi
        from io import BytesIO

        contents = await file.read()
        df = transform_transaksi(BytesIO(contents))

        if df.empty:
            raise HTTPException(
                status_code=400, detail="Data kosong atau format tidak sesuai"
            )

        db.execute(text("TRUNCATE TABLE transaksi_agen RESTART IDENTITY"))
        db.commit()

        conn = db.get_bind().raw_connection()
        cursor = conn.cursor()

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        columns = ", ".join(df.columns.tolist())
        cursor.copy_expert(
            f"COPY transaksi_agen ({columns}) FROM STDIN WITH CSV", buffer
        )

        conn.commit()
        cursor.close()
        columns = list(df.columns)

        return {
            "message": "Data transaksi berhasil diupload",
            "total_rows": len(df),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint for Upload Data Heartbeat EDC
@router.post("/heartbeat")
async def upload_heartbeat(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="File harus format .xlsx atau .xls")

    try:
        from transform_heartbeat import transform_heartbeat
        from io import BytesIO

        contents = await file.read()
        df = transform_heartbeat(BytesIO(contents))

        if df.empty:
            raise HTTPException(
                status_code=400, detail="Data kosong atau format tidak sesuai"
            )

        # Delete old data
        db.execute(text("TRUNCATE TABLE heartbeat_edc RESTART IDENTITY"))
        db.commit()

        conn = db.get_bind().raw_connection()
        cursor = conn.cursor()

        # Convert df to CSV in memory
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # Copy to table
        columns = ", ".join(df.columns.tolist())
        cursor.copy_expert(
            f"COPY heartbeat_edc ({columns}) FROM STDIN WITH CSV", buffer
        )
        conn.commit()
        cursor.close()
        conn.close()

        return {
            "message": "Data Heartbeat berhasil diupload",
            "total_rows": len(df),
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
