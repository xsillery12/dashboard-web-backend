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
