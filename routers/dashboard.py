from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from database import get_db
from routers.auth import get_current_user

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


def build_filter(month_from=None, month_to=None, region=None, area=None, cabang=None):
    conditions = ["tipe_bulan = 'Bulanan'"]
    params = {}

    if month_from:
        conditions.append("bulan_date >= :month_from")
    try:
        from datetime import datetime

        dt = datetime.strptime(month_from, "%b-%y")
        params["month_from"] = dt.strftime("%Y-%m-01")
    except:
        params["month_from"] = f"{month_from}-01"
    if month_to:
        conditions.append("bulan_date <= :month_to")
    try:
        from datetime import datetime

        dt = datetime.strptime(month_to, "%b-%y")
        params["month_to"] = dt.strftime("%Y-%m-01")
    except:
        params["month_to"] = f"{month_to}-01"
    if region:
        conditions.append("region = :region")
        params["region"] = region
    if area:
        conditions.append("area = :area")
        params["area"] = area
    if cabang:
        conditions.append("nama_cabang = :cabang")
        params["cabang"] = cabang

    where = "WHERE " + " AND ".join(conditions)
    return where, params


# ── KPI Cards ─────────────────────────────────────────────────────────────────
@router.get("/stats")
def get_stats(
    month_from: Optional[str] = Query(None, description="Format: 2026-01"),
    month_to: Optional[str] = Query(None, description="Format: 2026-03"),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, region, area, cabang)
    query = f"""
        SELECT
            COUNT(DISTINCT kode_agen) as total_agen,
            SUM(volume_transaksi)     as total_volume,
            SUM(jumlah_transaksi)     as total_jumlah
        FROM bsi_agen_monitoring
        {where}
    """
    result = db.execute(text(query), params).fetchone()
    return {
        "total_agen": result.total_agen,
        "total_volume": result.total_volume,
        "total_jumlah": result.total_jumlah,
    }


# ── KPI Agen Aktif ───────────────────────────────────────────────────────────
@router.get("/stats/agen-aktif")
def get_agen_aktif(
    month_from: Optional[str] = Query(None),
    month_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, region, area, cabang)
    query = f"""
        SELECT COUNT(DISTINCT kode_agen) as agen_aktif
        FROM bsi_agen_monitoring
        {where}
        AND jumlah_transaksi > 0
    """
    result = db.execute(text(query), params).fetchone()
    return {"agen_aktif": result.agen_aktif}


# ── Filters ───────────────────────────────────────────────────────────
@router.get("/filters")
def get_filters(
    region: str = None,
    area: str = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    try:
        # Base WHERE
        where = "WHERE tipe_bulan='Bulanan'"
        if region:
            where += f" AND region = :region"
        if area:
            where += f" AND area = :area"

        params = {}
        if region:
            params["region"] = region
        if area:
            params["area"] = area

        regions = db.execute(
            text(
                f"SELECT DISTINCT region FROM bsi_agen_monitoring WHERE tipe_bulan='Bulanan' ORDER BY region"
            )
        ).fetchall()

        areas = db.execute(
            text(
                f"SELECT DISTINCT area FROM bsi_agen_monitoring {where} ORDER BY area"
            ),
            params,
        ).fetchall()

        cabangs = db.execute(
            text(
                f"SELECT DISTINCT nama_cabang FROM bsi_agen_monitoring {where} ORDER BY nama_cabang"
            ),
            params,
        ).fetchall()

        bulans = db.execute(
            text(
                "SELECT DISTINCT bulan, bulan_date FROM bsi_agen_monitoring WHERE tipe_bulan='Bulanan' ORDER BY bulan_date"
            )
        ).fetchall()

        return {
            "regions": [r.region for r in regions],
            "areas": [a.area for a in areas],
            "cabangs": [c.nama_cabang for c in cabangs],
            "bulans": [
                {"label": b.bulan, "value": str(b.bulan_date)[:7]} for b in bulans
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Chart Device ──────────────────────────────────────────────────────────────
@router.get("/chart/device")
def get_chart_device(
    month_from: Optional[str] = Query(None),
    month_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, region, area, cabang)
    query = f"""
        SELECT
            device,
            SUM(fee_bank) as fee_bank,
            SUM(fee_agen) as fee_agen
        FROM bsi_agen_monitoring
        {where}
        GROUP BY device
        ORDER BY fee_bank DESC
    """
    rows = db.execute(text(query), params).fetchall()
    return [
        {
            "device": r.device,
            "fee_bank": r.fee_bank,
            "fee_agen": r.fee_agen,
        }
        for r in rows
    ]


# ── Chart Region Luar Aceh ────────────────────────────────────────────────────
@router.get("/chart/region")
def get_chart_region(
    month_from: Optional[str] = Query(None),
    month_to: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, None, area, cabang)
    # Tambah exclude Aceh
    where = where + " AND region != 'RO ACEH'"
    query = f"""
        SELECT
            region,
            SUM(fee_bank) as fee_bank,
            SUM(fee_agen) as fee_agen
        FROM bsi_agen_monitoring
        {where}
        GROUP BY region
        ORDER BY fee_agen DESC
    """
    rows = db.execute(text(query), params).fetchall()
    return [
        {
            "region": r.region,
            "fee_bank": r.fee_bank,
            "fee_agen": r.fee_agen,
        }
        for r in rows
    ]


# ── Chart Area Aceh ───────────────────────────────────────────────────────────
@router.get("/chart/aceh")
def get_chart_aceh(
    month_from: Optional[str] = Query(None),
    month_to: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, "RO ACEH", None, cabang)
    query = f"""
        SELECT
            area,
            SUM(fee_bank) as fee_bank,
            SUM(fee_agen) as fee_agen
        FROM bsi_agen_monitoring
        {where}
        GROUP BY area
        ORDER BY fee_bank DESC
    """
    rows = db.execute(text(query), params).fetchall()
    return [
        {
            "area": r.area,
            "fee_bank": r.fee_bank,
            "fee_agen": r.fee_agen,
        }
        for r in rows
    ]


# ── Chart Trend Line ───────────────────────────────────────────────────────────
@router.get("/chart/trend")
def get_trend(
    month_from: Optional[str] = Query(None),
    month_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, region, area, cabang)
    query = f"""
        SELECT
            bulan,
            bulan_date,
            SUM(fee_bank) as fee_bank,
            SUM(fee_agen) as fee_agen
        FROM bsi_agen_monitoring
        {where}
        GROUP BY bulan, bulan_date
        HAVING SUM(fee_bank) > 0 OR SUM(fee_agen) > 0
        ORDER BY bulan_date ASC
    """
    rows = db.execute(text(query), params).fetchall()
    return [
        {
            "bulan": r.bulan,
            "fee_bank": r.fee_bank,
            "fee_agen": r.fee_agen,
        }
        for r in rows
    ]


# ── Pareto ─────────────────────────────────────────────────────────────────
@router.get("/chart/pareto")
def get_pareto(
    month_from: Optional[str] = Query(None),
    month_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    # Build filter untuk join ke bsi_agen_monitoring
    conditions = ["a.tipe_bulan = 'Bulanan'"]
    params = {}

    if month_from:
        conditions.append("p.bulan_date >= :month_from")
    # Convert "Dec-25" → "2025-12-01"
    try:
        from datetime import datetime

        dt = datetime.strptime(month_from, "%b-%y")
        params["month_from"] = dt.strftime("%Y-%m-01")
    except:
        params["month_from"] = f"{month_from}-01"
    if month_to:
        conditions.append("p.bulan_date <= :month_to")
    try:
        from datetime import datetime

        dt = datetime.strptime(month_to, "%b-%y")
        params["month_to"] = dt.strftime("%Y-%m-01")
    except:
        params["month_to"] = f"{month_to}-01"
    if region:
        conditions.append("a.region = :region")
        params["region"] = region
    if area:
        conditions.append("a.area = :area")
        params["area"] = area
    if cabang:
        conditions.append("a.nama_cabang = :cabang")
        params["cabang"] = cabang

    where = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT
            p.produk,
            SUM(p.volume_transaksi)  as volume_transaksi,
            SUM(p.jumlah_transaksi)  as jumlah_transaksi
        FROM agen_produk p
        JOIN bsi_agen_monitoring a
            ON p.kode_agen = a.kode_agen
            AND p.bulan    = a.bulan
        {where}
        GROUP BY p.produk
        ORDER BY volume_transaksi DESC
        LIMIT 10
    """

    rows = db.execute(text(query), params).fetchall()

    # Hitung total persentase
    total_volume = sum(r.volume_transaksi for r in rows)
    result = []
    total = 0
    for r in rows:
        total += r.volume_transaksi
        result.append(
            {
                "produk": r.produk,
                "volume_transaksi": r.volume_transaksi,
                "jumlah_transaksi": r.jumlah_transaksi,
                "persentase": (
                    round(r.volume_transaksi / total_volume * 100, 1)
                    if total_volume
                    else 0
                ),
                "total_persentase": (
                    round(total / total_volume * 100, 1) if total_volume else 0
                ),
            }
        )

    return result


# ── Chart Map ─────────────────────────────────────────────────────────────────
@router.get("/map")
def get_map(
    month_from: Optional[str] = Query(None),
    month_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, region, area, cabang)
    query = f"""
        SELECT
            a.kecamatan,
            a.kab_kota,
            a.area,
            a.region,
            COUNT(DISTINCT a.kode_agen) as jumlah_agen,
            SUM(a.jumlah_transaksi)     as jumlah_transaksi,
            SUM(a.volume_transaksi)     as volume_transaksi,
            k.lat,
            k.lon
        FROM bsi_agen_monitoring a
        LEFT JOIN cabang_koordinat k
            ON TRIM(a.kode_cabang) = TRIM(k.kode_cabang)
        {where}
        AND k.lat IS NOT NULL
        GROUP BY a.kecamatan, a.kab_kota, a.area, a.region, k.lat, k.lon
        ORDER BY jumlah_agen DESC
    """
    rows = db.execute(text(query), params).fetchall()
    return [
        {
            "kecamatan": r.kecamatan,
            "kab_kota": r.kab_kota,
            "area": r.area,
            "region": r.region,
            "jumlah_agen": r.jumlah_agen,
            "jumlah_transaksi": r.jumlah_transaksi,
            "volume_transaksi": r.volume_transaksi,
            "lat": r.lat,
            "lon": r.lon,
        }
        for r in rows
    ]


# ── Top 10 Agen ───────────────────────────────────────────────────────────────
@router.get("/top10")
def get_top10(
    month_from: Optional[str] = Query(None),
    month_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, region, area, cabang)
    query = f"""
        SELECT
            kode_agen,
            nama_agen,
            nama_cabang,
            area,
            region,
            kecamatan,
            SUM(jumlah_transaksi) as jumlah_transaksi,
            SUM(volume_transaksi) as volume_transaksi,
            SUM(fee_bank) as fee_bank,
            SUM(fee_agen) as fee_agen
        FROM bsi_agen_monitoring
        {where}
        GROUP BY kode_agen, nama_agen, nama_cabang, area, region, kecamatan
        ORDER BY volume_transaksi DESC
    """
    rows = db.execute(text(query), params).fetchall()
    return [
        {
            "kode_agen": r.kode_agen,
            "nama_agen": r.nama_agen,
            "nama_cabang": r.nama_cabang,
            "area": r.area,
            "region": r.region,
            "kecamatan": r.kecamatan,
            "jumlah_transaksi": r.jumlah_transaksi,
            "volume_transaksi": r.volume_transaksi,
            "fee_bank": r.fee_bank,
            "fee_agen": r.fee_agen,
        }
        for r in rows
    ]


@router.get("/all-agen")
def get_all_agen(
    month_from: Optional[str] = Query(None),
    month_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, region, area, cabang)
    query = f"""
        SELECT
            kode_agen,
            nama_agen,
            nama_cabang,
            area,
            region,
            kecamatan,
            SUM(jumlah_transaksi) as jumlah_transaksi,
            SUM(volume_transaksi) as volume_transaksi,
            SUM(fee_bank) as fee_bank,
            SUM(fee_agen) as fee_agen
        FROM bsi_agen_monitoring
        {where}
        GROUP BY kode_agen, nama_agen, nama_cabang, area, region, kecamatan
        ORDER BY volume_transaksi DESC
    """
    rows = db.execute(text(query), params).fetchall()
    return [
        {
            "kode_agen": r.kode_agen,
            "nama_agen": r.nama_agen,
            "nama_cabang": r.nama_cabang,
            "area": r.area,
            "region": r.region,
            "kecamatan": r.kecamatan,
            "jumlah_transaksi": r.jumlah_transaksi,
            "volume_transaksi": r.volume_transaksi,
            "fee_bank": r.fee_bank,
            "fee_agen": r.fee_agen,
        }
        for r in rows
    ]
