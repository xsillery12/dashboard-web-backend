from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from database import get_db
from routers.auth import get_current_user

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


def build_filter(month_from=None, month_to=None, region=None, area=None, cabang=None):
    from datetime import datetime

    def parse_month(m):
        try:
            dt = datetime.strptime(m, "%b-%y")
            return dt.strftime("%Y-%m-01")
        except:
            return f"{m}-01"

    conditions = ["tipe_bulan = 'Bulanan'"]
    params = {}

    if month_from:
        conditions.append("bulan_date >= :month_from")
        params["month_from"] = parse_month(month_from)
    if month_to:
        conditions.append("bulan_date <= :month_to")
        params["month_to"] = parse_month(month_to)

    # Support multiple values (comma separated)
    if region:
        regions = [r.strip() for r in region.split(",")]
        if len(regions) == 1:
            conditions.append("region = :region")
            params["region"] = regions[0]
        else:
            placeholders = ", ".join([f":region_{i}" for i in range(len(regions))])
            conditions.append(f"region IN ({placeholders})")
            for i, r in enumerate(regions):
                params[f"region_{i}"] = r

    if area:
        areas = [a.strip() for a in area.split(",")]
        if len(areas) == 1:
            conditions.append("area = :area")
            params["area"] = areas[0]
        else:
            placeholders = ", ".join([f":area_{i}" for i in range(len(areas))])
            conditions.append(f"area IN ({placeholders})")
            for i, a in enumerate(areas):
                params[f"area_{i}"] = a

    if cabang:
        cabangs = [c.strip() for c in cabang.split(",")]
        if len(cabangs) == 1:
            conditions.append("nama_cabang = :cabang")
            params["cabang"] = cabangs[0]
        else:
            placeholders = ", ".join([f":cabang_{i}" for i in range(len(cabangs))])
            conditions.append(f"nama_cabang IN ({placeholders})")
            for i, c in enumerate(cabangs):
                params[f"cabang_{i}"] = c

    where = "WHERE " + " AND ".join(conditions)
    return where, params


def build_transaksi_filter(
    date_from=None,
    date_to=None,
    region=None,
    area=None,
    cabang=None,
):
    conditions = ["1=1"]
    params = {}

    if date_from:
        conditions.append("t.transaction_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("t.transaction_date <= :date_to")
        params["date_to"] = date_to

    if region:
        regions = [r.strip() for r in region.split(",")]
        if len(regions) == 1:
            conditions.append("c.region = :region")
            params["region"] = regions[0]
        else:
            placeholders = ", ".join([f":region_{i}" for i in range(len(regions))])
            conditions.append(f"c.region IN ({placeholders})")
            for i, r in enumerate(regions):
                params[f"region_{i}"] = r

    if area:
        areas = [a.strip() for a in area.split(",")]
        if len(areas) == 1:
            conditions.append("c.area = :area")
            params["area"] = areas[0]
        else:
            placeholders = ", ".join([f":area_{i}" for i in range(len(areas))])
            conditions.append(f"c.area IN ({placeholders})")
            for i, a in enumerate(areas):
                params[f"area_{i}"] = a

    if cabang:
        cabangs = [c.strip() for c in cabang.split(",")]
        if len(cabangs) == 1:
            conditions.append("c.nama_outlet = :cabang")
            params["cabang"] = cabangs[0]
        else:
            placeholders = ", ".join([f":cabang_{i}" for i in range(len(cabangs))])
            conditions.append(f"c.nama_outlet IN ({placeholders})")
            for i, cab in enumerate(cabangs):
                params[f"cabang_{i}"] = cab

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
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

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

    # Hitung periode sebelumnya
    prev_params = dict(params)
    comparison_label = "vs 30 Days"

    if month_from and month_to:
        # Hitung Selisih Bulan
        dt_from = datetime.strptime(params["month_from"], "%Y-%m-01")
        dt_to = datetime.strptime(params["month_to"], "%Y-%m-01")
        diff = relativedelta(dt_to, dt_from)
        total_months = diff.months + diff.years * 12 + 1

        prev_to = dt_from - relativedelta(months=1)
        prev_from = prev_to - relativedelta(months=total_months - 1)

        prev_params["month_from"] = prev_from.strftime("%Y-%m-01")
        prev_params["month_to"] = prev_to.strftime("%Y-%m-01")

        if total_months == 1:
            comparison_label = f"vs {prev_to.strftime('%b %Y')}"
        else:
            comparison_label = f"vs {total_months} months ago"

    elif month_from:
        dt_from = datetime.strptime(params["month_from"], "%Y-%m-01")
        prev_from = dt_from - relativedelta(months=1)
        prev_params["month_from"] = prev_from.strftime("%Y-%m-01")
        comparison_label = f"vs {prev_from.strftime('%b %Y')}"

    else:
        # default: 30 days
        today = datetime.today()
        prev_to = today - timedelta(days=1)
        prev_from = prev_to - timedelta(days=29)
        prev_params["month_from"] = prev_from.strftime("%Y-%m-01")
        prev_params["month_to"] = prev_to.strftime("%Y-%m-01")
        comparison_label = "vs 30 Days"

    # build prev where
    prev_conditions = ["tipe_bulan = 'Bulanan'"]
    if "month_from" in prev_params:
        prev_conditions.append("bulan_date >= :month_from")
    if "month_to" in prev_params:
        prev_conditions.append("bulan_date <= :month_to")
    # Region - Support multiple values
    if region:
        regions_list = [r.strip() for r in region.split(",")]
        if len(regions_list) == 1:
            prev_conditions.append("region = :region")
        else:
            placeholders = ", ".join([f":region_{i}" for i in range(len(regions_list))])
            prev_conditions.append(f"region IN ({placeholders})")
    # Area - Support multiple values
    if area:
        areas_list = [a.strip() for a in area.split(",")]
        if len(areas_list) == 1:
            prev_conditions.append("area = :area")
        else:
            placeholders = ", ".join([f":area_{i}" for i in range(len(areas_list))])
            prev_conditions.append(f"area IN ({placeholders})")
    # Cabang - Support multiple values
    if cabang:
        cabang_list = [c.strip() for c in cabang.split(",")]
        if len(cabang_list) == 1:
            prev_conditions.append("nama_cabang = :cabang")
        else:
            placeholders = ", ".join([f":cabang_{i}" for i in range(len(cabang_list))])
            prev_conditions.append(f"nama_cabang IN ({placeholders})")

    prev_where = "WHERE " + " AND ".join(prev_conditions)

    prev_result = db.execute(
        text(f"""
        SELECT
            COUNT(DISTINCT kode_agen) as total_agen,
            SUM(volume_transaksi) as total_volume,
            SUM(jumlah_transaksi) as total_jumlah
        FROM bsi_agen_monitoring {prev_where}
    """),
        prev_params,
    ).fetchone()

    def calc_change(current, previous):
        if not previous or previous == 0:
            return None
        return round((current - previous) / previous * 100, 1)

    return {
        "total_agen": result.total_agen,
        "total_volume": result.total_volume,
        "total_jumlah": result.total_jumlah,
        "comparison_label": comparison_label,
        "changes": {
            "total_agen": calc_change(
                result.total_agen or 0, prev_result.total_agen or 0
            ),
            "total_volume": calc_change(
                result.total_volume or 0, prev_result.total_volume or 0
            ),
            "total_jumlah": calc_change(
                result.total_jumlah or 0, prev_result.total_jumlah or 0
            ),
        },
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
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    # Regions take all
    regions = db.execute(
        text(
            "SELECT DISTINCT region FROM bsi_agen_monitoring "
            "WHERE tipe_bulan='Bulanan' AND region IS NOT NULL ORDER BY region"
        )
    ).fetchall()

    # Areas depend on region
    area_where = "WHERE tipe_bulan='Bulanan' AND area IS NOT NULL"
    area_params = {}
    if region:
        regions_list = [r.strip() for r in region.split(",")]
        if len(regions_list) == 1:
            area_where += " AND region = :region"
            area_params["region"] = regions_list[0]
        else:
            placeholders = ", ".join([f":region_{i}" for i in range(len(regions_list))])
            area_where += f" AND region IN ({placeholders})"
            for i, r in enumerate(regions_list):
                area_params[f"region_{i}"] = r

    areas = db.execute(
        text(f"""
        SELECT DISTINCT area FROM bsi_agen_monitoring
        {area_where}
        ORDER BY area
    """),
        area_params,
    ).fetchall()

    # Cabangs depend on area
    cabang_where = "WHERE tipe_bulan='Bulanan' AND nama_cabang IS NOT NULL"
    cabang_params = dict(area_params)
    if area:
        areas_list = [a.strip() for a in area.split(",")]
        if len(areas_list) == 1:
            cabang_where += " AND area = :area"
            cabang_params["area"] = areas_list[0]
        else:
            placeholders = ", ".join([f":area_{i}" for i in range(len(areas_list))])
            cabang_where += f" AND area IN ({placeholders})"
            for i, a in enumerate(areas_list):
                cabang_params[f"area_{i}"] = a

    cabangs = db.execute(
        text(f"""
        SELECT DISTINCT nama_cabang FROM bsi_agen_monitoring
        {cabang_where}
        ORDER BY nama_cabang
    """),
        cabang_params,
    ).fetchall()

    bulans = db.execute(
        text(
            "SELECT DISTINCT bulan FROM bsi_agen_monitoring "
            "WHERE tipe_bulan='Bulanan' ORDER BY bulan_date"
        )
    ).fetchall()

    return {
        "regions": [r.region for r in regions],
        "areas": [a.area for a in areas],
        "cabangs": [c.nama_cabang for c in cabangs],
        "bulans": [{"label": b.bulan, "value": str(b.bulan_date)[:7]} for b in bulans],
    }


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
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_filter(month_from, month_to, region, area, cabang)
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


# ── Dashboard Transaksi Agen ──────────────────────────────────────────────────
@router.get("/transaksi/filters")
def get_transaksi_filters(
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    try:
        regions = db.execute(text("""
            SELECT DISTINCT c.region
            FROM transaksi_agen t
            JOIN cabang_region c ON t.agent_branch = c.kode_cabang
            WHERE c.region IS NOT NULL
            ORDER BY c.region
        """)).fetchall()

        area_where = "WHERE c.area IS NOT NULL"
        area_params = {}
        if region:
            regions_list = [r.strip() for r in region.split(",")]
            if len(regions_list) == 1:
                area_where += " AND c.region = :region"
                area_params["region"] = regions_list[0]
            else:
                placeholders = ", ".join(
                    [f":region_{i}" for i in range(len(regions_list))]
                )
                area_where += f" AND c.region IN ({placeholders})"
                for i, r in enumerate(regions_list):
                    area_params[f"region_{i}"] = r

        areas = db.execute(
            text(f"""
            SELECT DISTINCT c.area
            FROM transaksi_agen t
            JOIN cabang_region c ON t.agent_branch = c.kode_cabang
            {area_where}
            ORDER BY c.area
        """),
            area_params,
        ).fetchall()

        cabang_where = area_where
        cabang_params = dict(area_params)
        if area:
            areas_list = [a.strip() for a in area.split(",")]
            if len(areas_list) == 1:
                cabang_where += " AND c.area = :area"
                cabang_params["area"] = areas_list[0]
            else:
                placeholders = ", ".join([f":area_{i}" for i in range(len(areas_list))])
                cabang_where += f" AND c.area IN ({placeholders})"
                for i, a in enumerate(areas_list):
                    cabang_params[f"area_{i}"] = a

        cabangs = db.execute(
            text(f"""
            SELECT DISTINCT c.nama_outlet
            FROM transaksi_agen t
            JOIN cabang_region c ON t.agent_branch = c.kode_cabang
            {cabang_where} AND c.nama_outlet IS NOT NULL
            ORDER BY c.nama_outlet
        """),
            cabang_params,
        ).fetchall()

        return {
            "regions": [r.region for r in regions],
            "areas": [a.area for a in areas],
            "cabangs": [c.nama_outlet for c in cabangs],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/transaksi/stats")
def get_transaksi_stats(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

    where, params = build_transaksi_filter(date_from, date_to, region, area, cabang)

    result = db.execute(
        text(f"""
        SELECT
            COUNT(*)              as total_transaksi,
            SUM(t.amount)         as total_volume,
            AVG(t.amount)         as avg_amount
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        {where}
    """),
        params,
    ).fetchone()

    # Komparasi periode sebelumnya
    prev_params = dict(params)
    comparison_label = "vs 30 Days"

    if date_from and date_to:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_to = datetime.strptime(date_to, "%Y-%m-%d")
        diff_days = (dt_to - dt_from).days + 1

        prev_to = dt_from - timedelta(days=1)
        prev_from = prev_to - timedelta(days=diff_days - 1)

        prev_params["date_from"] = prev_from.strftime("%Y-%m-%d")
        prev_params["date_to"] = prev_to.strftime("%Y-%m-%d")
        comparison_label = (
            f"vs {prev_from.strftime('%d %b')} - {prev_to.strftime('%d %b')}"
        )
    else:
        today = datetime.today()
        prev_to = today - timedelta(days=30)
        prev_from = prev_to - timedelta(days=29)
        prev_params["date_from"] = prev_from.strftime("%Y-%m-%d")
        prev_params["date_to"] = prev_to.strftime("%Y-%m-%d")
        comparison_label = "vs 30 Days"

    # Build prev_where
    prev_conditions = [
        "1=1",
        "t.transaction_date >= :date_from",
        "t.transaction_date <= :date_to",
    ]
    if region:
        regions_list = [r.strip() for r in region.split(",")]
        if len(regions_list) == 1:
            prev_conditions.append("c.region = :region")
        else:
            placeholders = ", ".join([f":region_{i}" for i in range(len(regions_list))])
            prev_conditions.append(f"c.region IN ({placeholders})")
    if area:
        areas_list = [a.strip() for a in area.split(",")]
        if len(areas_list) == 1:
            prev_conditions.append("c.area = :area")
        else:
            placeholders = ", ".join([f":area_{i}" for i in range(len(areas_list))])
            prev_conditions.append(f"c.area IN ({placeholders})")
    if cabang:
        cabangs_list = [c.strip() for c in cabang.split(",")]
        if len(cabangs_list) == 1:
            prev_conditions.append("c.nama_outlet = :cabang")
        else:
            placeholders = ", ".join([f":cabang_{i}" for i in range(len(cabangs_list))])
            prev_conditions.append(f"c.nama_outlet IN ({placeholders})")

    prev_where = "WHERE " + " AND ".join(prev_conditions)

    prev_result = db.execute(
        text(f"""
        SELECT
            COUNT(*)      as total_transaksi,
            SUM(t.amount) as total_volume,
            AVG(t.amount) as avg_amount
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        {prev_where}
    """),
        prev_params,
    ).fetchone()

    def calc_change(current, previous):
        if not previous or previous == 0:
            return None
        return round((float(current) - float(previous)) / float(previous) * 100, 1)

    return {
        "total_transaksi": result.total_transaksi or 0,
        "total_volume": float(result.total_volume or 0),
        "avg_amount": float(result.avg_amount or 0),
        "comparison_label": comparison_label,
        "changes": {
            "total_transaksi": calc_change(
                result.total_transaksi or 0, prev_result.total_transaksi or 0
            ),
            "total_volume": calc_change(
                result.total_volume or 0, prev_result.total_volume or 0
            ),
            "avg_amount": calc_change(
                result.avg_amount or 0, prev_result.avg_amount or 0
            ),
        },
    }


# Donut: Tunai vs Kartu berdasarkan Produk
@router.get("/transaksi/produk")
def get_transaksi_produk(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_transaksi_filter(date_from, date_to, region, area, cabang)

    rows = db.execute(
        text(f"""
        SELECT
            CASE
                WHEN t.produk ILIKE '%withdrawal%' OR t.produk ILIKE '%tunai%' THEN 'Tunai'
                ELSE 'Kartu'
            END as kategori,
            COUNT(*) as total
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        {where}
        GROUP BY kategori
        ORDER BY total DESC
    """),
        params,
    ).fetchall()

    return [{"kategori": r.kategori, "total": r.total} for r in rows]


# Bar: Perbandingan Transaksi tiap RO (region)
@router.get("/transaksi/per-region")
def get_transaksi_per_region(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_transaksi_filter(date_from, date_to, region, area, cabang)

    rows = db.execute(
        text(f"""
        SELECT
            c.region,
            COUNT(*)      as total_transaksi,
            SUM(t.amount) as total_volume
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        {where} AND c.region IS NOT NULL
        GROUP BY c.region
        ORDER BY total_transaksi DESC
    """),
        params,
    ).fetchall()

    return [
        {
            "region": r.region,
            "total_transaksi": r.total_transaksi,
            "total_volume": float(r.total_volume or 0),
        }
        for r in rows
    ]


# Traffic per jam dengan date range
@router.get("/transaksi/traffic")
def get_transaksi_traffic(
    interval: int = Query(2),
    dates: Optional[str] = Query(None),
    range_from: Optional[str] = Query(None),
    range_to: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    conditions = ["1=1"]
    params = {"interval": interval}
    use_per_hari = False

    if dates:
        date_list = [d.strip() for d in dates.split(",")]
        placeholders = ", ".join([f":d{i}" for i in range(len(date_list))])
        conditions.append(f"t.transaction_date IN ({placeholders})")
        for i, d in enumerate(date_list):
            params[f"d{i}"] = d
        user_per_hari = len(date_list) > 7

    elif range_from and range_to:
        conditions.append("t.transaction_date >= :range_from")
        conditions.append("t.transaction_date >= :range_to")
        params["range_from"] = range_from
        params[range_to] = range_to

        from datetime import datetime

        dt_from = datetime.strptime(range_from, "%Y-%m-%d")
        dt_to = datetime.strptime(range_to, "%Y-%m-%d")
        diff_days = (dt_to - dt_from).days + 1
        use_per_hari = diff_days > 7
    else:
        if date_from:
            conditions.append("t.transaction_date >= :date_from")
            params["date_from"] = date_from
        if date_to:
            conditions.append("t.transaction_date <= :date_to")
            params["date_to"] = date_to

    if region:
        regions = [r.strip() for r in region.split(",")]
        if len(regions) == 1:
            conditions.append("c.region = :region")
            params["region"] = regions[0]
        else:
            placeholders = ", ".join([f":region_{i}" for i in range(len(regions))])
            conditions.append(f"c.region IN ({placeholders})")
            for i, r in enumerate(regions):
                params[f"region_{i}"] = r

    if area:
        areas = [a.strip() for a in area.split(",")]
        if len(areas) == 1:
            conditions.append("c.area = :area")
            params["area"] = areas[0]
        else:
            placeholders = ", ".join([f":area_{i}" for i in range(len(areas))])
            conditions.append(f"c.area IN ({placeholders})")
            for i, a in enumerate(areas):
                params[f"area_{i}"] = a

    if cabang:
        cabangs = [cc.strip() for cc in cabang.split(",")]
        if len(cabangs) == 1:
            conditions.append("c.nama_outlet = :cabang")
            params["cabang"] = cabangs[0]
        else:
            placeholders = ", ".join([f":cabang_{i}" for i in range(len(cabangs))])
            conditions.append(f"c.nama_outlet IN ({placeholders})")
            for i, cb in enumerate(cabangs):
                params[f"cabang_{i}"] = cb

    where = "WHERE " + " AND ".join(conditions)

    if use_per_hari:
        rows = db.execute(
            text(f"""
        SELECT
            t.transaction_date::text as tanggal,
            COUNT(*) as tanggal
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        {where}
        GROUP BY t.transaction_date
        ORDER BY t.transaction_date
    """),
            params,
        ).fetchall()

        return {
            "mode": "per_hari",
            "labels": [str(r.tanggal) for r in rows],
            "datasets": [
                {
                    "tanggal": "Semua",
                    "total": [r.total for r in rows],
                }
            ],
        }

    rows = db.execute(
        text(f"""
        SELECT
            t.transaction_date::text as tanggal,
            (EXTRACT(HOUR FROM t.transaction_time) / :interval)::int * :interval AS jam_group,
            COUNT(*) as total
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        {where}
        GROUP BY t.transaction_date, jam_group
        ORDER BY t.transaction_date, jam_group
    """),
        params,
    ).fetchall()

    data_by_date = {}
    for r in rows:
        tgl = str(r.tanggal)
        if tgl not in data_by_date:
            data_by_date[tgl] = {}
        data_by_date[tgl][r.jam_group] = r.total

    jam_slots = [f"{h:02d}:00 - {h + interval:02d}:00" for h in range(0, 24, interval)]
    tanggal_unik = sorted(data_by_date.keys())

    return {
        "mode": "per_jam",
        "labels": jam_slots,
        "datasets": [
            {
                "tanggal": tgl,
                "total": [
                    data_by_date.get(tgl, {}).get(h, 0) for h in range(0, 24, interval)
                ],
            }
            for tgl in tanggal_unik
        ],
    }


# Top 10 Product Pareto
@router.get("/transaksi/pareto")
def get_transaksi_pareto(
    metric: str = Query("count"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_transaksi_filter(date_from, date_to, region, area, cabang)

    # Pilih agregat berdasarkan metric
    agg_expr = "SUM(t.amount)" if metric == "volume" else "COUNT(*)"

    rows = db.execute(
        text(f"""
        SELECT
            t.produk_detail as produk,
            {agg_expr} as total
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        {where} AND t.produk_detail IS NOT NULL
        GROUP BY t.produk_detail
        ORDER BY total DESC
        LIMIT 10
    """),
        params,
    ).fetchall()

    data = [{"produk": r.produk, "total": float(r.total or 0)} for r in rows]

    # Hitung cumulative percentage
    grand_total = sum(d["total"] for d in data)
    cumulative = 0
    for d in data:
        cumulative += d["total"]
        d["cumulative_pct"] = (
            round((cumulative / grand_total * 100), 1) if grand_total else 0
        )

    return data


# Top 10 agen
@router.get("/transaksi/top-agen")
def get_top_transaksi_agen(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_transaksi_filter(date_from, date_to, region, area, cabang)

    rows = db.execute(
        text(f"""
        SELECT
            t.agent_code, t.agent_name, c.region, c.area, c.nama_outlet,
            COUNT(*)      as total_transaksi,
            SUM(t.amount) as total_volume,
            AVG(t.amount) as avg_amount
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        {where}
        GROUP BY t.agent_code, t.agent_name, c.region, c.area, c.nama_outlet
        ORDER BY total_transaksi DESC
        LIMIT 10
    """),
        params,
    ).fetchall()

    return [
        {
            "agent_code": r.agent_code,
            "agent_name": r.agent_name,
            "region": r.region,
            "area": r.area,
            "nama_outlet": r.nama_outlet,
            "total_transaksi": r.total_transaksi,
            "total_volume": round(float(r.total_volume or 0), 2),
            "avg_amount": round(float(r.avg_amount or 0), 2),
        }
        for r in rows
    ]


# All agen
@router.get("/transaksi/all-agen")
def get_all_transaksi_agen(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    cabang: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_transaksi_filter(date_from, date_to, region, area, cabang)

    rows = db.execute(
        text(f"""
        SELECT
            t.agent_code, t.agent_name, c.region, c.area, c.nama_outlet,
            COUNT(*)      as total_transaksi,
            SUM(t.amount) as total_volume,
            AVG(t.amount) as avg_amount
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        {where}
        GROUP BY t.agent_code, t.agent_name, c.region, c.area, c.nama_outlet
        ORDER BY total_transaksi DESC
    """),
        params,
    ).fetchall()

    return [
        {
            "agent_code": r.agent_code,
            "agent_name": r.agent_name,
            "region": r.region,
            "area": r.area,
            "nama_outlet": r.nama_outlet,
            "total_transaksi": r.total_transaksi,
            "total_volume": round(float(r.total_volume or 0), 2),
            "avg_amount": round(float(r.avg_amount or 0), 2),
        }
        for r in rows
    ]


# ── Home Dashboard ────────────────────────────────────────────────────────────
@router.get("/home/stats")
def get_home_stats(
    db: Session = Depends(get_db), current_user: dict = Depends(get_current_user)
):
    # KPI Monitoring Agen
    agen = db.execute(text("""
        SELECT
            COUNT(DISTINCT kode_agen) as total_agen,
            COUNT(DISTINCT CASE WHEN jumlah_transaksi > 0 THEN kode_agen END) as agen_aktif,
            SUM(fee_bank) as total_fee_bank,
            SUM(fee_agen) as total_fee_agen
        FROM bsi_agen_monitoring
        WHERE tipe_bulan = 'Bulanan'
    """)).fetchone()

    # KPI Transaksi
    transaksi = db.execute(text("""
        SELECT
            COUNT(*) as total_transaksi,
            SUM(amount) as total_volume,
            AVG(amount) as avg_amount 
        FROM transaksi_agen
    """)).fetchone()

    total_trx = transaksi.total_transaksi or 0

    return {
        "total_agen": agen.total_agen or 0,
        "agen_aktif": agen.agen_aktif or 0,
        "total_fee_bank": float(agen.total_fee_bank or 0),
        "total_fee_agen": float(agen.total_fee_agen or 0),
        "total_transaksi": total_trx,
        "total_volume": float(transaksi.total_volume or 0),
        "avg_amount": float(transaksi.avg_amount or 0),
        "success_rate": 100,
    }


@router.get("/home/summary")
def get_home_summary(
    db: Session = Depends(get_db), current_user: dict = Depends(get_current_user)
):
    # Top Region by Fee Bank
    top_region = db.execute(text("""
        SELECT region, SUM(fee_bank) as fee_bank
        FROM bsi_agen_monitoring
        WHERE tipe_bulan = 'Bulanan'
        GROUP BY region
        ORDER BY fee_bank DESC
        LIMIT 1
    """)).fetchone()

    # Top Area by Transaksi
    top_area = db.execute(text("""
        SELECT c.area, COUNT(*) as total
        FROM transaksi_agen t
        JOIN cabang_region c ON t.agent_branch = c.kode_cabang
        GROUP BY c.area
        ORDER BY total DESC
        LIMIT 1
    """)).fetchone()

    # Top Produk
    top_produk = db.execute(text("""
        SELECT produk, COUNT(*) as total
        FROM transaksi_agen
        WHERE produk IS NOT NULL
        GROUP BY produk
        ORDER BY total DESC
        LIMIT 1
    """)).fetchone()

    return {
        "top_region": {
            "nama": top_region.region if top_region else "-",
            "nilai": top_region.fee_bank if top_region else 0,
        },
        "top_area": {
            "nama": top_area.area if top_area else "-",
            "nilai": top_area.total if top_area else 0,
        },
        "top_produk": {
            "nama": top_produk.produk if top_produk else "-",
            "nilai": top_produk.total if top_produk else 0,
        },
    }


@router.get("/home/trend")
def get_home_trend(
    db: Session = Depends(get_db), current_user: dict = Depends(get_current_user)
):
    rows = db.execute(text("""
        SELECT bulan, bulan_Date, SUM(fee_bank) as fee_bank
        FROM bsi_agen_monitoring
        WHERE tipe_bulan = 'Bulanan'
        AND fee_bank > 0
        GROUP BY bulan, bulan_Date
        ORDER BY bulan_date ASC
    """)).fetchall()

    return [{"bulan": r.bulan, "fee_bank": float(r.fee_bank)} for r in rows]


@router.get("/home/traffic")
def get_home_traffic(
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    from datetime import date

    # Default: tahun terbaru yang ada datanya
    if not year:
        last = db.execute(text("""
            SELECT EXTRACT(YEAR FROM MAX(transaction_date))::int as tahun
            FROM transaksi_agen
            WHERE transaction_date IS NOT NULL
        """)).fetchone()
        year = last.tahun if last and last.tahun else date.today().year

    # Tahun yang tersedia
    years_rows = db.execute(text("""
        SELECT DISTINCT EXTRACT(YEAR FROM transaction_date)::int as tahun
        FROM transaksi_agen
        WHERE transaction_date IS NOT NULL
        ORDER BY tahun DESC
    """)).fetchall()

    # Agregat per bulan
    rows = db.execute(
        text("""
        SELECT
            EXTRACT(MONTH FROM transaction_date)::int as bulan,
            COUNT(*) as total
        FROM transaksi_agen
        WHERE EXTRACT(YEAR FROM transaction_date) = :year
        GROUP BY bulan
        ORDER BY bulan
    """),
        {"year": year},
    ).fetchall()

    db_data = {int(r.bulan): r.total for r in rows}

    bulan_names = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "Mei",
        "Jun",
        "Jul",
        "Agu",
        "Sep",
        "Okt",
        "Nov",
        "Des",
    ]
    result = [
        {"bulan": nama, "total": db_data.get(i, 0)}
        for i, nama in enumerate(bulan_names, start=1)
    ]

    return {
        "year": year,
        "years_available": [r.tahun for r in years_rows],
        "data": result,
    }


# ── Dashboard Heartbeat EDC ───────────────────────────────────────────────────
def build_heartbeat_filter(date_from=None, date_to=None, region=None, area=None):
    conditions = ["1=1"]
    params = {}

    if date_from:
        conditions.append("last_heartbeat >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("last_heartbeat <= :date_to")
        params["date_to"] = date_to

    if region:
        regions = [r.strip() for r in region.split(",")]
        if len(regions) == 1:
            conditions.append("region = :region")
            params["region"] = regions[0]
        else:
            placeholders = ", ".join([f":region_{i}" for i in range(len(regions))])
            conditions.append(f"region IN ({placeholders})")
            for i, r in enumerate(regions):
                params[f"region_{i}"] = r

    if area:
        areas = [a.strip() for a in area.split(",")]
        if len(areas) == 1:
            conditions.append("area = :area")
            params["area"] = areas[0]
        else:
            placeholders = ", ".join([f":area_{i}" for i in range(len(areas))])
            conditions.append(f"area IN ({placeholders})")
            for i, a in enumerate(areas):
                params[f"area_{i}"] = a

    where = "WHERE " + " AND ".join(conditions)
    return where, params


@router.get("/heartbeat/filters")
def get_heartbeat_filters(
    region: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    # Regions (selalu ambil semua)
    regions = db.execute(text("""
        SELECT DISTINCT region FROM heartbeat_edc
        WHERE region IS NOT NULL
        ORDER BY region
    """)).fetchall()

    # Areas (filter by region jika ada)
    area_where = "WHERE area IS NOT NULL"
    area_params = {}
    if region:
        regions_list = [r.strip() for r in region.split(",")]
        if len(regions_list) == 1:
            area_where += " AND region = :region"
            area_params["region"] = regions_list[0]
        else:
            placeholders = ", ".join([f":region_{i}" for i in range(len(regions_list))])
            area_where += f" AND region IN ({placeholders})"
            for i, r in enumerate(regions_list):
                area_params[f"region_{i}"] = r

    areas = db.execute(
        text(f"""
            SELECT DISTINCT area FROM heartbeat_edc
            {area_where}
            ORDER BY area
        """),
        area_params,
    ).fetchall()

    return {
        "regions": [r.region for r in regions],
        "areas": [a.area for a in areas],
    }


@router.get("/heartbeat/stats")
def get_heartbeat_stats(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_heartbeat_filter(date_from, date_to, region, area)

    result = db.execute(
        text(f"""
        SELECT
            COUNT(*) as total_heartbeat,
            COUNT(CASE WHEN selisih_hari < 30 THEN 1 END) as overall_heartbeat,
            COUNT(DISTINCT kode_cabang) as total_cabang
        FROM heartbeat_edc {where}
    """),
        params,
    ).fetchone()

    total_hb = result.total_heartbeat or 0
    overall_hb = result.overall_heartbeat or 0
    overall_pct = round(overall_hb / total_hb * 100, 1) if total_hb else 0

    return {
        "total_heartbeat": total_hb,
        "overall_heartbeat": overall_hb,
        "overall_percent": overall_pct,
        "total_cabang": result.total_cabang or 0,
    }


@router.get("/heartbeat/chart-region")
def get_heartbeat_chart_region(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_heartbeat_filter(date_from, date_to, region, area)

    rows = db.execute(
        text(f"""
        SELECT
            region,
            COUNT(*) as total_edc
        FROM heartbeat_edc {where}
        AND selisih_hari > 30
        AND region IS NOT NULL
        GROUP BY region
        ORDER BY total_edc DESC
    """),
        params,
    ).fetchall()

    return [{"region": r.region, "total_edc": r.total_edc} for r in rows]


@router.get("/heartbeat/chart-kategori")
def get_heartbeat_chart_kategori(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_heartbeat_filter(date_from, date_to, region, area)

    rows = db.execute(
        text(f"""
        SELECT
            kategori,
            COUNT(*) as total
        FROM heartbeat_edc {where}
        AND kategori IN ('1-3 bulan', '3-12 bulan')
        GROUP BY kategori
        ORDER BY kategori
    """),
        params,
    ).fetchall()

    return [{"kategori": r.kategori, "total": r.total} for r in rows]


@router.get("/heartbeat/map")
def get_heartbeat_map(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_heartbeat_filter(date_from, date_to, region, area)

    rows = db.execute(
        text(f"""
        SELECT
            kode_agen,
            contact_person as nama_agen,
            serial_number,
            last_heartbeat,
            selisih_hari,
            kategori,
            region,
            area,
            kabupaten_kota,
            client_group,
            latitude,
            longitude
        FROM heartbeat_edc {where}
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND latitude BETWEEN -11 AND 6
        AND longitude BETWEEN 95 AND 141
    """),
        params,
    ).fetchall()

    return [
        {
            "kode_agen": r.kode_agen,
            "nama_agen": r.nama_agen,
            "serial_number": r.serial_number,
            "last_heartbeat": str(r.last_heartbeat) if r.last_heartbeat else None,
            "selisih_hari": r.selisih_hari,
            "kategori": r.kategori,
            "region": r.region,
            "area": r.area,
            "kabupaten_kota": r.kabupaten_kota,
            "client_group": r.client_group,
            "latitude": float(r.latitude),
            "longitude": float(r.longitude),
        }
        for r in rows
    ]


@router.get("/heartbeat/detail")
def get_heartbeat_detail(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    where, params = build_heartbeat_filter(date_from, date_to, region, area)

    rows = db.execute(
        text(f"""
        SELECT
            kode_agen,
            contact_person as nama_agen,
            serial_number,
            last_heartbeat,
            selisih_hari,
            region,
            area,
            nama_outlet
        FROM heartbeat_edc {where}
        ORDER BY selisih_hari DESC NULLS LAST
    """),
        params,
    ).fetchall()

    return [
        {
            "kode_agen": r.kode_agen,
            "nama_agen": r.nama_agen,
            "serial_number": r.serial_number,
            "last_heartbeat": str(r.last_heartbeat) if r.last_heartbeat else None,
            "selisih_hari": r.selisih_hari,
            "region": r.region,
            "area": r.area,
            "nama_outlet": r.nama_outlet,
        }
        for r in rows
    ]
