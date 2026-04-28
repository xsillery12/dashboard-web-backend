from sqlalchemy import Column, Date, Integer, String, Float, DateTime
from database import Base

class AgenMonitoring(Base):
    __tablename__ = "bsi_agen_monitoring"

    id               = Column(Integer, primary_key=True, index=True)
    kode_agen        = Column(String)
    nama_agen        = Column(String)
    nama_toko        = Column(String)
    kode_cabang      = Column(String)
    nama_cabang      = Column(String)
    area             = Column(String)
    region           = Column(String)
    kelurahan        = Column(String)
    kecamatan        = Column(String)
    kab/kota          = Column(String)
    device           = Column(String)
    tgl_daftar       = Column(DateTime)
    bulan            = Column(String)
    tipe_bulan       = Column(String)
    bulan_date       = Column(Date)
    jumlah_transaksi = Column(Float)
    volume_transaksi = Column(Float)