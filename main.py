from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer
from database import engine, Base
from routers import upload, dashboard, auth

# Buat tabel kalau belum ada
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="BSI Agen Dashboard API",
    description="API untuk monitoring agen BSI",
    version="1.0.0",
)

# CORS — izinkan Vue.js connect ke API ini
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(upload.router)
app.include_router(dashboard.router)
app.include_router(auth.router)


@app.get("/")
def root():
    return {"message": "BSI Agen Dashboard API", "status": "running"}
