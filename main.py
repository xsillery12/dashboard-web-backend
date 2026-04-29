from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer
from database import engine, Base
from routers import upload, dashboard, auth
import os

# Buat tabel kalau belum ada
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="BSI Agen Dashboard API",
    description="API untuk monitoring agen BSI",
    version="1.0.0",
)

# CORS — izinkan Vue.js connect ke API ini
allowed_origins = [
    "http://localhost:5173",
    "http://localhost:3000",
    "dashboard-web-frontend-production.up.railway.app",
]

# Tambah frontend URL dari environment variable kalau ada
frontend_url = os.getenv("FRONTEND_URL")
if frontend_url and frontend_url not in allowed_origins:
    allowed_origins.append(frontend_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
