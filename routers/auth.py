from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta
from typing import Optional
import bcrypt
from jose import JWTError, jwt
from dotenv import load_dotenv
import os

load_dotenv()

router = APIRouter(prefix="/auth", tags=["Auth"])

# Config

SECRET_KEY = os.getenv("SECRET_KEY", "bsi-secret-key-ganti-ini-nanti")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7

# Schemas


class RegisterRequest(BaseModel):
    nama: str
    email: EmailStr
    password: str
    role: Optional[str] = "staff"


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


# Helper Function
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())


def create_token(data: dict, expires_delta: timedelta) -> str:
    to_encode = data.copy()
    to_encode["exp"] = datetime.utcnow() + expires_delta
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def create_access_token(user: dict) -> str:
    return create_token(
        {
            "sub": str(user["id"]),
            "email": user["email"],
            "nama": user["nama"],
            "role": user["role"],
            "foto": user.get("foto", ""),
        },
        timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )


def create_refresh_token(user_id: int) -> str:
    return create_token(
        {"sub": str(user_id), "type": "refresh"},
        timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
    )


# Dependency: get current user
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database import get_db

security = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="TOKEN Tidak Valid")
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Token expired atau tidka valid")


# Endpoints
@router.post("/register")
def register(req: RegisterRequest, db: Session = Depends(get_db)):
    # Cek email yang sudah ada
    existing = db.execute(
        text("SELECT id FROM users WHERE email= :email"), {"email": req.email}
    ).fetchone()
    if existing:
        raise HTTPException(status_code=400, detail="Email sudah Terdaftar")

    # Hash Password & Save
    hashed = hash_password(req.password)
    db.execute(
        text(
            """INSERT INTO users (nama, email, password_hash, role) VALUES (:nama, :email, :password_hash, :role)"""
        ),
        {
            "nama": req.nama,
            "email": req.email,
            "password_hash": hashed,
            "role": req.role,
        },
    )
    db.commit()
    return {"message": "User berhasil didaftarkan"}


@router.post("/login")
def login(req: LoginRequest, db: Session = Depends(get_db)):
    # Cari User
    user = db.execute(
        text("SELECT * FROM users WHERE email = :email AND is_active = TRUE"),
        {"email": req.email},
    ).fetchone()

    if not user or not verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Email atau Password Salah")

    user_dict = dict(user._mapping)

    # Update last_login
    db.execute(
        text("UPDATE users SET last_login = NOW() WHERE id = :id"),
        {"id": user_dict["id"]},
    )
    db.commit()

    # Generate Tokens
    access_token = create_access_token(user_dict)
    refresh_token = create_refresh_token(user_dict["id"])

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "user": {
            "id": user_dict["id"],
            "nama": user_dict["nama"],
            "email": user_dict["email"],
            "role": user_dict["role"],
            "foto": user_dict.get("foto", ""),
        },
    }


@router.get("/me")
def get_me(
    current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)
):
    user = db.execute(
        text(
            "SELECT id, nama, email, role, foto, last_login FROM users WHERE id = :id"
        ),
        {"id": current_user["sub"]},
    ).fetchone()

    if not user:
        raise HTTPException(status_code=404, detail="User Tidak ditemukan")

    return dict(user._mapping)


@router.post("/refresh")
def refresh_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Bukan Refresh token")

        user = db.execute(
            text("SELECT * FROM user WHERE id = :id AND is_active = TRUE"),
            {"id": payload["sub"]},
        ).fetchone()

        if not user:
            raise HTTPException(status=401, detail="User Tidak Ditemukan")

        user_dict = dict(user._mapping)
        new_access_token = create_access_token(user_dict)

        return {"access_token": new_access_token, "token_type": "bearer"}

    except JWTError:
        raise HTTPException(status_code=401, detail="Refresh Token Expired")
