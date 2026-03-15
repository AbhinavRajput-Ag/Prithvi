import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, validator

load_dotenv(Path(__file__).with_name(".env"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN_TTL_HOURS = 24

app = FastAPI(
    title="Prithvi API",
    version="1.3",
    description="India's Agricultural Operating System — Crop Ledger Backend",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def database_is_ready():
    try:
        with closing(get_connection()) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                cursor.fetchone()
        return True
    except Exception as exc:
        logger.error("Database health check failed: %s", exc)
        return False


def handle_db_error(exc, conn=None, cursor=None):
    if conn:
        conn.rollback()
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    error = str(exc)
    if "duplicate key" in error:
        return {"error": "This record already exists — duplicate entry"}
    if "foreign key" in error:
        return {"error": "Referenced ID does not exist — check linked IDs"}
    if "not-null" in error:
        return {"error": "A required field is missing"}
    logger.error("Database error: %s", error)
    return {"error": "Something went wrong. Please try again."}


def to_day_count(value):
    return value.days if hasattr(value, "days") else int(value)


def stage_rank(stage):
    order = {"sowing": 1, "growing": 2, "harvest": 3, "logistics": 4, "storage": 5}
    return order.get(stage, 0)


def get_auth_secret():
    return os.getenv("AUTH_SECRET", "prithvi-dev-secret-change-me")


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100000)
    return f"{salt}${digest.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt, expected = stored_hash.split("$", 1)
    except ValueError:
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100000)
    return hmac.compare_digest(digest.hex(), expected)


def encode_segment(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("utf-8").rstrip("=")


def decode_segment(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def create_access_token(user: dict) -> str:
    payload = {
        "user_id": user["id"],
        "username": user["username"],
        "role": user["role"],
        "farmer_id": user["farmer_id"],
        "exp": int((datetime.now(timezone.utc) + timedelta(hours=TOKEN_TTL_HOURS)).timestamp()),
    }
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_b64 = encode_segment(payload_json)
    signature = hmac.new(get_auth_secret().encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_b64}.{encode_segment(signature)}"


def decode_access_token(token: str) -> dict:
    try:
        payload_b64, signature_b64 = token.split(".", 1)
        expected_signature = hmac.new(
            get_auth_secret().encode("utf-8"),
            payload_b64.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(expected_signature, decode_segment(signature_b64)):
            raise ValueError("Invalid token")
        payload = json.loads(decode_segment(payload_b64).decode("utf-8"))
        if payload["exp"] < int(datetime.now(timezone.utc).timestamp()):
            raise ValueError("Expired token")
        return payload
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token") from exc


def get_current_user(authorization: Optional[str] = Header(default=None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization required")
    claims = decode_access_token(authorization.split(" ", 1)[1].strip())
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, username, role, farmer_id FROM app_users WHERE id = %s;", (claims["user_id"],))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return dict(user)


def ensure_admin(user: dict):
    if user["role"] != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")


def ensure_farmer_access(user: dict, farmer_id: int):
    if user["role"] == "admin":
        return
    if user["farmer_id"] != farmer_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You can only access your own farmer data")


def get_farmer_id_by_name(name: str) -> Optional[int]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM farmers WHERE name = %s;", (name,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def get_crop_owner_id(crop_id: int) -> Optional[int]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT farmer_id FROM crops WHERE id = %s;", (crop_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def ensure_crop_access(user: dict, crop_id: int):
    farmer_id = get_crop_owner_id(crop_id)
    if farmer_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Crop not found")
    ensure_farmer_access(user, farmer_id)


def get_parcel_owner_id(parcel_id: int) -> Optional[int]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT farmer_id FROM land_parcels WHERE id = %s;", (parcel_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def ensure_parcel_access(user: dict, parcel_id: int):
    farmer_id = get_parcel_owner_id(parcel_id)
    if farmer_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parcel not found")
    ensure_farmer_access(user, farmer_id)


def get_deal_owner_id(deal_id: int) -> Optional[int]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT c.farmer_id FROM deals d JOIN crops c ON c.id = d.crop_id WHERE d.id = %s;", (deal_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def ensure_deal_access(user: dict, deal_id: int):
    farmer_id = get_deal_owner_id(deal_id)
    if farmer_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deal not found")
    ensure_farmer_access(user, farmer_id)


def get_payment_status(amount_received: float, gross_amount: float) -> str:
    if amount_received <= 0:
        return "pending"
    if amount_received < gross_amount:
        return "partial"
    return "paid"


class NewFarmer(BaseModel):
    name: str
    phone: str
    alternate_phone: Optional[str] = None
    village: str
    gram_panchayat: Optional[str] = None
    tehsil: Optional[str] = None
    district: str
    state: str
    postal_code: Optional[str] = None
    education_level: Optional[str] = None
    farming_experience_years: Optional[int] = None
    land_acres: float
    kyc_status: str = "pending"
    consent_status: str = "pending"

    @validator("name")
    def farmer_name(cls, value):
        if not value.strip():
            raise ValueError("Name cannot be empty")
        return value.strip()

    @validator("phone")
    def farmer_phone(cls, value):
        clean = value.strip()
        if not clean.isdigit() or len(clean) != 10:
            raise ValueError("Phone must be exactly 10 digits")
        return clean

    @validator("alternate_phone")
    def farmer_alt_phone(cls, value):
        if value is None or value == "":
            return None
        clean = value.strip()
        if not clean.isdigit() or len(clean) != 10:
            raise ValueError("Alternate phone must be exactly 10 digits")
        return clean

    @validator("land_acres")
    def farmer_land(cls, value):
        if value <= 0:
            raise ValueError("Land acres must be greater than zero")
        return value

    @validator("farming_experience_years")
    def farmer_experience(cls, value):
        if value is not None and value < 0:
            raise ValueError("Farming experience years cannot be negative")
        return value

    @validator("kyc_status", "consent_status")
    def farmer_statuses(cls, value):
        normalized = value.lower().strip()
        if normalized not in ["pending", "verified", "rejected", "granted", "revoked"]:
            raise ValueError("Status value is invalid")
        return normalized


class NewCrop(BaseModel):
    farmer_id: int
    parcel_id: Optional[int] = None
    seed_source_id: Optional[int] = None
    seed_brand: Optional[str] = None
    seed_lot_number: Optional[str] = None
    crop_type: str
    variety: str
    season: str
    year: int
    area_acres: float
    sowing_method: Optional[str] = None
    sowing_date: str
    expected_harvest: str
    expected_yield_quintal: float
    crop_status: str = "planned"

    @validator("season")
    def crop_season(cls, value):
        if value.lower() not in ["kharif", "rabi", "zaid"]:
            raise ValueError("Season must be kharif, rabi, or zaid")
        return value.lower()

    @validator("year")
    def crop_year(cls, value):
        if value < 2000 or value > 2100:
            raise ValueError("Year must be between 2000 and 2100")
        return value

    @validator("area_acres", "expected_yield_quintal")
    def crop_positive(cls, value):
        if value <= 0:
            raise ValueError("Value must be greater than zero")
        return value

    @validator("crop_type", "variety")
    def crop_text(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty")
        return value.strip()

    @validator("crop_status")
    def crop_status_value(cls, value):
        normalized = value.lower().strip()
        if normalized not in ["planned", "active", "completed", "cancelled"]:
            raise ValueError("Crop status must be planned, active, completed, or cancelled")
        return normalized


class NewParcel(BaseModel):
    farmer_id: int
    plot_name: str
    parcel_code: Optional[str] = None
    area_acres: float
    cultivable_area_acres: Optional[float] = None
    irrigated_area_acres: Optional[float] = None
    location: Optional[str] = None
    village: Optional[str] = None
    tehsil: Optional[str] = None
    survey_number: Optional[str] = None
    soil_type: Optional[str] = None
    irrigation_source: Optional[str] = None
    ownership_type: str = "owned"
    road_access: Optional[bool] = None
    power_access: Optional[bool] = None
    fencing_status: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @validator("plot_name")
    def parcel_name(cls, value):
        if not value.strip():
            raise ValueError("Plot name cannot be empty")
        return value.strip()

    @validator("area_acres")
    def parcel_area(cls, value):
        if value <= 0:
            raise ValueError("Area acres must be greater than zero")
        return value

    @validator("cultivable_area_acres", "irrigated_area_acres")
    def parcel_sub_areas(cls, value):
        if value is not None and value < 0:
            raise ValueError("Area values cannot be negative")
        return value

    @validator("ownership_type")
    def parcel_ownership(cls, value):
        normalized = value.lower().strip()
        if normalized not in ["owned", "leased", "shared"]:
            raise ValueError("Ownership type must be owned, leased, or shared")
        return normalized


class NewFarmerMember(BaseModel):
    farmer_id: int
    name: str
    relation: str
    age: Optional[int] = None
    gender: Optional[str] = None
    phone: Optional[str] = None
    role_in_agriculture: str
    primary_operator: bool = False
    decision_maker: bool = False

    @validator("name", "relation", "role_in_agriculture")
    def member_text(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty")
        return value.strip()

    @validator("age")
    def member_age(cls, value):
        if value is not None and value <= 0:
            raise ValueError("Age must be greater than zero")
        return value

    @validator("phone")
    def member_phone(cls, value):
        if value is None or value == "":
            return None
        clean = value.strip()
        if not clean.isdigit() or len(clean) != 10:
            raise ValueError("Phone must be exactly 10 digits")
        return clean


class NewSoilTest(BaseModel):
    farmer_id: int
    parcel_id: int
    sample_date: str
    lab_name: Optional[str] = None
    report_number: Optional[str] = None
    ph: Optional[float] = None
    organic_carbon: Optional[float] = None
    nitrogen: Optional[float] = None
    phosphorus: Optional[float] = None
    potassium: Optional[float] = None
    recommendation_summary: Optional[str] = None


class NewFarmerDocument(BaseModel):
    farmer_id: int
    parcel_id: Optional[int] = None
    crop_id: Optional[int] = None
    document_type: str
    document_number: Optional[str] = None
    issued_by: Optional[str] = None
    issue_date: Optional[str] = None
    expiry_date: Optional[str] = None
    verification_status: str = "pending"
    file_url: Optional[str] = None
    notes: Optional[str] = None

    @validator("document_type")
    def document_type_value(cls, value):
        if not value.strip():
            raise ValueError("Document type cannot be empty")
        return value.strip()

    @validator("verification_status")
    def document_status(cls, value):
        normalized = value.lower().strip()
        if normalized not in ["pending", "verified", "rejected"]:
            raise ValueError("Verification status must be pending, verified, or rejected")
        return normalized


class NewBuyerRegistry(BaseModel):
    name: str
    buyer_type: str
    contact_person: Optional[str] = None
    phone: Optional[str] = None
    location: Optional[str] = None
    district: Optional[str] = None
    state: Optional[str] = None
    payment_reliability_score: Optional[float] = None
    notes: Optional[str] = None

    @validator("name", "buyer_type")
    def buyer_text(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty")
        return value.strip()


class NewInputSupplier(BaseModel):
    name: str
    supplier_type: str
    contact_person: Optional[str] = None
    phone: Optional[str] = None
    location: Optional[str] = None
    district: Optional[str] = None
    state: Optional[str] = None
    gst_number: Optional[str] = None
    notes: Optional[str] = None

    @validator("name", "supplier_type")
    def supplier_text(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty")
        return value.strip()


class YieldEstimateRevisionEntry(BaseModel):
    crop_id: int
    revision_date: str
    new_estimate_quintal: float
    revision_reason: Optional[str] = None
    source: Optional[str] = None
    notes: Optional[str] = None

    @validator("new_estimate_quintal")
    def revision_yield_positive(cls, value):
        if value <= 0:
            raise ValueError("New estimate must be greater than zero")
        return value


class ExpenseReceiptEntry(BaseModel):
    farmer_id: int
    crop_id: Optional[int] = None
    input_cost_id: Optional[int] = None
    supplier_id: Optional[int] = None
    receipt_number: Optional[str] = None
    receipt_date: str
    amount: float
    payment_mode: Optional[str] = None
    file_url: Optional[str] = None
    verification_status: str = "pending"
    notes: Optional[str] = None

    @validator("amount")
    def receipt_amount_positive(cls, value):
        if value <= 0:
            raise ValueError("Amount must be greater than zero")
        return value

    @validator("verification_status")
    def receipt_status(cls, value):
        normalized = value.lower().strip()
        if normalized not in ["pending", "verified", "rejected"]:
            raise ValueError("Verification status must be pending, verified, or rejected")
        return normalized


class WeatherSnapshotEntry(BaseModel):
    farmer_id: int
    parcel_id: Optional[int] = None
    crop_id: Optional[int] = None
    source_name: str
    snapshot_time: str
    forecast_window_hours: Optional[int] = None
    rainfall_mm: Optional[float] = None
    temperature_min_c: Optional[float] = None
    temperature_max_c: Optional[float] = None
    humidity_percent: Optional[float] = None
    wind_speed_kmph: Optional[float] = None
    solar_radiation: Optional[float] = None
    heat_risk: Optional[str] = None
    flood_risk: Optional[str] = None
    drought_risk: Optional[str] = None
    hail_risk: Optional[str] = None
    lightning_risk: Optional[str] = None
    raw_payload: Optional[str] = None

    @validator("source_name")
    def weather_source(cls, value):
        if not value.strip():
            raise ValueError("Source name cannot be empty")
        return value.strip()

    @validator("forecast_window_hours")
    def weather_window(cls, value):
        if value is not None and value < 0:
            raise ValueError("Forecast window cannot be negative")
        return value

    @validator(
        "rainfall_mm",
        "humidity_percent",
        "wind_speed_kmph",
        "solar_radiation",
    )
    def weather_non_negative(cls, value):
        if value is not None and value < 0:
            raise ValueError("Value cannot be negative")
        return value


class MandiPriceSnapshotEntry(BaseModel):
    crop_type: str
    variety: Optional[str] = None
    market_name: str
    district: Optional[str] = None
    state: Optional[str] = None
    snapshot_date: str
    min_price: Optional[float] = None
    modal_price: Optional[float] = None
    max_price: Optional[float] = None
    arrival_quantity: Optional[float] = None
    source_name: str
    raw_payload: Optional[str] = None

    @validator("crop_type", "market_name", "source_name")
    def mandi_required_text(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty")
        return value.strip()

    @validator("min_price", "modal_price", "max_price", "arrival_quantity")
    def mandi_non_negative(cls, value):
        if value is not None and value < 0:
            raise ValueError("Value cannot be negative")
        return value


class RiskAlertEventEntry(BaseModel):
    farmer_id: int
    parcel_id: Optional[int] = None
    crop_id: Optional[int] = None
    alert_type: str
    severity: str
    detected_at: str
    source_type: str
    confidence_score: Optional[float] = None
    title: str
    message: str
    recommended_action: Optional[str] = None
    status: str = "open"
    resolved_at: Optional[str] = None

    @validator("alert_type", "source_type", "title", "message")
    def risk_text(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty")
        return value.strip()

    @validator("severity")
    def risk_severity(cls, value):
        normalized = value.lower().strip()
        if normalized not in ["low", "medium", "high", "critical"]:
            raise ValueError("Severity must be low, medium, high, or critical")
        return normalized

    @validator("status")
    def risk_status(cls, value):
        normalized = value.lower().strip()
        if normalized not in ["open", "reviewed", "resolved"]:
            raise ValueError("Status must be open, reviewed, or resolved")
        return normalized

    @validator("confidence_score")
    def risk_confidence(cls, value):
        if value is not None and (value < 0 or value > 100):
            raise ValueError("Confidence score must be between 0 and 100")
        return value


class NewCost(BaseModel):
    crop_id: int
    stage: str
    category: Optional[str] = None
    supplier_id: Optional[int] = None
    item_name: str
    invoice_number: Optional[str] = None
    quantity: float
    unit: str
    amount: float
    transaction_mode: Optional[str] = None
    transaction_reference: Optional[str] = None
    transaction_date: Optional[str] = None
    subsidized: bool = False
    receipt_id: Optional[int] = None

    @validator("stage")
    def cost_stage(cls, value):
        valid = ["sowing", "growing", "harvest", "logistics", "storage"]
        if value.lower() not in valid:
            raise ValueError(f"Stage must be one of: {valid}")
        return value.lower()

    @validator("quantity", "amount")
    def cost_positive(cls, value):
        if value <= 0:
            raise ValueError("Value must be greater than zero")
        return value

    @validator("item_name", "unit")
    def cost_text(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty")
        return value.strip()


class HarvestEntry(BaseModel):
    crop_id: int
    harvest_date: str
    yield_quintal: float
    yield_rejected_quintal: float = 0
    quality_grade: Optional[str] = None
    moisture_percent: Optional[float] = None
    bags_count: Optional[int] = None
    storage_location: Optional[str] = None
    selling_price: float
    buyer: str

    @validator("yield_quintal", "selling_price")
    def harvest_positive(cls, value):
        if value <= 0:
            raise ValueError("Value must be greater than zero")
        return value

    @validator("yield_rejected_quintal", "moisture_percent")
    def harvest_optional_non_negative(cls, value):
        if value is not None and value < 0:
            raise ValueError("Value cannot be negative")
        return value

    @validator("bags_count")
    def harvest_bags(cls, value):
        if value is not None and value < 0:
            raise ValueError("Bags count cannot be negative")
        return value

    @validator("buyer")
    def harvest_buyer(cls, value):
        if not value.strip():
            raise ValueError("Buyer cannot be empty")
        return value.strip()


class CropStageUpdate(BaseModel):
    stage: str

    @validator("stage")
    def stage_value(cls, value):
        valid = ["sowing", "growing", "harvest", "logistics", "storage"]
        if value.lower() not in valid:
            raise ValueError(f"Stage must be one of {valid}")
        return value.lower()


class CropYieldUpdate(BaseModel):
    expected_yield_quintal: float

    @validator("expected_yield_quintal")
    def yield_value(cls, value):
        if value <= 0:
            raise ValueError("Expected yield must be greater than zero")
        return value


class UserRegister(BaseModel):
    username: str
    password: str
    role: str = "farmer"
    farmer_id: Optional[int] = None

    @validator("username")
    def user_name(cls, value):
        if not value.strip():
            raise ValueError("Username cannot be empty")
        return value.strip()

    @validator("password")
    def user_password(cls, value):
        if len(value) < 8:
            raise ValueError("Password must be at least 8 characters")
        return value

    @validator("role")
    def user_role(cls, value):
        if value.lower() not in ["admin", "farmer"]:
            raise ValueError("Role must be admin or farmer")
        return value.lower()


class UserLogin(BaseModel):
    username: str
    password: str


class DealEntry(BaseModel):
    crop_id: int
    buyer_id: Optional[int] = None
    buyer_type: Optional[str] = None
    sale_date: str
    quantity_quintal: float
    price_per_quintal: float
    buyer: str
    deductions_amount: float = 0
    transport_cost: float = 0
    mandi_fee: float = 0
    amount_received: float = 0
    payment_terms: Optional[str] = None
    due_date: Optional[str] = None
    notes: Optional[str] = None

    @validator("quantity_quintal", "price_per_quintal")
    def deal_positive(cls, value):
        if value <= 0:
            raise ValueError("Value must be greater than zero")
        return value

    @validator("amount_received", "deductions_amount", "transport_cost", "mandi_fee")
    def deal_received(cls, value):
        if value < 0:
            raise ValueError("Amount received cannot be negative")
        return value

    @validator("buyer")
    def deal_buyer(cls, value):
        if not value.strip():
            raise ValueError("Buyer cannot be empty")
        return value.strip()


class DealPaymentUpdate(BaseModel):
    amount_received: float

    @validator("amount_received")
    def payment_value(cls, value):
        if value < 0:
            raise ValueError("Amount received cannot be negative")
        return value


@app.get("/")
def home():
    return {
        "system": "Prithvi",
        "version": "1.3",
        "message": "India's Agricultural Operating System",
        "database_ready": database_is_ready(),
    }


@app.get("/health")
def health_check():
    ready = database_is_ready()
    return {"status": "ok" if ready else "degraded", "database_ready": ready}


@app.post("/auth/register")
def register_user(payload: UserRegister):
    if payload.role == "farmer" and payload.farmer_id is None:
        return {"error": "farmer_id is required for farmer users"}
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if payload.farmer_id is not None:
            cursor.execute("SELECT id FROM farmers WHERE id = %s;", (payload.farmer_id,))
            if cursor.fetchone() is None:
                cursor.close()
                conn.close()
                return {"error": "Linked farmer not found"}
        cursor.execute(
            """
            INSERT INTO app_users (username, password_hash, role, farmer_id)
            VALUES (%s, %s, %s, %s)
            RETURNING id, username, role, farmer_id;
            """,
            (payload.username, hash_password(payload.password), payload.role, payload.farmer_id),
        )
        user = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "User registered", "user": dict(user)}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/auth/login")
def login_user(payload: UserLogin):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT id, username, password_hash, role, farmer_id
        FROM app_users
        WHERE username = %s;
        """,
        (payload.username,),
    )
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if user is None or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")
    token = create_access_token(dict(user))
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user["id"],
            "username": user["username"],
            "role": user["role"],
            "farmer_id": user["farmer_id"],
        },
    }


@app.get("/auth/me")
def auth_me(user: dict = Depends(get_current_user)):
    return {"user": user}


@app.get("/farmers")
def get_all_farmers(user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            f.id,
            f.name,
            f.village,
            f.land_acres,
            COUNT(lp.id) AS parcel_count
        FROM farmers f
        LEFT JOIN land_parcels lp ON lp.farmer_id = f.id
        GROUP BY f.id, f.name, f.village, f.land_acres
        ORDER BY f.id;
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "farmers": [
            {
                "id": row[0],
                "name": row[1],
                "village": row[2],
                "land_acres": float(row[3]),
                "parcel_count": row[4],
            }
            for row in rows
        ]
    }


@app.get("/dashboard/summary")
def get_dashboard_summary(user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        WITH crop_costs AS (
            SELECT crop_id, SUM(amount) AS total_cost
            FROM input_costs GROUP BY crop_id
        ),
        crop_harvests AS (
            SELECT crop_id, SUM(revenue) AS total_revenue, SUM(yield_quintal) AS total_yield
            FROM harvests GROUP BY crop_id
        )
        SELECT
            (SELECT COUNT(*) FROM farmers) AS total_farmers,
            (SELECT COUNT(*) FROM crops) AS total_crops,
            (SELECT COUNT(*) FROM crops WHERE current_stage IN ('sowing', 'growing', 'harvest')) AS active_crops,
            (SELECT COUNT(*) FROM harvests) AS harvest_entries,
            COALESCE(SUM(crop_costs.total_cost), 0) AS total_cost,
            COALESCE(SUM(crop_harvests.total_revenue), 0) AS total_revenue,
            COALESCE(SUM(crop_harvests.total_yield), 0) AS total_yield_quintal
        FROM crops
        LEFT JOIN crop_costs ON crop_costs.crop_id = crops.id
        LEFT JOIN crop_harvests ON crop_harvests.crop_id = crops.id;
        """
    )
    summary = cursor.fetchone()
    cursor.execute("SELECT current_stage AS stage, COUNT(*) AS crop_count FROM crops GROUP BY current_stage ORDER BY current_stage;")
    stage_distribution = cursor.fetchall()
    cursor.execute(
        """
        SELECT c.id AS crop_id, f.name AS farmer, c.crop_type, c.expected_harvest,
               (c.expected_harvest - CURRENT_DATE) AS days_to_harvest
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        WHERE c.expected_harvest IS NOT NULL
          AND c.expected_harvest BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '15 days'
        ORDER BY c.expected_harvest, c.id;
        """
    )
    upcoming = cursor.fetchall()
    cursor.close()
    conn.close()
    total_cost = float(summary["total_cost"] or 0)
    total_revenue = float(summary["total_revenue"] or 0)
    return {
        "totals": {
            "farmers": summary["total_farmers"],
            "crops": summary["total_crops"],
            "active_crops": summary["active_crops"],
            "harvest_entries": summary["harvest_entries"],
            "cost": total_cost,
            "revenue": total_revenue,
            "profit": total_revenue - total_cost,
            "yield_quintal": float(summary["total_yield_quintal"] or 0),
        },
        "stage_distribution": [{"stage": row["stage"], "crop_count": row["crop_count"]} for row in stage_distribution],
        "upcoming_harvests": [
            {
                "crop_id": row["crop_id"],
                "farmer": row["farmer"],
                "crop": row["crop_type"],
                "expected_harvest": str(row["expected_harvest"]),
                "days_to_harvest": to_day_count(row["days_to_harvest"]),
            }
            for row in upcoming
        ],
    }


@app.get("/dashboard/fpo-summary")
def get_fpo_summary(user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        WITH crop_costs AS (
            SELECT crop_id, SUM(amount) AS total_cost
            FROM input_costs GROUP BY crop_id
        ),
        crop_harvests AS (
            SELECT crop_id, SUM(revenue) AS total_revenue, SUM(yield_quintal) AS total_yield
            FROM harvests GROUP BY crop_id
        ),
        farmer_rollup AS (
            SELECT
                f.id, f.name, f.village,
                COUNT(DISTINCT c.id) AS crop_count,
                COALESCE(SUM(crop_costs.total_cost), 0) AS total_cost,
                COALESCE(SUM(crop_harvests.total_revenue), 0) AS total_revenue,
                COALESCE(SUM(crop_harvests.total_yield), 0) AS total_yield
            FROM farmers f
            LEFT JOIN crops c ON c.farmer_id = f.id
            LEFT JOIN crop_costs ON crop_costs.crop_id = c.id
            LEFT JOIN crop_harvests ON crop_harvests.crop_id = c.id
            GROUP BY f.id, f.name, f.village
        )
        SELECT id AS farmer_id, name, village, crop_count, total_cost, total_revenue, total_yield,
               (total_revenue - total_cost) AS profit
        FROM farmer_rollup
        ORDER BY total_revenue DESC, total_cost DESC, name;
        """
    )
    farmer_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT
            c.id AS crop_id, f.name AS farmer, c.crop_type, c.current_stage,
            c.expected_harvest, c.expected_yield_quintal,
            COALESCE(costs.total_cost, 0) AS total_cost,
            COALESCE(deals.total_revenue, COALESCE(harvests.total_revenue, 0)) AS revenue
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        LEFT JOIN (SELECT crop_id, SUM(amount) AS total_cost FROM input_costs GROUP BY crop_id) costs ON costs.crop_id = c.id
        LEFT JOIN (SELECT crop_id, SUM(revenue) AS total_revenue FROM harvests GROUP BY crop_id) harvests ON harvests.crop_id = c.id
        LEFT JOIN (SELECT crop_id, SUM(gross_amount) AS total_revenue FROM deals GROUP BY crop_id) deals ON deals.crop_id = c.id
        WHERE c.expected_yield_quintal IS NULL
           OR c.current_stage IN ('sowing', 'growing')
           OR c.expected_harvest BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '15 days'
        ORDER BY c.expected_harvest NULLS LAST, c.id;
        """
    )
    attention_rows = cursor.fetchall()
    cursor.close()
    conn.close()
    farmers = [
        {
            "farmer_id": row["farmer_id"],
            "name": row["name"],
            "village": row["village"],
            "crop_count": row["crop_count"],
            "total_cost": float(row["total_cost"] or 0),
            "total_revenue": float(row["total_revenue"] or 0),
            "total_yield_quintal": float(row["total_yield"] or 0),
            "profit": float(row["profit"] or 0),
        }
        for row in farmer_rows
    ]
    return {
        "portfolio_totals": {
            "farmers": len(farmers),
            "profitable_farmers": sum(1 for row in farmers if row["profit"] > 0),
            "revenue_generating_farmers": sum(1 for row in farmers if row["total_revenue"] > 0),
        },
        "top_revenue_farmers": farmers[:5],
        "attention_required": [
            {
                "crop_id": row["crop_id"],
                "farmer": row["farmer"],
                "crop": row["crop_type"],
                "stage": row["current_stage"],
                "expected_harvest": str(row["expected_harvest"]) if row["expected_harvest"] is not None else None,
                "expected_yield_quintal": float(row["expected_yield_quintal"]) if row["expected_yield_quintal"] is not None else None,
                "total_cost": float(row["total_cost"] or 0),
                "revenue": float(row["revenue"] or 0),
            }
            for row in attention_rows
        ],
    }


@app.get("/alerts/overview")
def get_alerts_overview(user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT c.id AS crop_id, f.name AS farmer, c.crop_type, c.current_stage FROM crops c JOIN farmers f ON f.id = c.farmer_id WHERE c.expected_yield_quintal IS NULL ORDER BY c.id;")
    missing_yield = cursor.fetchall()
    cursor.execute("SELECT c.id AS crop_id, f.name AS farmer, c.crop_type, c.current_stage FROM crops c JOIN farmers f ON f.id = c.farmer_id LEFT JOIN input_costs i ON i.crop_id = c.id WHERE i.id IS NULL ORDER BY c.id;")
    missing_costs = cursor.fetchall()
    cursor.execute(
        """
        SELECT c.id AS crop_id, f.name AS farmer, c.crop_type, c.expected_harvest,
               (c.expected_harvest - CURRENT_DATE) AS days_to_harvest
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        WHERE c.expected_harvest IS NOT NULL
          AND c.expected_harvest BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '15 days'
        ORDER BY c.expected_harvest, c.id;
        """
    )
    upcoming = cursor.fetchall()
    cursor.execute(
        """
        SELECT ra.id, ra.crop_id, f.name AS farmer, c.crop_type, ra.alert_type, ra.severity,
               ra.title, ra.status, ra.detected_at
        FROM risk_alert_events ra
        LEFT JOIN farmers f ON f.id = ra.farmer_id
        LEFT JOIN crops c ON c.id = ra.crop_id
        WHERE ra.status != 'resolved'
        ORDER BY
            CASE ra.severity
                WHEN 'critical' THEN 1
                WHEN 'high' THEN 2
                WHEN 'medium' THEN 3
                ELSE 4
            END,
            ra.detected_at DESC;
        """
    )
    risk_alerts = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "missing_expected_yield": missing_yield,
        "missing_cost_entries": missing_costs,
        "upcoming_harvests": [
            {
                "crop_id": row["crop_id"],
                "farmer": row["farmer"],
                "crop": row["crop_type"],
                "expected_harvest": str(row["expected_harvest"]),
                "days_to_harvest": to_day_count(row["days_to_harvest"]),
            }
            for row in upcoming
        ],
        "system_risk_alerts": [
            {
                "risk_alert_id": row["id"],
                "crop_id": row["crop_id"],
                "farmer": row["farmer"],
                "crop": row["crop_type"],
                "alert_type": row["alert_type"],
                "severity": row["severity"],
                "title": row["title"],
                "status": row["status"],
                "detected_at": str(row["detected_at"]),
            }
            for row in risk_alerts
        ],
    }


@app.get("/weather-snapshots")
def get_weather_snapshots(
    farmer_id: Optional[int] = None,
    parcel_id: Optional[int] = None,
    crop_id: Optional[int] = None,
    user: dict = Depends(get_current_user),
):
    if crop_id is not None:
        ensure_crop_access(user, crop_id)
    elif parcel_id is not None:
        ensure_parcel_access(user, parcel_id)
    elif farmer_id is not None:
        ensure_farmer_access(user, farmer_id)
    elif user["role"] != "admin":
        farmer_id = user["farmer_id"]

    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT
            id, farmer_id, parcel_id, crop_id, source_name, snapshot_time, forecast_window_hours,
            rainfall_mm, temperature_min_c, temperature_max_c, humidity_percent, wind_speed_kmph,
            solar_radiation, heat_risk, flood_risk, drought_risk, hail_risk, lightning_risk, raw_payload
        FROM weather_snapshots
    """
    params = ()
    if crop_id is not None:
        query += " WHERE crop_id = %s"
        params = (crop_id,)
    elif parcel_id is not None:
        query += " WHERE parcel_id = %s"
        params = (parcel_id,)
    elif farmer_id is not None:
        query += " WHERE farmer_id = %s"
        params = (farmer_id,)
    query += " ORDER BY snapshot_time DESC, id DESC;"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "weather_snapshots": [
            {
                "weather_snapshot_id": row[0],
                "farmer_id": row[1],
                "parcel_id": row[2],
                "crop_id": row[3],
                "source_name": row[4],
                "snapshot_time": str(row[5]),
                "forecast_window_hours": row[6],
                "rainfall_mm": float(row[7]) if row[7] is not None else None,
                "temperature_min_c": float(row[8]) if row[8] is not None else None,
                "temperature_max_c": float(row[9]) if row[9] is not None else None,
                "humidity_percent": float(row[10]) if row[10] is not None else None,
                "wind_speed_kmph": float(row[11]) if row[11] is not None else None,
                "solar_radiation": float(row[12]) if row[12] is not None else None,
                "heat_risk": row[13],
                "flood_risk": row[14],
                "drought_risk": row[15],
                "hail_risk": row[16],
                "lightning_risk": row[17],
                "raw_payload": row[18],
            }
            for row in rows
        ]
    }


@app.get("/mandi-prices")
def get_mandi_price_snapshots(
    crop_type: Optional[str] = None,
    market_name: Optional[str] = None,
    user: dict = Depends(get_current_user),
):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT
            id, crop_type, variety, market_name, district, state, snapshot_date,
            min_price, modal_price, max_price, arrival_quantity, source_name, raw_payload
        FROM mandi_price_snapshots
    """
    params = []
    clauses = []
    if crop_type:
        clauses.append("crop_type = %s")
        params.append(crop_type.strip())
    if market_name:
        clauses.append("market_name = %s")
        params.append(market_name.strip())
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY snapshot_date DESC, id DESC;"
    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "mandi_price_snapshots": [
            {
                "mandi_price_snapshot_id": row[0],
                "crop_type": row[1],
                "variety": row[2],
                "market_name": row[3],
                "district": row[4],
                "state": row[5],
                "snapshot_date": str(row[6]),
                "min_price": float(row[7]) if row[7] is not None else None,
                "modal_price": float(row[8]) if row[8] is not None else None,
                "max_price": float(row[9]) if row[9] is not None else None,
                "arrival_quantity": float(row[10]) if row[10] is not None else None,
                "source_name": row[11],
                "raw_payload": row[12],
            }
            for row in rows
        ]
    }


@app.get("/risk-alerts")
def get_risk_alerts(
    farmer_id: Optional[int] = None,
    parcel_id: Optional[int] = None,
    crop_id: Optional[int] = None,
    status: Optional[str] = None,
    user: dict = Depends(get_current_user),
):
    if crop_id is not None:
        ensure_crop_access(user, crop_id)
    elif parcel_id is not None:
        ensure_parcel_access(user, parcel_id)
    elif farmer_id is not None:
        ensure_farmer_access(user, farmer_id)
    elif user["role"] != "admin":
        farmer_id = user["farmer_id"]

    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT
            id, farmer_id, parcel_id, crop_id, alert_type, severity, detected_at, source_type,
            confidence_score, title, message, recommended_action, status, resolved_at
        FROM risk_alert_events
    """
    clauses = []
    params = []
    if crop_id is not None:
        clauses.append("crop_id = %s")
        params.append(crop_id)
    elif parcel_id is not None:
        clauses.append("parcel_id = %s")
        params.append(parcel_id)
    elif farmer_id is not None:
        clauses.append("farmer_id = %s")
        params.append(farmer_id)
    if status:
        clauses.append("status = %s")
        params.append(status.strip().lower())
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY detected_at DESC, id DESC;"
    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "risk_alerts": [
            {
                "risk_alert_id": row[0],
                "farmer_id": row[1],
                "parcel_id": row[2],
                "crop_id": row[3],
                "alert_type": row[4],
                "severity": row[5],
                "detected_at": str(row[6]),
                "source_type": row[7],
                "confidence_score": float(row[8]) if row[8] is not None else None,
                "title": row[9],
                "message": row[10],
                "recommended_action": row[11],
                "status": row[12],
                "resolved_at": str(row[13]) if row[13] is not None else None,
            }
            for row in rows
        ]
    }


@app.get("/parcels")
def get_parcels(farmer_id: Optional[int] = None, user: dict = Depends(get_current_user)):
    if farmer_id is not None:
        ensure_farmer_access(user, farmer_id)
    elif user["role"] != "admin":
        farmer_id = user["farmer_id"]

    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT
            lp.id, lp.farmer_id, f.name, lp.plot_name, lp.parcel_code, lp.area_acres,
            lp.cultivable_area_acres, lp.irrigated_area_acres, lp.location, lp.village, lp.tehsil,
            lp.survey_number, lp.soil_type, lp.irrigation_source, lp.ownership_type,
            lp.road_access, lp.power_access, lp.fencing_status, lp.latitude, lp.longitude
        FROM land_parcels lp
        JOIN farmers f ON f.id = lp.farmer_id
    """
    params = ()
    if farmer_id is not None:
        query += " WHERE lp.farmer_id = %s"
        params = (farmer_id,)
    query += " ORDER BY lp.id;"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "parcels": [
            {
                "parcel_id": row[0],
                "farmer_id": row[1],
                "farmer": row[2],
                "plot_name": row[3],
                "parcel_code": row[4],
                "area_acres": float(row[5]),
                "cultivable_area_acres": float(row[6]) if row[6] is not None else None,
                "irrigated_area_acres": float(row[7]) if row[7] is not None else None,
                "location": row[8],
                "village": row[9],
                "tehsil": row[10],
                "survey_number": row[11],
                "soil_type": row[12],
                "irrigation_source": row[13],
                "ownership_type": row[14],
                "road_access": row[15],
                "power_access": row[16],
                "fencing_status": row[17],
                "latitude": float(row[18]) if row[18] is not None else None,
                "longitude": float(row[19]) if row[19] is not None else None,
            }
            for row in rows
        ]
    }


@app.get("/parcels/{parcel_id}")
def get_parcel(parcel_id: int, user: dict = Depends(get_current_user)):
    ensure_parcel_access(user, parcel_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            lp.id, lp.farmer_id, f.name, lp.plot_name, lp.parcel_code, lp.area_acres,
            lp.cultivable_area_acres, lp.irrigated_area_acres, lp.location, lp.village, lp.tehsil,
            lp.survey_number, lp.soil_type, lp.irrigation_source, lp.ownership_type,
            lp.road_access, lp.power_access, lp.fencing_status, lp.latitude, lp.longitude
        FROM land_parcels lp
        JOIN farmers f ON f.id = lp.farmer_id
        WHERE lp.id = %s;
        """,
        (parcel_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": "Parcel not found"}
    return {
        "parcel_id": row[0],
        "farmer_id": row[1],
        "farmer": row[2],
        "plot_name": row[3],
        "parcel_code": row[4],
        "area_acres": float(row[5]),
        "cultivable_area_acres": float(row[6]) if row[6] is not None else None,
        "irrigated_area_acres": float(row[7]) if row[7] is not None else None,
        "location": row[8],
        "village": row[9],
        "tehsil": row[10],
        "survey_number": row[11],
        "soil_type": row[12],
        "irrigation_source": row[13],
        "ownership_type": row[14],
        "road_access": row[15],
        "power_access": row[16],
        "fencing_status": row[17],
        "latitude": float(row[18]) if row[18] is not None else None,
        "longitude": float(row[19]) if row[19] is not None else None,
    }


@app.get("/farmer-members")
def get_farmer_members(farmer_id: Optional[int] = None, user: dict = Depends(get_current_user)):
    if farmer_id is not None:
        ensure_farmer_access(user, farmer_id)
    elif user["role"] != "admin":
        farmer_id = user["farmer_id"]

    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT id, farmer_id, name, relation, age, gender, phone,
               role_in_agriculture, primary_operator, decision_maker
        FROM farmer_members
    """
    params = ()
    if farmer_id is not None:
        query += " WHERE farmer_id = %s"
        params = (farmer_id,)
    query += " ORDER BY id;"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "members": [
            {
                "member_id": row[0],
                "farmer_id": row[1],
                "name": row[2],
                "relation": row[3],
                "age": row[4],
                "gender": row[5],
                "phone": row[6],
                "role_in_agriculture": row[7],
                "primary_operator": row[8],
                "decision_maker": row[9],
            }
            for row in rows
        ]
    }


@app.get("/soil-tests")
def get_soil_tests(parcel_id: Optional[int] = None, farmer_id: Optional[int] = None, user: dict = Depends(get_current_user)):
    if parcel_id is not None:
        ensure_parcel_access(user, parcel_id)
    elif farmer_id is not None:
        ensure_farmer_access(user, farmer_id)
    elif user["role"] != "admin":
        farmer_id = user["farmer_id"]

    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT
            st.id, st.farmer_id, st.parcel_id, lp.plot_name, st.sample_date, st.lab_name,
            st.report_number, st.ph, st.organic_carbon, st.nitrogen, st.phosphorus,
            st.potassium, st.recommendation_summary
        FROM soil_tests st
        JOIN land_parcels lp ON lp.id = st.parcel_id
    """
    params = ()
    if parcel_id is not None:
        query += " WHERE st.parcel_id = %s"
        params = (parcel_id,)
    elif farmer_id is not None:
        query += " WHERE st.farmer_id = %s"
        params = (farmer_id,)
    query += " ORDER BY st.sample_date DESC, st.id DESC;"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "soil_tests": [
            {
                "soil_test_id": row[0],
                "farmer_id": row[1],
                "parcel_id": row[2],
                "parcel_name": row[3],
                "sample_date": str(row[4]),
                "lab_name": row[5],
                "report_number": row[6],
                "ph": float(row[7]) if row[7] is not None else None,
                "organic_carbon": float(row[8]) if row[8] is not None else None,
                "nitrogen": float(row[9]) if row[9] is not None else None,
                "phosphorus": float(row[10]) if row[10] is not None else None,
                "potassium": float(row[11]) if row[11] is not None else None,
                "recommendation_summary": row[12],
            }
            for row in rows
        ]
    }


@app.get("/documents")
def get_documents(farmer_id: Optional[int] = None, user: dict = Depends(get_current_user)):
    if farmer_id is not None:
        ensure_farmer_access(user, farmer_id)
    elif user["role"] != "admin":
        farmer_id = user["farmer_id"]

    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT id, farmer_id, parcel_id, crop_id, document_type, document_number,
               issued_by, issue_date, expiry_date, verification_status, file_url, notes
        FROM farmer_documents
    """
    params = ()
    if farmer_id is not None:
        query += " WHERE farmer_id = %s"
        params = (farmer_id,)
    query += " ORDER BY id DESC;"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "documents": [
            {
                "document_id": row[0],
                "farmer_id": row[1],
                "parcel_id": row[2],
                "crop_id": row[3],
                "document_type": row[4],
                "document_number": row[5],
                "issued_by": row[6],
                "issue_date": str(row[7]) if row[7] is not None else None,
                "expiry_date": str(row[8]) if row[8] is not None else None,
                "verification_status": row[9],
                "file_url": row[10],
                "notes": row[11],
            }
            for row in rows
        ]
    }


@app.get("/buyers")
def get_buyers(user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, buyer_type, contact_person, phone, location, district, state,
               payment_reliability_score, notes
        FROM buyer_registry
        ORDER BY id;
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "buyers": [
            {
                "buyer_id": row[0],
                "name": row[1],
                "buyer_type": row[2],
                "contact_person": row[3],
                "phone": row[4],
                "location": row[5],
                "district": row[6],
                "state": row[7],
                "payment_reliability_score": float(row[8]) if row[8] is not None else None,
                "notes": row[9],
            }
            for row in rows
        ]
    }


@app.get("/suppliers")
def get_suppliers(user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, supplier_type, contact_person, phone, location, district, state, gst_number, notes
        FROM input_suppliers
        ORDER BY id;
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "suppliers": [
            {
                "supplier_id": row[0],
                "name": row[1],
                "supplier_type": row[2],
                "contact_person": row[3],
                "phone": row[4],
                "location": row[5],
                "district": row[6],
                "state": row[7],
                "gst_number": row[8],
                "notes": row[9],
            }
            for row in rows
        ]
    }


@app.get("/crop/{crop_id}/yield-revisions")
def get_yield_revisions(crop_id: int, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, revision_date, previous_estimate_quintal, new_estimate_quintal, revision_reason, source, notes
        FROM yield_estimate_revisions
        WHERE crop_id = %s
        ORDER BY revision_date DESC, id DESC;
        """,
        (crop_id,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "crop_id": crop_id,
        "yield_revisions": [
            {
                "revision_id": row[0],
                "revision_date": str(row[1]),
                "previous_estimate_quintal": float(row[2]) if row[2] is not None else None,
                "new_estimate_quintal": float(row[3]),
                "revision_reason": row[4],
                "source": row[5],
                "notes": row[6],
            }
            for row in rows
        ],
    }


@app.get("/receipts")
def get_receipts(farmer_id: Optional[int] = None, crop_id: Optional[int] = None, user: dict = Depends(get_current_user)):
    if crop_id is not None:
        ensure_crop_access(user, crop_id)
    elif farmer_id is not None:
        ensure_farmer_access(user, farmer_id)
    elif user["role"] != "admin":
        farmer_id = user["farmer_id"]

    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT id, farmer_id, crop_id, input_cost_id, supplier_id, receipt_number, receipt_date,
               amount, payment_mode, file_url, verification_status, notes
        FROM expense_receipts
    """
    params = ()
    if crop_id is not None:
        query += " WHERE crop_id = %s"
        params = (crop_id,)
    elif farmer_id is not None:
        query += " WHERE farmer_id = %s"
        params = (farmer_id,)
    query += " ORDER BY receipt_date DESC, id DESC;"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "receipts": [
            {
                "receipt_id": row[0],
                "farmer_id": row[1],
                "crop_id": row[2],
                "input_cost_id": row[3],
                "supplier_id": row[4],
                "receipt_number": row[5],
                "receipt_date": str(row[6]),
                "amount": float(row[7]),
                "payment_mode": row[8],
                "file_url": row[9],
                "verification_status": row[10],
                "notes": row[11],
            }
            for row in rows
        ]
    }


@app.get("/farmer/{name}")
def get_farmer(name: str, user: dict = Depends(get_current_user)):
    farmer_id = get_farmer_id_by_name(name)
    if farmer_id is None:
        return {"error": f"No farmer found: {name}"}
    ensure_farmer_access(user, farmer_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            f.name, f.village, f.tehsil, f.gram_panchayat, f.alternate_phone,
            c.crop_type, c.variety, c.current_stage, COALESCE(SUM(i.amount), 0) AS total_cost
        FROM farmers f
        JOIN crops c ON c.farmer_id = f.id
        LEFT JOIN input_costs i ON i.crop_id = c.id
        WHERE f.name = %s
        GROUP BY f.name, f.village, f.tehsil, f.gram_panchayat, f.alternate_phone, c.crop_type, c.variety, c.current_stage;
        """,
        (name,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": f"No farmer found: {name}"}
    return {
        "farmer": row[0],
        "village": row[1],
        "tehsil": row[2],
        "gram_panchayat": row[3],
        "alternate_phone": row[4],
        "crop": row[5],
        "variety": row[6],
        "stage": row[7],
        "total_cost": float(row[8]),
    }


@app.get("/crop/{crop_id}")
def get_crop(crop_id: int, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            c.id, f.name, lp.id, lp.plot_name, c.seed_source_id, c.seed_brand, c.seed_lot_number,
            c.crop_type, c.variety, c.season, c.year, c.sowing_method, c.sowing_date,
            c.expected_harvest, c.expected_yield_quintal, c.current_stage, c.crop_status,
            COALESCE(SUM(i.amount), 0) AS total_cost
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        LEFT JOIN land_parcels lp ON lp.id = c.parcel_id
        LEFT JOIN input_costs i ON i.crop_id = c.id
        WHERE c.id = %s
        GROUP BY c.id, f.name, lp.id, lp.plot_name, c.seed_source_id, c.seed_brand, c.seed_lot_number,
                 c.crop_type, c.variety, c.season, c.year, c.sowing_method, c.sowing_date,
                 c.expected_harvest, c.expected_yield_quintal, c.current_stage, c.crop_status;
        """,
        (crop_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": "Crop not found"}
    return {
        "crop_id": row[0],
        "farmer": row[1],
        "parcel_id": row[2],
        "parcel_name": row[3],
        "seed_source_id": row[4],
        "seed_brand": row[5],
        "seed_lot_number": row[6],
        "crop": row[7],
        "variety": row[8],
        "season": row[9],
        "year": row[10],
        "sowing_method": row[11],
        "sowing_date": str(row[12]),
        "expected_harvest": str(row[13]),
        "expected_yield_quintal": float(row[14]) if row[14] is not None else None,
        "stage": row[15],
        "crop_status": row[16],
        "total_cost": float(row[17]),
    }


@app.get("/crop/{crop_id}/costs")
def get_crop_costs(crop_id: int, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, stage, item_name, quantity, unit, amount, entry_date FROM input_costs WHERE crop_id = %s ORDER BY id;", (crop_id,))
    rows = cursor.fetchall()
    if not rows:
        cursor.close()
        conn.close()
        return {"crop_id": crop_id, "costs": [], "stage_totals": [], "total_cost": 0.0}
    cursor.execute("SELECT stage, SUM(amount) AS stage_total FROM input_costs WHERE crop_id = %s GROUP BY stage ORDER BY stage;", (crop_id,))
    stage_rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "crop_id": crop_id,
        "costs": [
            {"cost_id": row[0], "stage": row[1], "item_name": row[2], "quantity": float(row[3]), "unit": row[4], "amount": float(row[5]), "entry_date": str(row[6])}
            for row in rows
        ],
        "stage_totals": [{"stage": row[0], "total": float(row[1])} for row in stage_rows],
        "total_cost": float(sum(row[5] for row in rows)),
    }


@app.get("/crop/{crop_id}/harvests")
def get_crop_harvests(crop_id: int, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, harvest_date, yield_quintal, selling_price, buyer, revenue FROM harvests WHERE crop_id = %s ORDER BY id;", (crop_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    if not rows:
        return {"crop_id": crop_id, "harvests": [], "total_yield_quintal": 0.0, "total_revenue": 0.0}
    return {
        "crop_id": crop_id,
        "harvests": [
            {"harvest_id": row[0], "harvest_date": str(row[1]), "yield_quintal": float(row[2]), "selling_price": float(row[3]), "buyer": row[4], "revenue": float(row[5])}
            for row in rows
        ],
        "total_yield_quintal": float(sum(row[2] for row in rows)),
        "total_revenue": float(sum(row[5] for row in rows)),
    }


@app.get("/crop/{crop_id}/deals")
def get_crop_deals(crop_id: int, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, sale_date, quantity_quintal, price_per_quintal, buyer, gross_amount, amount_received, payment_status, notes FROM deals WHERE crop_id = %s ORDER BY id;", (crop_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        "crop_id": crop_id,
        "deals": [
            {"deal_id": row[0], "sale_date": str(row[1]), "quantity_quintal": float(row[2]), "price_per_quintal": float(row[3]), "buyer": row[4], "gross_amount": float(row[5]), "amount_received": float(row[6]), "payment_status": row[7], "notes": row[8]}
            for row in rows
        ],
        "total_gross_amount": float(sum(row[5] for row in rows)) if rows else 0.0,
        "total_amount_received": float(sum(row[6] for row in rows)) if rows else 0.0,
    }


@app.get("/crop/{crop_id}/economics")
def get_crop_economics(crop_id: int, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            c.id, f.name, c.crop_type, c.expected_yield_quintal,
            COALESCE(costs.total_cost, 0) AS total_cost,
            COALESCE(harvests.total_yield_quintal, 0) AS harvested_yield,
            COALESCE(deals.gross_sales, COALESCE(harvests.total_revenue, 0), 0) AS gross_sales,
            COALESCE(deals.amount_received, COALESCE(harvests.total_revenue, 0), 0) AS amount_received
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        LEFT JOIN (SELECT crop_id, SUM(amount) AS total_cost FROM input_costs GROUP BY crop_id) costs ON costs.crop_id = c.id
        LEFT JOIN (SELECT crop_id, SUM(revenue) AS total_revenue, SUM(yield_quintal) AS total_yield_quintal FROM harvests GROUP BY crop_id) harvests ON harvests.crop_id = c.id
        LEFT JOIN (SELECT crop_id, SUM(gross_amount) AS gross_sales, SUM(amount_received) AS amount_received FROM deals GROUP BY crop_id) deals ON deals.crop_id = c.id
        WHERE c.id = %s;
        """,
        (crop_id,),
    )
    row = cursor.fetchone()
    cursor.execute(
        """
        SELECT id, revision_date, previous_estimate_quintal, new_estimate_quintal, revision_reason, source, notes
        FROM yield_estimate_revisions
        WHERE crop_id = %s
        ORDER BY revision_date DESC, id DESC;
        """,
        (crop_id,),
    )
    revision_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT id, receipt_number, receipt_date, amount, payment_mode, verification_status, notes
        FROM expense_receipts
        WHERE crop_id = %s
        ORDER BY receipt_date DESC, id DESC;
        """,
        (crop_id,),
    )
    receipt_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT
            id, source_name, snapshot_time, forecast_window_hours, rainfall_mm, temperature_min_c,
            temperature_max_c, humidity_percent, wind_speed_kmph, solar_radiation, heat_risk,
            flood_risk, drought_risk, hail_risk, lightning_risk
        FROM weather_snapshots
        WHERE crop_id = %s
        ORDER BY snapshot_time DESC, id DESC;
        """,
        (crop_id,),
    )
    weather_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT
            id, alert_type, severity, detected_at, source_type, confidence_score, title,
            message, recommended_action, status, resolved_at
        FROM risk_alert_events
        WHERE crop_id = %s
        ORDER BY detected_at DESC, id DESC;
        """,
        (crop_id,),
    )
    risk_rows = cursor.fetchall()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": "Crop not found"}
    total_cost = float(row[4])
    gross_sales = float(row[6])
    amount_received = float(row[7])
    expected_yield = float(row[3]) if row[3] is not None else None
    break_even = None if not expected_yield else round(total_cost / expected_yield, 2)
    return {
        "crop_id": row[0],
        "farmer": row[1],
        "crop": row[2],
        "expected_yield_quintal": expected_yield,
        "harvested_yield_quintal": float(row[5]),
        "total_cost": total_cost,
        "gross_sales": gross_sales,
        "amount_received": amount_received,
        "outstanding_amount": gross_sales - amount_received,
        "profit": gross_sales - total_cost,
        "realized_profit": amount_received - total_cost,
        "margin_percent": 0 if gross_sales == 0 else round(((gross_sales - total_cost) / gross_sales) * 100, 2),
        "breakeven_per_quintal": break_even,
        "yield_revisions": [
            {
                "revision_id": revision[0],
                "revision_date": str(revision[1]),
                "previous_estimate_quintal": float(revision[2]) if revision[2] is not None else None,
                "new_estimate_quintal": float(revision[3]),
                "revision_reason": revision[4],
                "source": revision[5],
                "notes": revision[6],
            }
            for revision in revision_rows
        ],
        "expense_receipts": [
            {
                "receipt_id": receipt[0],
                "receipt_number": receipt[1],
                "receipt_date": str(receipt[2]),
                "amount": float(receipt[3]),
                "payment_mode": receipt[4],
                "verification_status": receipt[5],
                "notes": receipt[6],
            }
            for receipt in receipt_rows
        ],
        "weather_snapshots": [
            {
                "weather_snapshot_id": weather[0],
                "source_name": weather[1],
                "snapshot_time": str(weather[2]),
                "forecast_window_hours": weather[3],
                "rainfall_mm": float(weather[4]) if weather[4] is not None else None,
                "temperature_min_c": float(weather[5]) if weather[5] is not None else None,
                "temperature_max_c": float(weather[6]) if weather[6] is not None else None,
                "humidity_percent": float(weather[7]) if weather[7] is not None else None,
                "wind_speed_kmph": float(weather[8]) if weather[8] is not None else None,
                "solar_radiation": float(weather[9]) if weather[9] is not None else None,
                "heat_risk": weather[10],
                "flood_risk": weather[11],
                "drought_risk": weather[12],
                "hail_risk": weather[13],
                "lightning_risk": weather[14],
            }
            for weather in weather_rows
        ],
        "risk_alerts": [
            {
                "risk_alert_id": risk[0],
                "alert_type": risk[1],
                "severity": risk[2],
                "detected_at": str(risk[3]),
                "source_type": risk[4],
                "confidence_score": float(risk[5]) if risk[5] is not None else None,
                "title": risk[6],
                "message": risk[7],
                "recommended_action": risk[8],
                "status": risk[9],
                "resolved_at": str(risk[10]) if risk[10] is not None else None,
            }
            for risk in risk_rows
        ],
    }


@app.get("/crop/{crop_id}/final-profit")
def get_crop_final_profit(crop_id: int, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT c.id, f.name, c.crop_type, COALESCE(costs.total_cost, 0), COALESCE(deals.gross_amount, 0), COALESCE(deals.amount_received, 0)
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        LEFT JOIN (SELECT crop_id, SUM(amount) AS total_cost FROM input_costs GROUP BY crop_id) costs ON costs.crop_id = c.id
        LEFT JOIN (SELECT crop_id, SUM(gross_amount) AS gross_amount, SUM(amount_received) AS amount_received FROM deals GROUP BY crop_id) deals ON deals.crop_id = c.id
        WHERE c.id = %s;
        """,
        (crop_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": "Crop not found"}
    total_cost = float(row[3])
    gross_sales = float(row[4])
    amount_received = float(row[5])
    return {
        "crop_id": row[0],
        "farmer": row[1],
        "crop": row[2],
        "total_cost": total_cost,
        "gross_sales": gross_sales,
        "amount_received": amount_received,
        "outstanding_amount": gross_sales - amount_received,
        "final_profit": gross_sales - total_cost,
        "realized_profit": amount_received - total_cost,
        "ledger_closed": gross_sales > 0 and amount_received >= gross_sales,
    }


@app.get("/breakeven/{name}")
def get_breakeven(name: str, user: dict = Depends(get_current_user)):
    farmer_id = get_farmer_id_by_name(name)
    if farmer_id is None:
        return {"error": f"No data found: {name}"}
    ensure_farmer_access(user, farmer_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT f.name, c.crop_type, SUM(i.amount), c.expected_yield_quintal,
               ROUND((SUM(i.amount) / NULLIF(c.expected_yield_quintal, 0))::numeric, 2)
        FROM farmers f
        JOIN crops c ON c.farmer_id = f.id
        JOIN input_costs i ON i.crop_id = c.id
        WHERE f.name = %s
        GROUP BY f.name, c.crop_type, c.expected_yield_quintal;
        """,
        (name,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": f"No data found: {name}"}
    if row[3] is None or row[4] is None:
        return {"error": f"Expected yield is missing for: {name}"}
    return {"farmer": row[0], "crop": row[1], "total_cost": float(row[2]), "expected_yield_quintal": float(row[3]), "breakeven_per_quintal": float(row[4])}


@app.post("/farmers/add")
def add_farmer(farmer: NewFarmer, user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO farmers
                (name, phone, alternate_phone, village, gram_panchayat, tehsil, district, state,
                 postal_code, education_level, farming_experience_years, land_acres, kyc_status, consent_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                farmer.name,
                farmer.phone,
                farmer.alternate_phone,
                farmer.village,
                farmer.gram_panchayat,
                farmer.tehsil,
                farmer.district,
                farmer.state,
                farmer.postal_code,
                farmer.education_level,
                farmer.farming_experience_years,
                farmer.land_acres,
                farmer.kyc_status,
                farmer.consent_status,
            ),
        )
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Farmer registered successfully", "farmer_id": new_id, "name": farmer.name}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/parcels/add")
def add_parcel(parcel: NewParcel, user: dict = Depends(get_current_user)):
    ensure_farmer_access(user, parcel.farmer_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM farmers WHERE id = %s;", (parcel.farmer_id,))
        if cursor.fetchone() is None:
            cursor.close()
            conn.close()
            return {"error": "Farmer not found"}
        cursor.execute(
            """
            INSERT INTO land_parcels
                (farmer_id, plot_name, parcel_code, area_acres, cultivable_area_acres, irrigated_area_acres,
                 location, village, tehsil, survey_number, soil_type, irrigation_source,
                 ownership_type, road_access, power_access, fencing_status, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                parcel.farmer_id,
                parcel.plot_name,
                parcel.parcel_code,
                parcel.area_acres,
                parcel.cultivable_area_acres,
                parcel.irrigated_area_acres,
                parcel.location,
                parcel.village,
                parcel.tehsil,
                parcel.survey_number,
                parcel.soil_type,
                parcel.irrigation_source,
                parcel.ownership_type,
                parcel.road_access,
                parcel.power_access,
                parcel.fencing_status,
                parcel.latitude,
                parcel.longitude,
            ),
        )
        parcel_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {
            "message": "Parcel registered successfully",
            "parcel_id": parcel_id,
            "plot_name": parcel.plot_name,
            "farmer_id": parcel.farmer_id,
        }
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/farmer-members/add")
def add_farmer_member(member: NewFarmerMember, user: dict = Depends(get_current_user)):
    ensure_farmer_access(user, member.farmer_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM farmers WHERE id = %s;", (member.farmer_id,))
        if cursor.fetchone() is None:
            cursor.close()
            conn.close()
            return {"error": "Farmer not found"}
        cursor.execute(
            """
            INSERT INTO farmer_members
                (farmer_id, name, relation, age, gender, phone, role_in_agriculture,
                 primary_operator, decision_maker)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                member.farmer_id,
                member.name,
                member.relation,
                member.age,
                member.gender,
                member.phone,
                member.role_in_agriculture,
                member.primary_operator,
                member.decision_maker,
            ),
        )
        member_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Farmer member added", "member_id": member_id, "farmer_id": member.farmer_id}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/soil-tests/add")
def add_soil_test(test: NewSoilTest, user: dict = Depends(get_current_user)):
    ensure_farmer_access(user, test.farmer_id)
    ensure_parcel_access(user, test.parcel_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT farmer_id FROM land_parcels WHERE id = %s;", (test.parcel_id,))
        parcel_row = cursor.fetchone()
        if parcel_row is None:
            cursor.close()
            conn.close()
            return {"error": "Parcel not found"}
        if parcel_row[0] != test.farmer_id:
            cursor.close()
            conn.close()
            return {"error": "Parcel does not belong to this farmer"}
        cursor.execute(
            """
            INSERT INTO soil_tests
                (farmer_id, parcel_id, sample_date, lab_name, report_number, ph,
                 organic_carbon, nitrogen, phosphorus, potassium, recommendation_summary)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                test.farmer_id,
                test.parcel_id,
                test.sample_date,
                test.lab_name,
                test.report_number,
                test.ph,
                test.organic_carbon,
                test.nitrogen,
                test.phosphorus,
                test.potassium,
                test.recommendation_summary,
            ),
        )
        soil_test_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Soil test recorded", "soil_test_id": soil_test_id, "parcel_id": test.parcel_id}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/documents/add")
def add_document(document: NewFarmerDocument, user: dict = Depends(get_current_user)):
    ensure_farmer_access(user, document.farmer_id)
    if document.parcel_id is not None:
        ensure_parcel_access(user, document.parcel_id)
    if document.crop_id is not None:
        ensure_crop_access(user, document.crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO farmer_documents
                (farmer_id, parcel_id, crop_id, document_type, document_number, issued_by,
                 issue_date, expiry_date, verification_status, file_url, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                document.farmer_id,
                document.parcel_id,
                document.crop_id,
                document.document_type,
                document.document_number,
                document.issued_by,
                document.issue_date,
                document.expiry_date,
                document.verification_status,
                document.file_url,
                document.notes,
            ),
        )
        document_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Document recorded", "document_id": document_id, "farmer_id": document.farmer_id}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/buyers/add")
def add_buyer(buyer: NewBuyerRegistry, user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO buyer_registry
                (name, buyer_type, contact_person, phone, location, district, state,
                 payment_reliability_score, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                buyer.name,
                buyer.buyer_type,
                buyer.contact_person,
                buyer.phone,
                buyer.location,
                buyer.district,
                buyer.state,
                buyer.payment_reliability_score,
                buyer.notes,
            ),
        )
        buyer_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Buyer added", "buyer_id": buyer_id, "name": buyer.name}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/suppliers/add")
def add_supplier(supplier: NewInputSupplier, user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO input_suppliers
                (name, supplier_type, contact_person, phone, location, district, state, gst_number, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                supplier.name,
                supplier.supplier_type,
                supplier.contact_person,
                supplier.phone,
                supplier.location,
                supplier.district,
                supplier.state,
                supplier.gst_number,
                supplier.notes,
            ),
        )
        supplier_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Supplier added", "supplier_id": supplier_id, "name": supplier.name}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/weather-snapshots/add")
def add_weather_snapshot(entry: WeatherSnapshotEntry, user: dict = Depends(get_current_user)):
    ensure_farmer_access(user, entry.farmer_id)
    if entry.parcel_id is not None:
        ensure_parcel_access(user, entry.parcel_id)
    if entry.crop_id is not None:
        ensure_crop_access(user, entry.crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if entry.parcel_id is not None:
            cursor.execute("SELECT farmer_id FROM land_parcels WHERE id = %s;", (entry.parcel_id,))
            parcel_row = cursor.fetchone()
            if parcel_row is None:
                cursor.close()
                conn.close()
                return {"error": "Parcel not found"}
            if parcel_row[0] != entry.farmer_id:
                cursor.close()
                conn.close()
                return {"error": "Parcel does not belong to this farmer"}
        if entry.crop_id is not None:
            cursor.execute("SELECT farmer_id, parcel_id FROM crops WHERE id = %s;", (entry.crop_id,))
            crop_row = cursor.fetchone()
            if crop_row is None:
                cursor.close()
                conn.close()
                return {"error": "Crop not found"}
            if crop_row[0] != entry.farmer_id:
                cursor.close()
                conn.close()
                return {"error": "Crop does not belong to this farmer"}
            if entry.parcel_id is not None and crop_row[1] is not None and crop_row[1] != entry.parcel_id:
                cursor.close()
                conn.close()
                return {"error": "Crop does not belong to this parcel"}
        cursor.execute(
            """
            INSERT INTO weather_snapshots
                (farmer_id, parcel_id, crop_id, source_name, snapshot_time, forecast_window_hours,
                 rainfall_mm, temperature_min_c, temperature_max_c, humidity_percent, wind_speed_kmph,
                 solar_radiation, heat_risk, flood_risk, drought_risk, hail_risk, lightning_risk, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                entry.farmer_id,
                entry.parcel_id,
                entry.crop_id,
                entry.source_name,
                entry.snapshot_time,
                entry.forecast_window_hours,
                entry.rainfall_mm,
                entry.temperature_min_c,
                entry.temperature_max_c,
                entry.humidity_percent,
                entry.wind_speed_kmph,
                entry.solar_radiation,
                entry.heat_risk,
                entry.flood_risk,
                entry.drought_risk,
                entry.hail_risk,
                entry.lightning_risk,
                entry.raw_payload,
            ),
        )
        weather_snapshot_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Weather snapshot recorded", "weather_snapshot_id": weather_snapshot_id}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/mandi-prices/add")
def add_mandi_price_snapshot(entry: MandiPriceSnapshotEntry, user: dict = Depends(get_current_user)):
    ensure_admin(user)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO mandi_price_snapshots
                (crop_type, variety, market_name, district, state, snapshot_date, min_price,
                 modal_price, max_price, arrival_quantity, source_name, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                entry.crop_type,
                entry.variety,
                entry.market_name,
                entry.district,
                entry.state,
                entry.snapshot_date,
                entry.min_price,
                entry.modal_price,
                entry.max_price,
                entry.arrival_quantity,
                entry.source_name,
                entry.raw_payload,
            ),
        )
        mandi_price_snapshot_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Mandi price snapshot recorded", "mandi_price_snapshot_id": mandi_price_snapshot_id}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/risk-alerts/add")
def add_risk_alert(entry: RiskAlertEventEntry, user: dict = Depends(get_current_user)):
    ensure_farmer_access(user, entry.farmer_id)
    if entry.parcel_id is not None:
        ensure_parcel_access(user, entry.parcel_id)
    if entry.crop_id is not None:
        ensure_crop_access(user, entry.crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if entry.parcel_id is not None:
            cursor.execute("SELECT farmer_id FROM land_parcels WHERE id = %s;", (entry.parcel_id,))
            parcel_row = cursor.fetchone()
            if parcel_row is None:
                cursor.close()
                conn.close()
                return {"error": "Parcel not found"}
            if parcel_row[0] != entry.farmer_id:
                cursor.close()
                conn.close()
                return {"error": "Parcel does not belong to this farmer"}
        if entry.crop_id is not None:
            cursor.execute("SELECT farmer_id, parcel_id FROM crops WHERE id = %s;", (entry.crop_id,))
            crop_row = cursor.fetchone()
            if crop_row is None:
                cursor.close()
                conn.close()
                return {"error": "Crop not found"}
            if crop_row[0] != entry.farmer_id:
                cursor.close()
                conn.close()
                return {"error": "Crop does not belong to this farmer"}
            if entry.parcel_id is not None and crop_row[1] is not None and crop_row[1] != entry.parcel_id:
                cursor.close()
                conn.close()
                return {"error": "Crop does not belong to this parcel"}
        cursor.execute(
            """
            INSERT INTO risk_alert_events
                (farmer_id, parcel_id, crop_id, alert_type, severity, detected_at, source_type,
                 confidence_score, title, message, recommended_action, status, resolved_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                entry.farmer_id,
                entry.parcel_id,
                entry.crop_id,
                entry.alert_type,
                entry.severity,
                entry.detected_at,
                entry.source_type,
                entry.confidence_score,
                entry.title,
                entry.message,
                entry.recommended_action,
                entry.status,
                entry.resolved_at,
            ),
        )
        risk_alert_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Risk alert recorded", "risk_alert_id": risk_alert_id}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/yield-revisions/add")
def add_yield_revision(entry: YieldEstimateRevisionEntry, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, entry.crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT expected_yield_quintal FROM crops WHERE id = %s;", (entry.crop_id,))
        crop_row = cursor.fetchone()
        if crop_row is None:
            cursor.close()
            conn.close()
            return {"error": "Crop not found"}
        previous_estimate = float(crop_row[0]) if crop_row[0] is not None else None
        cursor.execute(
            """
            INSERT INTO yield_estimate_revisions
                (crop_id, revision_date, previous_estimate_quintal, new_estimate_quintal,
                 revision_reason, source, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                entry.crop_id,
                entry.revision_date,
                previous_estimate,
                entry.new_estimate_quintal,
                entry.revision_reason,
                entry.source,
                entry.notes,
            ),
        )
        revision_id = cursor.fetchone()[0]
        cursor.execute(
            "UPDATE crops SET expected_yield_quintal = %s WHERE id = %s;",
            (entry.new_estimate_quintal, entry.crop_id),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return {
            "message": "Yield estimate revised",
            "revision_id": revision_id,
            "crop_id": entry.crop_id,
            "previous_estimate_quintal": previous_estimate,
            "new_estimate_quintal": entry.new_estimate_quintal,
        }
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/receipts/add")
def add_receipt(entry: ExpenseReceiptEntry, user: dict = Depends(get_current_user)):
    ensure_farmer_access(user, entry.farmer_id)
    if entry.crop_id is not None:
        ensure_crop_access(user, entry.crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if entry.input_cost_id is not None:
            cursor.execute(
                """
                SELECT c.farmer_id
                FROM input_costs i
                JOIN crops c ON c.id = i.crop_id
                WHERE i.id = %s;
                """,
                (entry.input_cost_id,),
            )
            cost_row = cursor.fetchone()
            if cost_row is None:
                cursor.close()
                conn.close()
                return {"error": "Input cost not found"}
            if cost_row[0] != entry.farmer_id:
                cursor.close()
                conn.close()
                return {"error": "Input cost does not belong to this farmer"}
        cursor.execute(
            """
            INSERT INTO expense_receipts
                (farmer_id, crop_id, input_cost_id, supplier_id, receipt_number, receipt_date,
                 amount, payment_mode, file_url, verification_status, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                entry.farmer_id,
                entry.crop_id,
                entry.input_cost_id,
                entry.supplier_id,
                entry.receipt_number,
                entry.receipt_date,
                entry.amount,
                entry.payment_mode,
                entry.file_url,
                entry.verification_status,
                entry.notes,
            ),
        )
        receipt_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Expense receipt recorded", "receipt_id": receipt_id, "farmer_id": entry.farmer_id}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/crops/add")
def add_crop(crop: NewCrop, user: dict = Depends(get_current_user)):
    ensure_farmer_access(user, crop.farmer_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if crop.parcel_id is not None:
            cursor.execute("SELECT farmer_id FROM land_parcels WHERE id = %s;", (crop.parcel_id,))
            parcel_row = cursor.fetchone()
            if parcel_row is None:
                cursor.close()
                conn.close()
                return {"error": "Parcel not found"}
            if parcel_row[0] != crop.farmer_id:
                cursor.close()
                conn.close()
                return {"error": "Parcel does not belong to this farmer"}
        cursor.execute(
            """
            INSERT INTO crops
                (farmer_id, parcel_id, seed_source_id, seed_brand, seed_lot_number, crop_type, variety,
                 season, year, sowing_method, sowing_date, expected_harvest, expected_yield_quintal,
                 current_stage, crop_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'sowing', %s)
            RETURNING id;
            """,
            (
                crop.farmer_id,
                crop.parcel_id,
                crop.seed_source_id,
                crop.seed_brand,
                crop.seed_lot_number,
                crop.crop_type,
                crop.variety,
                crop.season,
                crop.year,
                crop.sowing_method,
                crop.sowing_date,
                crop.expected_harvest,
                crop.expected_yield_quintal,
                crop.crop_status,
            ),
        )
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {
            "message": "Crop registered successfully",
            "crop_id": new_id,
            "crop": crop.crop_type,
            "farmer_id": crop.farmer_id,
            "parcel_id": crop.parcel_id,
            "crop_status": crop.crop_status,
        }
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/costs/add")
def add_cost(cost: NewCost, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, cost.crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT current_stage FROM crops WHERE id = %s;", (cost.crop_id,))
        crop = cursor.fetchone()
        if crop is None:
            cursor.close()
            conn.close()
            return {"error": "Crop not found"}
        cursor.execute(
            """
            INSERT INTO input_costs
                (crop_id, stage, category, supplier_id, item_name, invoice_number, quantity, unit,
                 amount, transaction_mode, transaction_reference, transaction_date, subsidized, receipt_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                cost.crop_id,
                cost.stage,
                cost.category,
                cost.supplier_id,
                cost.item_name,
                cost.invoice_number,
                cost.quantity,
                cost.unit,
                cost.amount,
                cost.transaction_mode,
                cost.transaction_reference,
                cost.transaction_date,
                cost.subsidized,
                cost.receipt_id,
            ),
        )
        new_id = cursor.fetchone()[0]
        if stage_rank(cost.stage) > stage_rank(crop[0]):
            cursor.execute("UPDATE crops SET current_stage = %s WHERE id = %s;", (cost.stage, cost.crop_id))
        cursor.execute("SELECT SUM(amount) FROM input_costs WHERE crop_id = %s;", (cost.crop_id,))
        total = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Cost logged successfully", "cost_id": new_id, "item": cost.item_name, "amount": cost.amount, "running_total": float(total)}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/harvests/add")
def add_harvest(entry: HarvestEntry, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, entry.crop_id)
    revenue = entry.yield_quintal * entry.selling_price
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT expected_yield_quintal FROM crops WHERE id = %s;", (entry.crop_id,))
        crop = cursor.fetchone()
        if crop is None:
            cursor.close()
            conn.close()
            return {"error": "Crop not found"}
        cursor.execute(
            """
            SELECT id FROM harvests
            WHERE crop_id = %s AND harvest_date = %s AND buyer = %s
              AND yield_quintal = %s AND selling_price = %s;
            """,
            (entry.crop_id, entry.harvest_date, entry.buyer, entry.yield_quintal, entry.selling_price),
        )
        if cursor.fetchone() is not None:
            cursor.close()
            conn.close()
            return {"error": "This harvest entry already exists"}
        expected_yield = crop[0]
        cursor.execute("SELECT COALESCE(SUM(yield_quintal), 0) FROM harvests WHERE crop_id = %s;", (entry.crop_id,))
        harvested_so_far = float(cursor.fetchone()[0])
        if expected_yield is not None and harvested_so_far + entry.yield_quintal > float(expected_yield) * 1.25:
            cursor.close()
            conn.close()
            return {"error": "Harvest exceeds expected yield by more than 25%. Please verify the entry."}
        cursor.execute(
            """
            INSERT INTO harvests
                (crop_id, harvest_date, yield_quintal, yield_rejected_quintal, quality_grade,
                 moisture_percent, bags_count, storage_location, selling_price, buyer, revenue)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                entry.crop_id,
                entry.harvest_date,
                entry.yield_quintal,
                entry.yield_rejected_quintal,
                entry.quality_grade,
                entry.moisture_percent,
                entry.bags_count,
                entry.storage_location,
                entry.selling_price,
                entry.buyer,
                revenue,
            ),
        )
        harvest_id = cursor.fetchone()[0]
        cursor.execute("UPDATE crops SET current_stage = 'harvest' WHERE id = %s AND current_stage IN ('sowing', 'growing');", (entry.crop_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Harvest logged", "harvest_id": harvest_id, "revenue": revenue}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.post("/deals/add")
def add_deal(entry: DealEntry, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, entry.crop_id)
    gross_amount = entry.quantity_quintal * entry.price_per_quintal
    net_amount = max(0, gross_amount - entry.deductions_amount - entry.transport_cost - entry.mandi_fee)
    amount_received = min(entry.amount_received, net_amount)
    payment_status = get_payment_status(amount_received, net_amount)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO deals
                (crop_id, buyer_id, buyer_type, sale_date, quantity_quintal, price_per_quintal, buyer,
                 gross_amount, deductions_amount, transport_cost, mandi_fee, net_amount,
                 amount_received, payment_terms, due_date, payment_status, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                entry.crop_id,
                entry.buyer_id,
                entry.buyer_type,
                entry.sale_date,
                entry.quantity_quintal,
                entry.price_per_quintal,
                entry.buyer,
                gross_amount,
                entry.deductions_amount,
                entry.transport_cost,
                entry.mandi_fee,
                net_amount,
                amount_received,
                entry.payment_terms,
                entry.due_date,
                payment_status,
                entry.notes,
            ),
        )
        deal_id = cursor.fetchone()[0]
        cursor.execute("UPDATE crops SET current_stage = CASE WHEN %s = 'paid' THEN 'storage' ELSE 'logistics' END WHERE id = %s;", (payment_status, entry.crop_id))
        conn.commit()
        cursor.close()
        conn.close()
        return {
            "message": "Deal recorded",
            "deal_id": deal_id,
            "gross_amount": gross_amount,
            "net_amount": net_amount,
            "amount_received": amount_received,
            "payment_status": payment_status,
        }
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.patch("/deals/{deal_id}/payment")
def update_deal_payment(deal_id: int, data: DealPaymentUpdate, user: dict = Depends(get_current_user)):
    ensure_deal_access(user, deal_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT gross_amount, crop_id FROM deals WHERE id = %s;", (deal_id,))
        row = cursor.fetchone()
        if row is None:
            cursor.close()
            conn.close()
            return {"error": "Deal not found"}
        gross_amount = float(row[0])
        crop_id = row[1]
        amount_received = min(data.amount_received, gross_amount)
        payment_status = get_payment_status(amount_received, gross_amount)
        cursor.execute("UPDATE deals SET amount_received = %s, payment_status = %s WHERE id = %s RETURNING id;", (amount_received, payment_status, deal_id))
        cursor.fetchone()
        cursor.execute(
            """
            UPDATE crops
            SET current_stage = CASE
                WHEN EXISTS (SELECT 1 FROM deals WHERE crop_id = %s AND payment_status != 'paid') THEN 'logistics'
                ELSE 'storage'
            END
            WHERE id = %s;
            """,
            (crop_id, crop_id),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Deal payment updated", "deal_id": deal_id, "amount_received": amount_received, "payment_status": payment_status, "outstanding_amount": gross_amount - amount_received}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.patch("/crops/{crop_id}/stage")
def update_crop_stage(crop_id: int, data: CropStageUpdate, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE crops SET current_stage = %s WHERE id = %s RETURNING id;", (data.stage, crop_id))
        row = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        if row is None:
            return {"error": "Crop not found"}
        return {"message": "Stage updated", "crop_id": crop_id, "new_stage": data.stage}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.patch("/crops/{crop_id}/yield")
def update_crop_yield(crop_id: int, data: CropYieldUpdate, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE crops SET expected_yield_quintal = %s WHERE id = %s RETURNING id;", (data.expected_yield_quintal, crop_id))
        row = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        if row is None:
            return {"error": "Crop not found"}
        return {"message": "Expected yield updated", "crop_id": crop_id, "expected_yield_quintal": data.expected_yield_quintal}
    except Exception as exc:
        return handle_db_error(exc, conn, cursor)


@app.get("/farmer/{name}/economics")
def farmer_economics(name: str, user: dict = Depends(get_current_user)):
    farmer_id = get_farmer_id_by_name(name)
    if farmer_id is None:
        return {"error": "Farmer not found"}
    ensure_farmer_access(user, farmer_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            f.name,
            COALESCE(SUM(costs.total_cost), 0) AS total_cost,
            COALESCE(SUM(COALESCE(deals.gross_sales, harvests.revenue, 0)), 0) AS gross_sales,
            COALESCE(SUM(COALESCE(deals.amount_received, harvests.revenue, 0)), 0) AS amount_received
        FROM farmers f
        JOIN crops c ON c.farmer_id = f.id
        LEFT JOIN (SELECT crop_id, SUM(amount) AS total_cost FROM input_costs GROUP BY crop_id) costs ON costs.crop_id = c.id
        LEFT JOIN (SELECT crop_id, SUM(revenue) AS revenue FROM harvests GROUP BY crop_id) harvests ON harvests.crop_id = c.id
        LEFT JOIN (SELECT crop_id, SUM(gross_amount) AS gross_sales, SUM(amount_received) AS amount_received FROM deals GROUP BY crop_id) deals ON deals.crop_id = c.id
        WHERE f.name = %s
        GROUP BY f.name;
        """,
        (name,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": "Farmer not found"}
    total_cost = float(row[1])
    gross_sales = float(row[2])
    amount_received = float(row[3])
    return {
        "farmer": row[0],
        "total_cost": total_cost,
        "gross_sales": gross_sales,
        "amount_received": amount_received,
        "outstanding_amount": gross_sales - amount_received,
        "profit": gross_sales - total_cost,
        "realized_profit": amount_received - total_cost,
        "margin_percent": 0 if gross_sales == 0 else round(((gross_sales - total_cost) / gross_sales) * 100, 2),
    }


@app.get("/farmer/me/full-ledger")
def get_my_farmer_full_ledger(user: dict = Depends(get_current_user)):
    if user["role"] != "farmer" or user["farmer_id"] is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Farmer access required")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM farmers WHERE id = %s;", (user["farmer_id"],))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": "Linked farmer not found"}
    return get_farmer_full_ledger(row[0], user)


@app.get("/farmer/{name}/full-ledger")
def get_farmer_full_ledger(name: str, user: dict = Depends(get_current_user)):
    farmer_id = get_farmer_id_by_name(name)
    if farmer_id is None:
        return {"error": "Farmer not found"}
    ensure_farmer_access(user, farmer_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            id, name, phone, alternate_phone, village, gram_panchayat, tehsil, district, state,
            postal_code, education_level, farming_experience_years, land_acres, kyc_status, consent_status
        FROM farmers
        WHERE name = %s;
        """,
        (name,),
    )
    farmer = cursor.fetchone()
    if farmer is None:
        cursor.close()
        conn.close()
        return {"error": "Farmer not found"}
    cursor.execute(
        """
        SELECT
            id, plot_name, parcel_code, area_acres, cultivable_area_acres, irrigated_area_acres,
            location, village, tehsil, survey_number, soil_type, irrigation_source, ownership_type,
            road_access, power_access, fencing_status, latitude, longitude
        FROM land_parcels
        WHERE farmer_id = %s
        ORDER BY id;
        """,
        (farmer_id,),
    )
    parcel_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT
            id, name, relation, age, gender, phone, role_in_agriculture,
            primary_operator, decision_maker
        FROM farmer_members
        WHERE farmer_id = %s
        ORDER BY id;
        """,
        (farmer_id,),
    )
    member_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT
            st.id, st.parcel_id, lp.plot_name, st.sample_date, st.lab_name, st.report_number,
            st.ph, st.organic_carbon, st.nitrogen, st.phosphorus, st.potassium,
            st.recommendation_summary
        FROM soil_tests st
        JOIN land_parcels lp ON lp.id = st.parcel_id
        WHERE st.farmer_id = %s
        ORDER BY st.sample_date DESC, st.id DESC;
        """,
        (farmer_id,),
    )
    soil_test_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT
            id, parcel_id, crop_id, document_type, document_number, issued_by,
            issue_date, expiry_date, verification_status, file_url, notes
        FROM farmer_documents
        WHERE farmer_id = %s
        ORDER BY id DESC;
        """,
        (farmer_id,),
    )
    document_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT
            c.id, c.parcel_id, lp.plot_name, c.seed_source_id, c.seed_brand, c.seed_lot_number, c.crop_type, c.variety, c.season, c.year,
            c.sowing_method, c.sowing_date, c.expected_harvest, c.expected_yield_quintal, c.current_stage, c.crop_status,
            COALESCE(costs.total_cost, 0), COALESCE(harvests.total_revenue, 0),
            COALESCE(harvests.total_yield_quintal, 0), COALESCE(deals.gross_amount, 0),
            COALESCE(deals.amount_received, 0)
        FROM crops c
        LEFT JOIN land_parcels lp ON lp.id = c.parcel_id
        LEFT JOIN (SELECT crop_id, SUM(amount) AS total_cost FROM input_costs GROUP BY crop_id) costs ON costs.crop_id = c.id
        LEFT JOIN (SELECT crop_id, SUM(revenue) AS total_revenue, SUM(yield_quintal) AS total_yield_quintal FROM harvests GROUP BY crop_id) harvests ON harvests.crop_id = c.id
        LEFT JOIN (SELECT crop_id, SUM(gross_amount) AS gross_amount, SUM(amount_received) AS amount_received FROM deals GROUP BY crop_id) deals ON deals.crop_id = c.id
        WHERE c.farmer_id = %s
        ORDER BY c.id;
        """,
        (farmer_id,),
    )
    crop_rows = cursor.fetchall()
    cursor.execute("SELECT c.id, i.id, i.stage, i.category, i.supplier_id, i.item_name, i.invoice_number, i.quantity, i.unit, i.amount, i.transaction_mode, i.transaction_reference, i.transaction_date, i.subsidized, i.receipt_id, i.entry_date FROM crops c LEFT JOIN input_costs i ON i.crop_id = c.id WHERE c.farmer_id = %s ORDER BY c.id, i.id;", (farmer_id,))
    cost_rows = cursor.fetchall()
    cursor.execute("SELECT c.id, h.id, h.harvest_date, h.yield_quintal, h.yield_rejected_quintal, h.quality_grade, h.moisture_percent, h.bags_count, h.storage_location, h.selling_price, h.buyer, h.revenue FROM crops c LEFT JOIN harvests h ON h.crop_id = c.id WHERE c.farmer_id = %s ORDER BY c.id, h.id;", (farmer_id,))
    harvest_rows = cursor.fetchall()
    cursor.execute("SELECT c.id, d.id, d.buyer_id, d.buyer_type, d.sale_date, d.quantity_quintal, d.price_per_quintal, d.buyer, d.gross_amount, d.deductions_amount, d.transport_cost, d.mandi_fee, d.net_amount, d.amount_received, d.payment_terms, d.due_date, d.payment_status, d.notes FROM crops c LEFT JOIN deals d ON d.crop_id = c.id WHERE c.farmer_id = %s ORDER BY c.id, d.id;", (farmer_id,))
    deal_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT c.id, y.id, y.revision_date, y.previous_estimate_quintal, y.new_estimate_quintal, y.revision_reason, y.source, y.notes
        FROM crops c
        LEFT JOIN yield_estimate_revisions y ON y.crop_id = c.id
        WHERE c.farmer_id = %s
        ORDER BY c.id, y.revision_date DESC, y.id DESC;
        """,
        (farmer_id,),
    )
    revision_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT c.id, r.id, r.receipt_number, r.receipt_date, r.amount, r.payment_mode, r.verification_status, r.notes
        FROM crops c
        LEFT JOIN expense_receipts r ON r.crop_id = c.id
        WHERE c.farmer_id = %s
        ORDER BY c.id, r.receipt_date DESC, r.id DESC;
        """,
        (farmer_id,),
    )
    receipt_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT
            c.id, w.id, w.source_name, w.snapshot_time, w.forecast_window_hours, w.rainfall_mm,
            w.temperature_min_c, w.temperature_max_c, w.humidity_percent, w.wind_speed_kmph,
            w.solar_radiation, w.heat_risk, w.flood_risk, w.drought_risk, w.hail_risk, w.lightning_risk
        FROM crops c
        LEFT JOIN weather_snapshots w ON w.crop_id = c.id
        WHERE c.farmer_id = %s
        ORDER BY c.id, w.snapshot_time DESC, w.id DESC;
        """,
        (farmer_id,),
    )
    weather_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT
            c.id, ra.id, ra.alert_type, ra.severity, ra.detected_at, ra.source_type,
            ra.confidence_score, ra.title, ra.message, ra.recommended_action, ra.status, ra.resolved_at
        FROM crops c
        LEFT JOIN risk_alert_events ra ON ra.crop_id = c.id
        WHERE c.farmer_id = %s
        ORDER BY c.id, ra.detected_at DESC, ra.id DESC;
        """,
        (farmer_id,),
    )
    risk_rows = cursor.fetchall()
    cursor.close()
    conn.close()
    crops = []
    for row in crop_rows:
        crop_id = row[0]
        crops.append({
            "crop_id": crop_id,
            "parcel_id": row[1],
            "parcel_name": row[2],
            "seed_source_id": row[3],
            "seed_brand": row[4],
            "seed_lot_number": row[5],
            "crop": row[6],
            "variety": row[7],
            "season": row[8],
            "year": row[9],
            "sowing_method": row[10],
            "sowing_date": str(row[11]),
            "expected_harvest": str(row[12]),
            "expected_yield_quintal": float(row[13]) if row[13] is not None else None,
            "stage": row[14],
            "crop_status": row[15],
            "total_cost": float(row[16]),
            "total_revenue": float(row[17]),
            "total_yield_quintal": float(row[18]),
            "gross_sales": float(row[19]),
            "amount_received": float(row[20]),
            "costs": [
                {
                    "cost_id": cost_row[1],
                    "stage": cost_row[2],
                    "category": cost_row[3],
                    "supplier_id": cost_row[4],
                    "item_name": cost_row[5],
                    "invoice_number": cost_row[6],
                    "quantity": float(cost_row[7]),
                    "unit": cost_row[8],
                    "amount": float(cost_row[9]),
                    "transaction_mode": cost_row[10],
                    "transaction_reference": cost_row[11],
                    "transaction_date": str(cost_row[12]) if cost_row[12] is not None else None,
                    "subsidized": cost_row[13],
                    "receipt_id": cost_row[14],
                    "entry_date": str(cost_row[15]),
                }
                for cost_row in cost_rows if cost_row[0] == crop_id and cost_row[1] is not None
            ],
            "harvests": [
                {
                    "harvest_id": harvest_row[1],
                    "harvest_date": str(harvest_row[2]),
                    "yield_quintal": float(harvest_row[3]),
                    "yield_rejected_quintal": float(harvest_row[4]) if harvest_row[4] is not None else 0,
                    "quality_grade": harvest_row[5],
                    "moisture_percent": float(harvest_row[6]) if harvest_row[6] is not None else None,
                    "bags_count": harvest_row[7],
                    "storage_location": harvest_row[8],
                    "selling_price": float(harvest_row[9]),
                    "buyer": harvest_row[10],
                    "revenue": float(harvest_row[11]),
                }
                for harvest_row in harvest_rows if harvest_row[0] == crop_id and harvest_row[1] is not None
            ],
            "deals": [
                {
                    "deal_id": deal_row[1],
                    "buyer_id": deal_row[2],
                    "buyer_type": deal_row[3],
                    "sale_date": str(deal_row[4]),
                    "quantity_quintal": float(deal_row[5]),
                    "price_per_quintal": float(deal_row[6]),
                    "buyer": deal_row[7],
                    "gross_amount": float(deal_row[8]),
                    "deductions_amount": float(deal_row[9]),
                    "transport_cost": float(deal_row[10]),
                    "mandi_fee": float(deal_row[11]),
                    "net_amount": float(deal_row[12]),
                    "amount_received": float(deal_row[13]),
                    "payment_terms": deal_row[14],
                    "due_date": str(deal_row[15]) if deal_row[15] is not None else None,
                    "payment_status": deal_row[16],
                    "notes": deal_row[17],
                }
                for deal_row in deal_rows if deal_row[0] == crop_id and deal_row[1] is not None
            ],
            "yield_revisions": [
                {
                    "revision_id": revision_row[1],
                    "revision_date": str(revision_row[2]),
                    "previous_estimate_quintal": float(revision_row[3]) if revision_row[3] is not None else None,
                    "new_estimate_quintal": float(revision_row[4]),
                    "revision_reason": revision_row[5],
                    "source": revision_row[6],
                    "notes": revision_row[7],
                }
                for revision_row in revision_rows if revision_row[0] == crop_id and revision_row[1] is not None
            ],
            "expense_receipts": [
                {
                    "receipt_id": receipt_row[1],
                    "receipt_number": receipt_row[2],
                    "receipt_date": str(receipt_row[3]),
                    "amount": float(receipt_row[4]),
                    "payment_mode": receipt_row[5],
                    "verification_status": receipt_row[6],
                    "notes": receipt_row[7],
                }
                for receipt_row in receipt_rows if receipt_row[0] == crop_id and receipt_row[1] is not None
            ],
            "weather_snapshots": [
                {
                    "weather_snapshot_id": weather_row[1],
                    "source_name": weather_row[2],
                    "snapshot_time": str(weather_row[3]),
                    "forecast_window_hours": weather_row[4],
                    "rainfall_mm": float(weather_row[5]) if weather_row[5] is not None else None,
                    "temperature_min_c": float(weather_row[6]) if weather_row[6] is not None else None,
                    "temperature_max_c": float(weather_row[7]) if weather_row[7] is not None else None,
                    "humidity_percent": float(weather_row[8]) if weather_row[8] is not None else None,
                    "wind_speed_kmph": float(weather_row[9]) if weather_row[9] is not None else None,
                    "solar_radiation": float(weather_row[10]) if weather_row[10] is not None else None,
                    "heat_risk": weather_row[11],
                    "flood_risk": weather_row[12],
                    "drought_risk": weather_row[13],
                    "hail_risk": weather_row[14],
                    "lightning_risk": weather_row[15],
                }
                for weather_row in weather_rows if weather_row[0] == crop_id and weather_row[1] is not None
            ],
            "risk_alerts": [
                {
                    "risk_alert_id": risk_row[1],
                    "alert_type": risk_row[2],
                    "severity": risk_row[3],
                    "detected_at": str(risk_row[4]),
                    "source_type": risk_row[5],
                    "confidence_score": float(risk_row[6]) if risk_row[6] is not None else None,
                    "title": risk_row[7],
                    "message": risk_row[8],
                    "recommended_action": risk_row[9],
                    "status": risk_row[10],
                    "resolved_at": str(risk_row[11]) if risk_row[11] is not None else None,
                }
                for risk_row in risk_rows if risk_row[0] == crop_id and risk_row[1] is not None
            ],
        })
    total_cost = sum(crop["total_cost"] for crop in crops)
    gross_sales = sum(crop["gross_sales"] or crop["total_revenue"] for crop in crops)
    amount_received = sum(crop["amount_received"] or crop["total_revenue"] for crop in crops)
    return {
        "farmer": {
            "farmer_id": farmer[0],
            "name": farmer[1],
            "phone": farmer[2],
            "alternate_phone": farmer[3],
            "village": farmer[4],
            "gram_panchayat": farmer[5],
            "tehsil": farmer[6],
            "district": farmer[7],
            "state": farmer[8],
            "postal_code": farmer[9],
            "education_level": farmer[10],
            "farming_experience_years": farmer[11],
            "land_acres": float(farmer[12]),
            "kyc_status": farmer[13],
            "consent_status": farmer[14],
        },
        "members": [
            {
                "member_id": row[0],
                "name": row[1],
                "relation": row[2],
                "age": row[3],
                "gender": row[4],
                "phone": row[5],
                "role_in_agriculture": row[6],
                "primary_operator": row[7],
                "decision_maker": row[8],
            }
            for row in member_rows
        ],
        "parcels": [
            {
                "parcel_id": row[0],
                "plot_name": row[1],
                "parcel_code": row[2],
                "area_acres": float(row[3]),
                "cultivable_area_acres": float(row[4]) if row[4] is not None else None,
                "irrigated_area_acres": float(row[5]) if row[5] is not None else None,
                "location": row[6],
                "village": row[7],
                "tehsil": row[8],
                "survey_number": row[9],
                "soil_type": row[10],
                "irrigation_source": row[11],
                "ownership_type": row[12],
                "road_access": row[13],
                "power_access": row[14],
                "fencing_status": row[15],
                "latitude": float(row[16]) if row[16] is not None else None,
                "longitude": float(row[17]) if row[17] is not None else None,
            }
            for row in parcel_rows
        ],
        "soil_tests": [
            {
                "soil_test_id": row[0],
                "parcel_id": row[1],
                "parcel_name": row[2],
                "sample_date": str(row[3]),
                "lab_name": row[4],
                "report_number": row[5],
                "ph": float(row[6]) if row[6] is not None else None,
                "organic_carbon": float(row[7]) if row[7] is not None else None,
                "nitrogen": float(row[8]) if row[8] is not None else None,
                "phosphorus": float(row[9]) if row[9] is not None else None,
                "potassium": float(row[10]) if row[10] is not None else None,
                "recommendation_summary": row[11],
            }
            for row in soil_test_rows
        ],
        "documents": [
            {
                "document_id": row[0],
                "parcel_id": row[1],
                "crop_id": row[2],
                "document_type": row[3],
                "document_number": row[4],
                "issued_by": row[5],
                "issue_date": str(row[6]) if row[6] is not None else None,
                "expiry_date": str(row[7]) if row[7] is not None else None,
                "verification_status": row[8],
                "file_url": row[9],
                "notes": row[10],
            }
            for row in document_rows
        ],
        "risk_alerts": [
            {
                "risk_alert_id": risk_row[1],
                "crop_id": risk_row[0],
                "alert_type": risk_row[2],
                "severity": risk_row[3],
                "detected_at": str(risk_row[4]),
                "source_type": risk_row[5],
                "confidence_score": float(risk_row[6]) if risk_row[6] is not None else None,
                "title": risk_row[7],
                "message": risk_row[8],
                "recommended_action": risk_row[9],
                "status": risk_row[10],
                "resolved_at": str(risk_row[11]) if risk_row[11] is not None else None,
            }
            for risk_row in risk_rows if risk_row[1] is not None
        ],
        "crops": crops,
        "economics": {
            "total_cost": total_cost,
            "gross_sales": gross_sales,
            "amount_received": amount_received,
            "outstanding_amount": gross_sales - amount_received,
            "profit": gross_sales - total_cost,
            "realized_profit": amount_received - total_cost,
            "margin_percent": 0 if gross_sales == 0 else round(((gross_sales - total_cost) / gross_sales) * 100, 2),
        },
    }




# ═══════════════════════════════════════════════════════════════════════════════
# MANDI PRICE SYNC — data.gov.in Agmarknet Integration
# ═══════════════════════════════════════════════════════════════════════════════

AGMARKNET_URL = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"

MANDI_TARGETS = [
    {"commodity": "Wheat", "crop_type": "wheat", "state": "Madhya Pradesh"},
    {"commodity": "Maize", "crop_type": "maize", "state": "Madhya Pradesh"},
    # Soybean and Rice added when in season (kharif arrivals Oct-Nov)
    # {"commodity": "Soyabean", "crop_type": "soybean", "state": "Madhya Pradesh"},
    # {"commodity": "Rice",     "crop_type": "rice",     "state": "Madhya Pradesh"},
]


def get_agmarknet_api_key():
    return os.getenv("AGMARKNET_API_KEY", "")


def fetch_mandi_records(commodity: str, state: str, limit: int = 50) -> list:
    """Fetch latest mandi prices from data.gov.in for a commodity+state."""
    import urllib.request
    import urllib.parse

    api_key = get_agmarknet_api_key()
    if not api_key:
        raise ValueError("AGMARKNET_API_KEY not set")

    params = urllib.parse.urlencode({
        "api-key": api_key,
        "format": "json",
        "limit": limit,
        "filters[state.keyword]": state,
        "filters[commodity]": commodity,
    })
    url = f"{AGMARKNET_URL}?{params}"

    req = urllib.request.Request(url, headers={"User-Agent": "Prithvi/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())

    return data.get("records", [])


def store_mandi_records(records: list, crop_type: str, source_name: str = "agmarknet") -> int:
    """Insert mandi price records into mandi_price_snapshots. Returns count inserted."""
    if not records:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0

    try:
        for rec in records:
            try:
                # Parse arrival date DD/MM/YYYY → YYYY-MM-DD
                raw_date = rec.get("arrival_date", "")
                if raw_date:
                    parts = raw_date.split("/")
                    snapshot_date = f"{parts[2]}-{parts[1]}-{parts[0]}" if len(parts) == 3 else raw_date
                else:
                    from datetime import date
                    snapshot_date = str(date.today())

                cursor.execute(
                    """
                    INSERT INTO mandi_price_snapshots
                        (crop_type, variety, market_name, district, state,
                         snapshot_date, min_price, modal_price, max_price,
                         source_name, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (
                        crop_type,
                        rec.get("variety"),
                        rec.get("market"),
                        rec.get("district"),
                        rec.get("state"),
                        snapshot_date,
                        rec.get("min_price"),
                        rec.get("modal_price"),
                        rec.get("max_price"),
                        source_name,
                        json.dumps(rec),
                    ),
                )
                inserted += 1
            except Exception as row_exc:
                logger.warning("Skipped mandi row: %s", row_exc)
                continue

        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        cursor.close()
        conn.close()

    return inserted


@app.get("/mandi-prices/latest", tags=["Passive Data"])
def get_latest_mandi_prices(user: dict = Depends(get_current_user)):
    """
    Returns the single most recent modal price per crop_type per market
    from mandi_price_snapshots. Useful for dashboard price widgets.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT DISTINCT ON (crop_type, market_name)
                crop_type, variety, market_name, district, state,
                snapshot_date, min_price, modal_price, max_price, source_name
            FROM mandi_price_snapshots
            ORDER BY crop_type, market_name, snapshot_date DESC, id DESC;
            """
        )
        rows = cursor.fetchall()
        return {
            "count": len(rows),
            "prices": [
                {
                    "crop_type":     row[0],
                    "variety":       row[1],
                    "market":        row[2],
                    "district":      row[3],
                    "state":         row[4],
                    "date":          str(row[5]),
                    "min_price":     float(row[6]) if row[6] else None,
                    "modal_price":   float(row[7]) if row[7] else None,
                    "max_price":     float(row[8]) if row[8] else None,
                    "source":        row[9],
                }
                for row in rows
            ],
        }
    finally:
        cursor.close()
        conn.close()


@app.post("/mandi-prices/sync", tags=["Passive Data"])
def sync_mandi_prices(user: dict = Depends(get_current_user)):
    """
    Fetches latest mandi prices from data.gov.in Agmarknet for
    wheat, soybean, and rice in Madhya Pradesh and stores them.
    Admin only.
    """
    ensure_admin(user)

    results = []
    errors  = []

    for target in MANDI_TARGETS:
        try:
            records = fetch_mandi_records(target["commodity"], target["state"])
            count   = store_mandi_records(records, target["crop_type"])
            results.append({
                "commodity": target["commodity"],
                "fetched":   len(records),
                "inserted":  count,
            })
            logger.info("Mandi sync: %s → %d fetched, %d inserted", target["commodity"], len(records), count)
        except Exception as exc:
            errors.append({"commodity": target["commodity"], "error": str(exc)})
            logger.error("Mandi sync failed for %s: %s", target["commodity"], exc)

    return {
        "message": "Mandi sync complete",
        "results": results,
        "errors":  errors,
    }
