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
from fastapi import Depends, FastAPI, Header, HTTPException, status
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
    village: str
    district: str
    state: str
    land_acres: float

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

    @validator("land_acres")
    def farmer_land(cls, value):
        if value <= 0:
            raise ValueError("Land acres must be greater than zero")
        return value


class NewCrop(BaseModel):
    farmer_id: int
    parcel_id: Optional[int] = None
    crop_type: str
    variety: str
    season: str
    year: int
    area_acres: float
    sowing_date: str
    expected_harvest: str
    expected_yield_quintal: float

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


class NewParcel(BaseModel):
    farmer_id: int
    plot_name: str
    area_acres: float
    location: Optional[str] = None
    survey_number: Optional[str] = None
    soil_type: Optional[str] = None
    irrigation_source: Optional[str] = None
    ownership_type: str = "owned"
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

    @validator("ownership_type")
    def parcel_ownership(cls, value):
        normalized = value.lower().strip()
        if normalized not in ["owned", "leased", "shared"]:
            raise ValueError("Ownership type must be owned, leased, or shared")
        return normalized


class NewCost(BaseModel):
    crop_id: int
    stage: str
    item_name: str
    quantity: float
    unit: str
    amount: float

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
    selling_price: float
    buyer: str

    @validator("yield_quintal", "selling_price")
    def harvest_positive(cls, value):
        if value <= 0:
            raise ValueError("Value must be greater than zero")
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
    sale_date: str
    quantity_quintal: float
    price_per_quintal: float
    buyer: str
    amount_received: float = 0
    notes: Optional[str] = None

    @validator("quantity_quintal", "price_per_quintal")
    def deal_positive(cls, value):
        if value <= 0:
            raise ValueError("Value must be greater than zero")
        return value

    @validator("amount_received")
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
            lp.id, lp.farmer_id, f.name, lp.plot_name, lp.area_acres, lp.location,
            lp.survey_number, lp.soil_type, lp.irrigation_source, lp.ownership_type,
            lp.latitude, lp.longitude
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
                "area_acres": float(row[4]),
                "location": row[5],
                "survey_number": row[6],
                "soil_type": row[7],
                "irrigation_source": row[8],
                "ownership_type": row[9],
                "latitude": float(row[10]) if row[10] is not None else None,
                "longitude": float(row[11]) if row[11] is not None else None,
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
            lp.id, lp.farmer_id, f.name, lp.plot_name, lp.area_acres, lp.location,
            lp.survey_number, lp.soil_type, lp.irrigation_source, lp.ownership_type,
            lp.latitude, lp.longitude
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
        "area_acres": float(row[4]),
        "location": row[5],
        "survey_number": row[6],
        "soil_type": row[7],
        "irrigation_source": row[8],
        "ownership_type": row[9],
        "latitude": float(row[10]) if row[10] is not None else None,
        "longitude": float(row[11]) if row[11] is not None else None,
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
        SELECT f.name, f.village, c.crop_type, c.variety, c.current_stage, SUM(i.amount) AS total_cost
        FROM farmers f
        JOIN crops c ON c.farmer_id = f.id
        JOIN input_costs i ON i.crop_id = c.id
        WHERE f.name = %s
        GROUP BY f.name, f.village, c.crop_type, c.variety, c.current_stage;
        """,
        (name,),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": f"No farmer found: {name}"}
    return {"farmer": row[0], "village": row[1], "crop": row[2], "variety": row[3], "stage": row[4], "total_cost": float(row[5])}


@app.get("/crop/{crop_id}")
def get_crop(crop_id: int, user: dict = Depends(get_current_user)):
    ensure_crop_access(user, crop_id)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            c.id, f.name, lp.id, lp.plot_name, c.crop_type, c.variety, c.season, c.year,
            c.sowing_date, c.expected_harvest, c.expected_yield_quintal,
            c.current_stage, COALESCE(SUM(i.amount), 0) AS total_cost
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        LEFT JOIN land_parcels lp ON lp.id = c.parcel_id
        LEFT JOIN input_costs i ON i.crop_id = c.id
        WHERE c.id = %s
        GROUP BY c.id, f.name, lp.id, lp.plot_name, c.crop_type, c.variety, c.season, c.year,
                 c.sowing_date, c.expected_harvest, c.expected_yield_quintal, c.current_stage;
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
        "crop": row[4],
        "variety": row[5],
        "season": row[6],
        "year": row[7],
        "sowing_date": str(row[8]),
        "expected_harvest": str(row[9]),
        "expected_yield_quintal": float(row[10]) if row[10] is not None else None,
        "stage": row[11],
        "total_cost": float(row[12]),
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
            INSERT INTO farmers (name, phone, village, district, state, land_acres)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (farmer.name, farmer.phone, farmer.village, farmer.district, farmer.state, farmer.land_acres),
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
                (farmer_id, plot_name, area_acres, location, survey_number, soil_type,
                 irrigation_source, ownership_type, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                parcel.farmer_id,
                parcel.plot_name,
                parcel.area_acres,
                parcel.location,
                parcel.survey_number,
                parcel.soil_type,
                parcel.irrigation_source,
                parcel.ownership_type,
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
                (farmer_id, parcel_id, crop_type, variety, season, year, sowing_date,
                 expected_harvest, expected_yield_quintal, current_stage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'sowing')
            RETURNING id;
            """,
            (
                crop.farmer_id,
                crop.parcel_id,
                crop.crop_type,
                crop.variety,
                crop.season,
                crop.year,
                crop.sowing_date,
                crop.expected_harvest,
                crop.expected_yield_quintal,
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
            INSERT INTO input_costs (crop_id, stage, item_name, quantity, unit, amount)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (cost.crop_id, cost.stage, cost.item_name, cost.quantity, cost.unit, cost.amount),
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
            INSERT INTO harvests (crop_id, harvest_date, yield_quintal, selling_price, buyer, revenue)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (entry.crop_id, entry.harvest_date, entry.yield_quintal, entry.selling_price, entry.buyer, revenue),
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
    amount_received = min(entry.amount_received, gross_amount)
    payment_status = get_payment_status(amount_received, gross_amount)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO deals
                (crop_id, sale_date, quantity_quintal, price_per_quintal, buyer,
                 gross_amount, amount_received, payment_status, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (entry.crop_id, entry.sale_date, entry.quantity_quintal, entry.price_per_quintal, entry.buyer, gross_amount, amount_received, payment_status, entry.notes),
        )
        deal_id = cursor.fetchone()[0]
        cursor.execute("UPDATE crops SET current_stage = CASE WHEN %s = 'paid' THEN 'storage' ELSE 'logistics' END WHERE id = %s;", (payment_status, entry.crop_id))
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Deal recorded", "deal_id": deal_id, "gross_amount": gross_amount, "amount_received": amount_received, "payment_status": payment_status}
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
    cursor.execute("SELECT id, name, phone, village, district, state, land_acres FROM farmers WHERE name = %s;", (name,))
    farmer = cursor.fetchone()
    if farmer is None:
        cursor.close()
        conn.close()
        return {"error": "Farmer not found"}
    cursor.execute(
        """
        SELECT
            id, plot_name, area_acres, location, survey_number, soil_type,
            irrigation_source, ownership_type, latitude, longitude
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
            c.id, c.parcel_id, lp.plot_name, c.crop_type, c.variety, c.season, c.year, c.sowing_date, c.expected_harvest,
            c.expected_yield_quintal, c.current_stage,
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
    cursor.execute("SELECT c.id, i.id, i.stage, i.item_name, i.quantity, i.unit, i.amount, i.entry_date FROM crops c LEFT JOIN input_costs i ON i.crop_id = c.id WHERE c.farmer_id = %s ORDER BY c.id, i.id;", (farmer_id,))
    cost_rows = cursor.fetchall()
    cursor.execute("SELECT c.id, h.id, h.harvest_date, h.yield_quintal, h.selling_price, h.buyer, h.revenue FROM crops c LEFT JOIN harvests h ON h.crop_id = c.id WHERE c.farmer_id = %s ORDER BY c.id, h.id;", (farmer_id,))
    harvest_rows = cursor.fetchall()
    cursor.execute("SELECT c.id, d.id, d.sale_date, d.quantity_quintal, d.price_per_quintal, d.buyer, d.gross_amount, d.amount_received, d.payment_status, d.notes FROM crops c LEFT JOIN deals d ON d.crop_id = c.id WHERE c.farmer_id = %s ORDER BY c.id, d.id;", (farmer_id,))
    deal_rows = cursor.fetchall()
    cursor.close()
    conn.close()
    crops = []
    for row in crop_rows:
        crop_id = row[0]
        crops.append({
            "crop_id": crop_id,
            "parcel_id": row[1],
            "parcel_name": row[2],
            "crop": row[3],
            "variety": row[4],
            "season": row[5],
            "year": row[6],
            "sowing_date": str(row[7]),
            "expected_harvest": str(row[8]),
            "expected_yield_quintal": float(row[9]) if row[9] is not None else None,
            "stage": row[10],
            "total_cost": float(row[11]),
            "total_revenue": float(row[12]),
            "total_yield_quintal": float(row[13]),
            "gross_sales": float(row[14]),
            "amount_received": float(row[15]),
            "costs": [
                {"cost_id": cost_row[1], "stage": cost_row[2], "item_name": cost_row[3], "quantity": float(cost_row[4]), "unit": cost_row[5], "amount": float(cost_row[6]), "entry_date": str(cost_row[7])}
                for cost_row in cost_rows if cost_row[0] == crop_id and cost_row[1] is not None
            ],
            "harvests": [
                {"harvest_id": harvest_row[1], "harvest_date": str(harvest_row[2]), "yield_quintal": float(harvest_row[3]), "selling_price": float(harvest_row[4]), "buyer": harvest_row[5], "revenue": float(harvest_row[6])}
                for harvest_row in harvest_rows if harvest_row[0] == crop_id and harvest_row[1] is not None
            ],
            "deals": [
                {"deal_id": deal_row[1], "sale_date": str(deal_row[2]), "quantity_quintal": float(deal_row[3]), "price_per_quintal": float(deal_row[4]), "buyer": deal_row[5], "gross_amount": float(deal_row[6]), "amount_received": float(deal_row[7]), "payment_status": deal_row[8], "notes": deal_row[9]}
                for deal_row in deal_rows if deal_row[0] == crop_id and deal_row[1] is not None
            ],
        })
    total_cost = sum(crop["total_cost"] for crop in crops)
    gross_sales = sum(crop["gross_sales"] or crop["total_revenue"] for crop in crops)
    amount_received = sum(crop["amount_received"] or crop["total_revenue"] for crop in crops)
    return {
        "farmer": {"farmer_id": farmer[0], "name": farmer[1], "phone": farmer[2], "village": farmer[3], "district": farmer[4], "state": farmer[5], "land_acres": float(farmer[6])},
        "parcels": [
            {
                "parcel_id": row[0],
                "plot_name": row[1],
                "area_acres": float(row[2]),
                "location": row[3],
                "survey_number": row[4],
                "soil_type": row[5],
                "irrigation_source": row[6],
                "ownership_type": row[7],
                "latitude": float(row[8]) if row[8] is not None else None,
                "longitude": float(row[9]) if row[9] is not None else None,
            }
            for row in parcel_rows
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
