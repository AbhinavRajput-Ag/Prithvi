# api.py
# Prithvi — Agricultural Operating System
# Version 1.2 — analytics, alerts, and richer crop economics

import os
import logging
from pathlib import Path
from contextlib import closing
from fastapi import FastAPI
from pydantic import BaseModel, validator
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

# ── LOGGING ──────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── APP ──────────────────────────────────────────
app = FastAPI(
    title="Prithvi API",
    version="1.2",
    description="India's Agricultural Operating System — Crop Ledger Backend"
)

# ── DATABASE CONNECTION ───────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def database_is_ready():
    try:
        with closing(get_connection()) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                cursor.fetchone()
        return True
    except Exception as exc:
        logger.error(f"Database health check failed: {exc}")
        return False

def to_day_count(value):
    return value.days if hasattr(value, "days") else int(value)

def stage_rank(stage):
    order = {
        "sowing": 1,
        "growing": 2,
        "harvest": 3,
        "logistics": 4,
        "storage": 5
    }
    return order.get(stage, 0)

def handle_db_error(e, conn, cursor):
    conn.rollback()
    cursor.close()
    conn.close()
    error = str(e)
    if "duplicate key" in error:
        return {"error": "This record already exists — duplicate entry"}
    if "foreign key" in error:
        return {"error": "Referenced ID does not exist — check farmer_id or crop_id"}
    if "not-null" in error:
        return {"error": "A required field is missing"}
    logger.error(f"Database error: {error}")
    return {"error": "Something went wrong. Please try again."}

# ── DATA MODELS ───────────────────────────────────

class NewFarmer(BaseModel):
    name: str
    phone: str
    village: str
    district: str
    state: str
    land_acres: float

    @validator('name')
    def name_not_empty(cls, v):
        if not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip()

    @validator('phone')
    def phone_valid(cls, v):
        clean = v.strip()
        if not clean.isdigit():
            raise ValueError('Phone must contain only numbers')
        if len(clean) != 10:
            raise ValueError('Phone must be exactly 10 digits')
        return clean

    @validator('land_acres')
    def acres_positive(cls, v):
        if v <= 0:
            raise ValueError('Land acres must be greater than zero')
        return v

class NewCrop(BaseModel):
    farmer_id: int
    crop_type: str
    variety: str
    season: str
    year: int
    area_acres: float
    sowing_date: str
    expected_harvest: str
    expected_yield_quintal: float

    @validator('season')
    def season_valid(cls, v):
        if v.lower() not in ['kharif', 'rabi', 'zaid']:
            raise ValueError('Season must be kharif, rabi, or zaid')
        return v.lower()

    @validator('year')
    def year_valid(cls, v):
        if v < 2000 or v > 2100:
            raise ValueError('Year must be between 2000 and 2100')
        return v

    @validator('area_acres')
    def acres_positive(cls, v):
        if v <= 0:
            raise ValueError('Area must be greater than zero')
        return v

    @validator('expected_yield_quintal')
    def yield_positive(cls, v):
        if v <= 0:
            raise ValueError('Expected yield must be greater than zero')
        return v

    @validator('crop_type', 'variety')
    def not_empty(cls, v):
        if not v.strip():
            raise ValueError('Field cannot be empty')
        return v.strip()

class NewCost(BaseModel):
    crop_id: int
    stage: str
    item_name: str
    quantity: float
    unit: str
    amount: float

    @validator('stage')
    def stage_valid(cls, v):
        valid = ['sowing', 'growing', 'harvest', 'logistics', 'storage']
        if v.lower() not in valid:
            raise ValueError(f'Stage must be one of: {valid}')
        return v.lower()

    @validator('amount')
    def amount_positive(cls, v):
        if v <= 0:
            raise ValueError('Amount must be greater than zero')
        return v

    @validator('quantity')
    def quantity_positive(cls, v):
        if v <= 0:
            raise ValueError('Quantity must be greater than zero')
        return v

    @validator('item_name')
    def item_not_empty(cls, v):
        if not v.strip():
            raise ValueError('Item name cannot be empty')
        return v.strip()

class HarvestEntry(BaseModel):
    crop_id: int
    harvest_date: str
    yield_quintal: float
    selling_price: float
    buyer: str

    @validator('yield_quintal')
    def yield_positive(cls, v):
        if v <= 0:
            raise ValueError("Yield must be greater than zero")
        return v

    @validator('selling_price')
    def price_positive(cls, v):
        if v <= 0:
            raise ValueError("Selling price must be greater than zero")
        return v

    @validator('buyer')
    def buyer_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Buyer cannot be empty")
        return v.strip()

class CropStageUpdate(BaseModel):
    stage: str

    @validator('stage')
    def stage_valid(cls, v):
        valid = ['sowing', 'growing', 'harvest', 'logistics', 'storage']
        if v.lower() not in valid:
            raise ValueError(f"Stage must be one of {valid}")
        return v.lower()

class CropYieldUpdate(BaseModel):
    expected_yield_quintal: float

    @validator('expected_yield_quintal')
    def yield_positive(cls, v):
        if v <= 0:
            raise ValueError("Expected yield must be greater than zero")
        return v

# ── ROUTES ────────────────────────────────────────

@app.get("/",
    summary="System info",
    description="Returns Prithvi system name and version")
def home():
    return {
        "system": "Prithvi",
        "version": "1.2",
        "message": "India's Agricultural Operating System",
        "database_ready": database_is_ready()
    }

@app.get("/health",
    summary="Health check",
    description="Checks whether the API and database connection are healthy.")
def health_check():
    ready = database_is_ready()
    return {
        "status": "ok" if ready else "degraded",
        "database_ready": ready
    }

@app.get("/dashboard/summary",
    summary="Get dashboard summary",
    description="Returns top-line portfolio metrics, stage distribution, and key operational counts.")
def get_dashboard_summary():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
        WITH crop_costs AS (
            SELECT crop_id, SUM(amount) AS total_cost
            FROM input_costs
            GROUP BY crop_id
        ),
        crop_harvests AS (
            SELECT crop_id, SUM(revenue) AS total_revenue, SUM(yield_quintal) AS total_yield
            FROM harvests
            GROUP BY crop_id
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
    """)
    summary = cursor.fetchone()

    cursor.execute("""
        SELECT current_stage AS stage, COUNT(*) AS crop_count
        FROM crops
        GROUP BY current_stage
        ORDER BY current_stage;
    """)
    stage_distribution = cursor.fetchall()

    cursor.execute("""
        SELECT
            c.id AS crop_id,
            f.name AS farmer,
            c.crop_type,
            c.expected_harvest,
            (c.expected_harvest - CURRENT_DATE) AS days_to_harvest
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        WHERE c.expected_harvest IS NOT NULL
          AND c.expected_harvest BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '15 days'
        ORDER BY c.expected_harvest, c.id;
    """)
    upcoming_harvests = cursor.fetchall()

    cursor.close()
    conn.close()

    total_cost = float(summary["total_cost"] or 0)
    total_revenue = float(summary["total_revenue"] or 0)
    profit = total_revenue - total_cost

    return {
        "totals": {
            "farmers": summary["total_farmers"],
            "crops": summary["total_crops"],
            "active_crops": summary["active_crops"],
            "harvest_entries": summary["harvest_entries"],
            "cost": total_cost,
            "revenue": total_revenue,
            "profit": profit,
            "yield_quintal": float(summary["total_yield_quintal"] or 0)
        },
        "stage_distribution": [
            {"stage": row["stage"], "crop_count": row["crop_count"]}
            for row in stage_distribution
        ],
        "upcoming_harvests": [
            {
                "crop_id": row["crop_id"],
                "farmer": row["farmer"],
                "crop": row["crop_type"],
                "expected_harvest": str(row["expected_harvest"]),
                "days_to_harvest": to_day_count(row["days_to_harvest"])
            }
            for row in upcoming_harvests
        ]
    }

@app.get("/dashboard/fpo-summary",
    summary="Get FPO portfolio summary",
    description="Returns a demo-friendly portfolio view with top farmers, revenue leaders, and crops needing attention.")
def get_fpo_summary():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
        WITH crop_costs AS (
            SELECT crop_id, SUM(amount) AS total_cost
            FROM input_costs
            GROUP BY crop_id
        ),
        crop_harvests AS (
            SELECT crop_id,
                   SUM(revenue) AS total_revenue,
                   SUM(yield_quintal) AS total_yield
            FROM harvests
            GROUP BY crop_id
        ),
        farmer_rollup AS (
            SELECT
                f.id,
                f.name,
                f.village,
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
        SELECT
            id AS farmer_id,
            name,
            village,
            crop_count,
            total_cost,
            total_revenue,
            total_yield,
            (total_revenue - total_cost) AS profit
        FROM farmer_rollup
        ORDER BY total_revenue DESC, total_cost DESC, name;
    """)
    farmer_rows = cursor.fetchall()

    cursor.execute("""
        SELECT
            c.id AS crop_id,
            f.name AS farmer,
            c.crop_type,
            c.current_stage,
            c.expected_harvest,
            c.expected_yield_quintal,
            COALESCE(costs.total_cost, 0) AS total_cost,
            COALESCE(harvests.total_revenue, 0) AS revenue
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        LEFT JOIN (
            SELECT crop_id, SUM(amount) AS total_cost
            FROM input_costs
            GROUP BY crop_id
        ) costs ON costs.crop_id = c.id
        LEFT JOIN (
            SELECT crop_id, SUM(revenue) AS total_revenue
            FROM harvests
            GROUP BY crop_id
        ) harvests ON harvests.crop_id = c.id
        WHERE c.expected_yield_quintal IS NULL
           OR c.current_stage IN ('sowing', 'growing')
           OR c.expected_harvest BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '15 days'
        ORDER BY c.expected_harvest NULLS LAST, c.id;
    """)
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
            "profit": float(row["profit"] or 0)
        }
        for row in farmer_rows
    ]

    return {
        "portfolio_totals": {
            "farmers": len(farmers),
            "profitable_farmers": sum(1 for row in farmers if row["profit"] > 0),
            "revenue_generating_farmers": sum(1 for row in farmers if row["total_revenue"] > 0)
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
                "revenue": float(row["revenue"] or 0)
            }
            for row in attention_rows
        ]
    }

@app.get("/farmers",
    summary="List all farmers",
    description="Returns all registered farmers with village and land details")
def get_all_farmers():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, village, land_acres FROM farmers;")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"farmers": [
        {"id": r[0], "name": r[1], "village": r[2], "land_acres": float(r[3])}
        for r in rows
    ]}

@app.get("/farmer/{name}",
    summary="Get farmer profile",
    description="Returns crop, cost, and pipeline stage for a specific farmer")
def get_farmer(name: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT f.name, f.village, c.crop_type, c.variety,
               c.current_stage, SUM(i.amount) AS total_cost
        FROM farmers f
        JOIN crops c ON c.farmer_id = f.id
        JOIN input_costs i ON i.crop_id = c.id
        WHERE f.name = %s
        GROUP BY f.name, f.village, c.crop_type, c.variety, c.current_stage;
    """, (name,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": f"No farmer found: {name}"}
    return {
        "farmer": row[0], "village": row[1],
        "crop": row[2], "variety": row[3],
        "stage": row[4], "total_cost": float(row[5])
    }

@app.get("/crop/{crop_id}",
    summary="Get crop ledger",
    description="Returns the crop record with farmer, stage, and total cost details.")
def get_crop(crop_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            c.id,
            f.name,
            c.crop_type,
            c.variety,
            c.season,
            c.year,
            c.sowing_date,
            c.expected_harvest,
            c.expected_yield_quintal,
            c.current_stage,
            COALESCE(SUM(i.amount), 0) AS total_cost
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        LEFT JOIN input_costs i ON i.crop_id = c.id
        WHERE c.id = %s
        GROUP BY
            c.id, f.name, c.crop_type, c.variety, c.season, c.year,
            c.sowing_date, c.expected_harvest, c.expected_yield_quintal, c.current_stage;
    """, (crop_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row is None:
        return {"error": "Crop not found"}

    return {
        "crop_id": row[0],
        "farmer": row[1],
        "crop": row[2],
        "variety": row[3],
        "season": row[4],
        "year": row[5],
        "sowing_date": str(row[6]),
        "expected_harvest": str(row[7]),
        "expected_yield_quintal": float(row[8]) if row[8] is not None else None,
        "stage": row[9],
        "total_cost": float(row[10])
    }

@app.get("/crop/{crop_id}/economics",
    summary="Get crop economics",
    description="Returns cost, realized revenue, realized profit, and break-even metrics for one crop.")
def get_crop_economics(crop_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            c.id,
            f.name,
            c.crop_type,
            c.expected_yield_quintal,
            COALESCE(costs.total_cost, 0) AS total_cost,
            COALESCE(harvests.total_revenue, 0) AS revenue,
            COALESCE(harvests.total_yield_quintal, 0) AS harvested_yield
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        LEFT JOIN (
            SELECT crop_id, SUM(amount) AS total_cost
            FROM input_costs
            GROUP BY crop_id
        ) costs ON costs.crop_id = c.id
        LEFT JOIN (
            SELECT crop_id,
                   SUM(revenue) AS total_revenue,
                   SUM(yield_quintal) AS total_yield_quintal
            FROM harvests
            GROUP BY crop_id
        ) harvests ON harvests.crop_id = c.id
        WHERE c.id = %s;
    """, (crop_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row is None:
        return {"error": "Crop not found"}

    total_cost = float(row[4])
    revenue = float(row[5])
    profit = revenue - total_cost
    expected_yield = float(row[3]) if row[3] is not None else None
    break_even = None if not expected_yield else round(total_cost / expected_yield, 2)

    return {
        "crop_id": row[0],
        "farmer": row[1],
        "crop": row[2],
        "expected_yield_quintal": expected_yield,
        "harvested_yield_quintal": float(row[6]),
        "total_cost": total_cost,
        "revenue": revenue,
        "profit": profit,
        "margin_percent": 0 if revenue == 0 else round((profit / revenue) * 100, 2),
        "breakeven_per_quintal": break_even
    }

@app.get("/crop/{crop_id}/costs",
    summary="Get crop costs",
    description="Returns every cost entry for a crop along with stage totals.")
def get_crop_costs(crop_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, stage, item_name, quantity, unit, amount, entry_date
        FROM input_costs
        WHERE crop_id = %s
        ORDER BY id;
    """, (crop_id,))
    rows = cursor.fetchall()

    if not rows:
        cursor.close()
        conn.close()
        return {"crop_id": crop_id, "costs": [], "stage_totals": [], "total_cost": 0.0}

    cursor.execute("""
        SELECT stage, SUM(amount) AS stage_total
        FROM input_costs
        WHERE crop_id = %s
        GROUP BY stage
        ORDER BY stage;
    """, (crop_id,))
    stage_rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return {
        "crop_id": crop_id,
        "costs": [
            {
                "cost_id": row[0],
                "stage": row[1],
                "item_name": row[2],
                "quantity": float(row[3]),
                "unit": row[4],
                "amount": float(row[5]),
                "entry_date": str(row[6])
            }
            for row in rows
        ],
        "stage_totals": [
            {"stage": row[0], "total": float(row[1])}
            for row in stage_rows
        ],
        "total_cost": float(sum(row[5] for row in rows))
    }

@app.get("/crop/{crop_id}/harvests",
    summary="Get crop harvest history",
    description="Returns all harvest entries for a crop with total yield and revenue.")
def get_crop_harvests(crop_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, harvest_date, yield_quintal, selling_price, buyer, revenue
        FROM harvests
        WHERE crop_id = %s
        ORDER BY id;
    """, (crop_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        return {"crop_id": crop_id, "harvests": [], "total_yield_quintal": 0.0, "total_revenue": 0.0}

    return {
        "crop_id": crop_id,
        "harvests": [
            {
                "harvest_id": row[0],
                "harvest_date": str(row[1]),
                "yield_quintal": float(row[2]),
                "selling_price": float(row[3]),
                "buyer": row[4],
                "revenue": float(row[5])
            }
            for row in rows
        ],
        "total_yield_quintal": float(sum(row[2] for row in rows)),
        "total_revenue": float(sum(row[5] for row in rows))
    }

@app.get("/alerts/overview",
    summary="Get operational alerts",
    description="Returns crops that need attention, including missing yields, missing costs, and upcoming harvests.")
def get_alerts_overview():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
        SELECT c.id AS crop_id, f.name AS farmer, c.crop_type, c.current_stage
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        WHERE c.expected_yield_quintal IS NULL
        ORDER BY c.id;
    """)
    missing_yield = cursor.fetchall()

    cursor.execute("""
        SELECT c.id AS crop_id, f.name AS farmer, c.crop_type, c.current_stage
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        LEFT JOIN input_costs i ON i.crop_id = c.id
        WHERE i.id IS NULL
        ORDER BY c.id;
    """)
    missing_costs = cursor.fetchall()

    cursor.execute("""
        SELECT
            c.id AS crop_id,
            f.name AS farmer,
            c.crop_type,
            c.expected_harvest,
            (c.expected_harvest - CURRENT_DATE) AS days_to_harvest
        FROM crops c
        JOIN farmers f ON f.id = c.farmer_id
        WHERE c.expected_harvest IS NOT NULL
          AND c.expected_harvest BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '15 days'
        ORDER BY c.expected_harvest, c.id;
    """)
    upcoming_harvests = cursor.fetchall()

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
                "days_to_harvest": to_day_count(row["days_to_harvest"])
            }
            for row in upcoming_harvests
        ]
    }

@app.get("/breakeven/{name}",
    summary="Get break-even price",
    description="Calculates minimum selling price per quintal for a farmer to recover all costs")
def get_breakeven(name: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            f.name,
            c.crop_type,
            SUM(i.amount) AS total_cost,
            c.expected_yield_quintal,
            ROUND((SUM(i.amount) / NULLIF(c.expected_yield_quintal, 0))::numeric, 2) AS breakeven
        FROM farmers f
        JOIN crops c ON c.farmer_id = f.id
        JOIN input_costs i ON i.crop_id = c.id
        WHERE f.name = %s
        GROUP BY f.name, c.crop_type, c.expected_yield_quintal;
    """, (name,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row is None:
        return {"error": f"No data found: {name}"}
    if row[3] is None or row[4] is None:
        return {"error": f"Expected yield is missing for: {name}"}
    return {
        "farmer": row[0], "crop": row[1],
        "total_cost": float(row[2]),
        "expected_yield_quintal": int(row[3]),
        "breakeven_per_quintal": float(row[4])
    }

@app.post("/farmers/add",
    summary="Register new farmer",
    description="Creates a farmer profile. Phone must be 10 digits and unique.")
def add_farmer(farmer: NewFarmer):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO farmers (name, phone, village, district, state, land_acres)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
        """, (farmer.name, farmer.phone, farmer.village,
              farmer.district, farmer.state, farmer.land_acres))
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"New farmer registered: {farmer.name} — ID {new_id}")
        return {"message": "Farmer registered successfully",
                "farmer_id": new_id, "name": farmer.name}
    except Exception as e:
        return handle_db_error(e, conn, cursor)

@app.post("/crops/add",
    summary="Register new crop",
    description="Opens a crop ledger for a farmer. Season must be kharif, rabi, or zaid.")
def add_crop(crop: NewCrop):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO crops
                (farmer_id, crop_type, variety, season, year,
                 sowing_date, expected_harvest, expected_yield_quintal, current_stage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'sowing') RETURNING id;
        """, (crop.farmer_id, crop.crop_type, crop.variety,
              crop.season, crop.year, crop.sowing_date,
              crop.expected_harvest, crop.expected_yield_quintal))
        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Crop registered: {crop.crop_type} for farmer_id {crop.farmer_id} — crop_id {new_id}")
        return {"message": "Crop registered successfully",
                "crop_id": new_id, "crop": crop.crop_type,
                "farmer_id": crop.farmer_id}
    except Exception as e:
        return handle_db_error(e, conn, cursor)

@app.post("/harvests/add",
    summary="Log harvest data",
    description="Creates a harvest ledger entry and stores the computed revenue.")
def add_harvest(entry: HarvestEntry):
    revenue = entry.yield_quintal * entry.selling_price

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT expected_yield_quintal, current_stage
            FROM crops
            WHERE id = %s;
        """, (entry.crop_id,))
        crop = cursor.fetchone()

        if crop is None:
            cursor.close()
            conn.close()
            return {"error": "Crop not found"}

        cursor.execute("""
            SELECT id
            FROM harvests
            WHERE crop_id = %s
              AND harvest_date = %s
              AND buyer = %s
              AND yield_quintal = %s
              AND selling_price = %s;
        """, (entry.crop_id, entry.harvest_date, entry.buyer,
              entry.yield_quintal, entry.selling_price))
        duplicate = cursor.fetchone()

        if duplicate is not None:
            cursor.close()
            conn.close()
            return {"error": "This harvest entry already exists"}

        expected_yield = crop[0]
        cursor.execute("""
            SELECT COALESCE(SUM(yield_quintal), 0)
            FROM harvests
            WHERE crop_id = %s;
        """, (entry.crop_id,))
        harvested_so_far = float(cursor.fetchone()[0])

        if expected_yield is not None and harvested_so_far + entry.yield_quintal > float(expected_yield) * 1.25:
            cursor.close()
            conn.close()
            return {"error": "Harvest exceeds expected yield by more than 25%. Please verify the entry."}

        cursor.execute("""
            INSERT INTO harvests
                (crop_id, harvest_date, yield_quintal, selling_price, buyer, revenue)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (entry.crop_id, entry.harvest_date, entry.yield_quintal,
              entry.selling_price, entry.buyer, revenue))
        hid = cursor.fetchone()[0]

        cursor.execute("""
            UPDATE crops
            SET current_stage = 'harvest'
            WHERE id = %s AND current_stage IN ('sowing', 'growing');
        """, (entry.crop_id,))

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "message": "Harvest logged",
            "harvest_id": hid,
            "revenue": revenue
        }
    except Exception as e:
        return handle_db_error(e, conn, cursor)

@app.patch("/crops/{crop_id}/stage",
    summary="Update crop stage",
    description="Moves a crop ledger entry to a new operational stage.")
def update_crop_stage(crop_id: int, data: CropStageUpdate):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE crops
            SET current_stage = %s
            WHERE id = %s
            RETURNING id;
        """, (data.stage, crop_id))
        row = cursor.fetchone()

        conn.commit()
        cursor.close()
        conn.close()

        if row is None:
            return {"error": "Crop not found"}

        return {
            "message": "Stage updated",
            "crop_id": crop_id,
            "new_stage": data.stage
        }
    except Exception as e:
        return handle_db_error(e, conn, cursor)

@app.patch("/crops/{crop_id}/yield",
    summary="Update expected crop yield",
    description="Updates the expected yield for a crop so break-even and planning stay accurate.")
def update_crop_yield(crop_id: int, data: CropYieldUpdate):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE crops
            SET expected_yield_quintal = %s
            WHERE id = %s
            RETURNING id;
        """, (data.expected_yield_quintal, crop_id))
        row = cursor.fetchone()

        conn.commit()
        cursor.close()
        conn.close()

        if row is None:
            return {"error": "Crop not found"}

        return {
            "message": "Expected yield updated",
            "crop_id": crop_id,
            "expected_yield_quintal": data.expected_yield_quintal
        }
    except Exception as e:
        return handle_db_error(e, conn, cursor)

@app.get("/farmer/{name}/economics",
    summary="Get farmer economics",
    description="Returns aggregate cost, revenue, profit, and margin for a farmer.")
def farmer_economics(name: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            f.name,
            COALESCE(SUM(costs.total_cost), 0) AS total_cost,
            COALESCE(SUM(harvests.revenue), 0) AS revenue
        FROM farmers f
        JOIN crops c ON c.farmer_id = f.id
        LEFT JOIN (
            SELECT crop_id, SUM(amount) AS total_cost
            FROM input_costs
            GROUP BY crop_id
        ) costs ON costs.crop_id = c.id
        LEFT JOIN (
            SELECT crop_id, SUM(revenue) AS revenue
            FROM harvests
            GROUP BY crop_id
        ) harvests ON harvests.crop_id = c.id
        WHERE f.name = %s
        GROUP BY f.name;
    """, (name,))

    row = cursor.fetchone()

    cursor.close()
    conn.close()

    if row is None:
        return {"error": "Farmer not found"}

    total_cost = float(row[1])
    revenue = float(row[2])
    profit = revenue - total_cost
    margin = 0 if revenue == 0 else round((profit / revenue) * 100, 2)

    return {
        "farmer": row[0],
        "total_cost": total_cost,
        "revenue": revenue,
        "profit": profit,
        "margin_percent": margin
    }

@app.get("/farmer/{name}/full-ledger",
    summary="Get full farmer ledger",
    description="Returns the farmer profile, crops, costs, harvests, and economics in one response.")
def get_farmer_full_ledger(name: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, name, phone, village, district, state, land_acres
        FROM farmers
        WHERE name = %s;
    """, (name,))
    farmer = cursor.fetchone()

    if farmer is None:
        cursor.close()
        conn.close()
        return {"error": "Farmer not found"}

    farmer_id = farmer[0]

    cursor.execute("""
        SELECT
            c.id,
            c.crop_type,
            c.variety,
            c.season,
            c.year,
            c.sowing_date,
            c.expected_harvest,
            c.expected_yield_quintal,
            c.current_stage,
            COALESCE(costs.total_cost, 0) AS total_cost,
            COALESCE(harvests.total_revenue, 0) AS total_revenue,
            COALESCE(harvests.total_yield_quintal, 0) AS total_yield_quintal
        FROM crops c
        LEFT JOIN (
            SELECT crop_id, SUM(amount) AS total_cost
            FROM input_costs
            GROUP BY crop_id
        ) costs ON costs.crop_id = c.id
        LEFT JOIN (
            SELECT crop_id,
                   SUM(revenue) AS total_revenue,
                   SUM(yield_quintal) AS total_yield_quintal
            FROM harvests
            GROUP BY crop_id
        ) harvests ON harvests.crop_id = c.id
        WHERE c.farmer_id = %s
        ORDER BY c.id;
    """, (farmer_id,))
    crop_rows = cursor.fetchall()

    cursor.execute("""
        SELECT
            c.id,
            i.id,
            i.stage,
            i.item_name,
            i.quantity,
            i.unit,
            i.amount,
            i.entry_date
        FROM crops c
        LEFT JOIN input_costs i ON i.crop_id = c.id
        WHERE c.farmer_id = %s
        ORDER BY c.id, i.id;
    """, (farmer_id,))
    cost_rows = cursor.fetchall()

    cursor.execute("""
        SELECT
            c.id,
            h.id,
            h.harvest_date,
            h.yield_quintal,
            h.selling_price,
            h.buyer,
            h.revenue
        FROM crops c
        LEFT JOIN harvests h ON h.crop_id = c.id
        WHERE c.farmer_id = %s
        ORDER BY c.id, h.id;
    """, (farmer_id,))
    harvest_rows = cursor.fetchall()
    cursor.close()
    conn.close()

    crops = []
    for row in crop_rows:
        crop_id = row[0]
        crop_costs = [
            {
                "cost_id": cost_row[1],
                "stage": cost_row[2],
                "item_name": cost_row[3],
                "quantity": float(cost_row[4]),
                "unit": cost_row[5],
                "amount": float(cost_row[6]),
                "entry_date": str(cost_row[7])
            }
            for cost_row in cost_rows
            if cost_row[0] == crop_id and cost_row[1] is not None
        ]
        crop_harvests = [
            {
                "harvest_id": harvest_row[1],
                "harvest_date": str(harvest_row[2]),
                "yield_quintal": float(harvest_row[3]),
                "selling_price": float(harvest_row[4]),
                "buyer": harvest_row[5],
                "revenue": float(harvest_row[6])
            }
            for harvest_row in harvest_rows
            if harvest_row[0] == crop_id and harvest_row[1] is not None
        ]

        crops.append({
            "crop_id": crop_id,
            "crop": row[1],
            "variety": row[2],
            "season": row[3],
            "year": row[4],
            "sowing_date": str(row[5]),
            "expected_harvest": str(row[6]),
            "expected_yield_quintal": float(row[7]) if row[7] is not None else None,
            "stage": row[8],
            "total_cost": float(row[9]),
            "total_revenue": float(row[10]),
            "total_yield_quintal": float(row[11]),
            "costs": crop_costs,
            "harvests": crop_harvests
        })

    total_cost = sum(crop["total_cost"] for crop in crops)
    revenue = sum(crop["total_revenue"] for crop in crops)
    profit = revenue - total_cost
    margin = 0 if revenue == 0 else round((profit / revenue) * 100, 2)

    return {
        "farmer": {
            "farmer_id": farmer[0],
            "name": farmer[1],
            "phone": farmer[2],
            "village": farmer[3],
            "district": farmer[4],
            "state": farmer[5],
            "land_acres": float(farmer[6])
        },
        "crops": crops,
        "economics": {
            "total_cost": total_cost,
            "revenue": revenue,
            "profit": profit,
            "margin_percent": margin
        }
    }

@app.post("/costs/add",
    summary="Log a cost",
    description="Adds a cost entry to the crop ledger and returns the updated running total.")
def add_cost(cost: NewCost):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT current_stage FROM crops WHERE id = %s;", (cost.crop_id,))
        crop = cursor.fetchone()
        if crop is None:
            cursor.close()
            conn.close()
            return {"error": "Crop not found"}

        cursor.execute("""
            INSERT INTO input_costs
                (crop_id, stage, item_name, quantity, unit, amount)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
        """, (cost.crop_id, cost.stage, cost.item_name,
              cost.quantity, cost.unit, cost.amount))
        new_id = cursor.fetchone()[0]

        current_stage = crop[0]
        if stage_rank(cost.stage) > stage_rank(current_stage):
            cursor.execute("""
                UPDATE crops
                SET current_stage = %s
                WHERE id = %s;
            """, (cost.stage, cost.crop_id))

        cursor.execute(
            "SELECT SUM(amount) FROM input_costs WHERE crop_id = %s;",
            (cost.crop_id,))
        total = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Cost logged: {cost.item_name} Rs {cost.amount} for crop_id {cost.crop_id}")
        return {"message": "Cost logged successfully",
                "cost_id": new_id, "item": cost.item_name,
                "amount": cost.amount, "running_total": float(total)}
    except Exception as e:
        return handle_db_error(e, conn, cursor)
