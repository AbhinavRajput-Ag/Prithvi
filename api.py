# api.py
# Prithvi — First API
# Runs a web server that answers requests from a browser

from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2

app = FastAPI(title="Prithvi API", version="1.0")

# ── DATABASE CONNECTION ──────────────────────────
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="prithvi test",
        user="postgres",
        password="Vijay@18091945"
    )

# ── ROUTE 1: Welcome message ─────────────────────
@app.get("/")
def home():
    return {
        "system": "Prithvi",
        "version": "1.0",
        "message": "India's Agricultural Operating System"
    }

# ── ROUTE 2: Get all farmers ─────────────────────
@app.get("/farmers")
def get_all_farmers():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name, village, land_acres FROM farmers;")
    rows = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    farmers = []
    for row in rows:
        farmers.append({
            "id": row[0],
            "name": row[1],
            "village": row[2],
            "land_acres": float(row[3])
        })
    
    return {"farmers": farmers}

# ── ROUTE 3: Get one farmer by name ─────────────
@app.get("/farmer/{name}")
def get_farmer(name: str):
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            f.name,
            f.village,
            c.crop_type,
            c.variety,
            c.current_stage,
            SUM(i.amount) AS total_cost
        FROM farmers f
        JOIN crops c ON c.farmer_id = f.id
        JOIN input_costs i ON i.crop_id = c.id
        WHERE f.name = %s
        GROUP BY f.name, f.village, c.crop_type,
                 c.variety, c.current_stage;
    """, (name,))
    
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if row is None:
        return {"error": f"No farmer found: {name}"}
    
    return {
        "farmer": row[0],
        "village": row[1],
        "crop": row[2],
        "variety": row[3],
        "stage": row[4],
        "total_cost": float(row[5])
    }

# ── ROUTE 4: Get break-even for a farmer ────────
@app.get("/breakeven/{name}")
def get_breakeven(name: str):
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            f.name,
            c.crop_type,
            SUM(i.amount) AS total_cost,
            CASE 
                WHEN f.name = 'Suresh Kumar' THEN 63
                WHEN f.name = 'Ramesh Patel' THEN 28
                ELSE 1
            END AS expected_yield,
            ROUND(SUM(i.amount) /
                CASE 
                    WHEN f.name = 'Suresh Kumar' THEN 63
                    WHEN f.name = 'Ramesh Patel' THEN 28
                    ELSE 1
                END, 2) AS breakeven
        FROM farmers f
        JOIN crops c ON c.farmer_id = f.id
        JOIN input_costs i ON i.crop_id = c.id
        WHERE f.name = %s
        GROUP BY f.name, c.crop_type;
    """, (name,))
    
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if row is None:
        return {"error": f"No data found: {name}"}
    
    return {
        "farmer": row[0],
        "crop": row[1],
        "total_cost": float(row[2]),
        "expected_yield_quintal": int(row[3]),
        "breakeven_per_quintal": float(row[4])
    }
from pydantic import BaseModel

# ── DATA MODEL for new farmer ────────────────────
class NewFarmer(BaseModel):
    name: str
    phone: str
    village: str
    district: str
    state: str
    land_acres: float

# ── ROUTE 5: Add a new farmer ────────────────────
@app.post("/farmers/add")
def add_farmer(farmer: NewFarmer):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO farmers (name, phone, village, district, state, land_acres)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (farmer.name, farmer.phone, farmer.village,
              farmer.district, farmer.state, farmer.land_acres))

        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()

        return {
            "message": "Farmer registered successfully",
            "farmer_id": new_id,
            "name": farmer.name
        }

    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return {"error": str(e)}

# ── DATA MODEL for new crop ──────────────────────
class NewCrop(BaseModel):
    farmer_id: int
    crop_type: str
    variety: str
    season: str
    year: int
    area_acres: float
    sowing_date: str
    expected_harvest: str

# ── ROUTE 6: Register a new crop ─────────────────
@app.post("/crops/add")
def add_crop(crop: NewCrop):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO crops 
                (farmer_id, crop_type, variety, season, year,
                 sowing_date, expected_harvest, current_stage)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'sowing')
            RETURNING id;
        """, (crop.farmer_id, crop.crop_type, crop.variety,
              crop.season, crop.year, crop.sowing_date,
              crop.expected_harvest))

        new_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()

        return {
            "message": "Crop registered successfully",
            "crop_id": new_id,
            "crop": crop.crop_type,
            "farmer_id": crop.farmer_id
        }

    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return {"error": str(e)}


# ── DATA MODEL for new cost ──────────────────────
class NewCost(BaseModel):
    crop_id: int
    stage: str
    item_name: str
    quantity: float
    unit: str
    amount: float

# ── ROUTE 7: Log a cost ──────────────────────────
@app.post("/costs/add")
def add_cost(cost: NewCost):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO input_costs
                (crop_id, stage, item_name, quantity, unit, amount)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (cost.crop_id, cost.stage, cost.item_name,
              cost.quantity, cost.unit, cost.amount))

        new_id = cursor.fetchone()[0]

        # Get updated total cost for this crop
        cursor.execute("""
            SELECT SUM(amount) FROM input_costs 
            WHERE crop_id = %s;
        """, (cost.crop_id,))
        
        total = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()

        return {
            "message": "Cost logged successfully",
            "cost_id": new_id,
            "item": cost.item_name,
            "amount": cost.amount,
            "running_total": float(total)
        }

    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return {"error": str(e)}