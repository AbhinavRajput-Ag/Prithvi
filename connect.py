# connect.py
# Prithvi — Functions v1
# Get any farmer's data by name

import psycopg2

# ── DATABASE CONNECTION ──────────────────────────
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="prithvi test",
        user="postgres",
        password="Vijay@18091945"
    )

# ── FUNCTION 1: Get all farmers ──────────────────
def get_all_farmers():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name, village, land_acres FROM farmers;")
    farmers = cursor.fetchall()
    
    print("=" * 45)
    print("ALL FARMERS")
    print("=" * 45)
    for f in farmers:
        print(f"ID:{f[0]}  {f[1]}  —  {f[2]}  —  {f[3]} acres")
    
    cursor.close()
    conn.close()

# ── FUNCTION 2: Get one farmer by name ───────────
def get_farmer(name):
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
    
    if row is None:
        print(f"No farmer found with name: {name}")
    else:
        print("=" * 45)
        print(f"FARMER  : {row[0]}")
        print(f"VILLAGE : {row[1]}")
        print(f"CROP    : {row[2]} — {row[3]}")
        print(f"STAGE   : {row[4]}")
        print(f"TOTAL COST : Rs {row[5]:,.2f}")
        print("=" * 45)
    
    cursor.close()
    conn.close()

# ── FUNCTION 3: Get cost breakdown by stage ──────
def get_cost_breakdown(name):
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            i.stage,
            i.item_name,
            i.amount
        FROM input_costs i
        JOIN crops c ON i.crop_id = c.id
        JOIN farmers f ON c.farmer_id = f.id
        WHERE f.name = %s
        ORDER BY i.id;
    """, (name,))
    
    rows = cursor.fetchall()
    
    if not rows:
        print(f"No costs found for: {name}")
    else:
        print("=" * 45)
        print(f"COST BREAKDOWN — {name}")
        print("=" * 45)
        total = 0
        for row in rows:
            print(f"{row[0]:12} | {row[1]:25} | Rs {row[2]:,.2f}")
            total += row[2]
        print("-" * 45)
        print(f"{'TOTAL':12} | {'':25} | Rs {total:,.2f}")
        print("=" * 45)
    
    cursor.close()
    conn.close()

# ── FUNCTION 4: Get break-even price ─────────────
def get_breakeven(name):
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

    if row is None:
        print(f"No data found for: {name}")
    else:
        print("=" * 45)
        print(f"BREAK-EVEN — {row[0]}")
        print("=" * 45)
        print(f"Crop          : {row[1]}")
        print(f"Total Cost    : Rs {row[2]:,.2f}")
        print(f"Expected Yield: {row[3]} quintal")
        print(f"Break-even    : Rs {row[4]:,.2f} per quintal")
        print("=" * 45)

    cursor.close()
    conn.close()
    
# ── RUN ──────────────────────────────────────────
get_all_farmers()
print()
get_farmer("Suresh Kumar")
print()
get_farmer("Ramesh Patel")
print()
get_cost_breakdown("Suresh Kumar")
print()
get_cost_breakdown("Ramesh Patel")
print()
get_breakeven("Suresh Kumar")
print()
get_breakeven("Ramesh Patel")