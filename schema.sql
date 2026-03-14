-- ================================================
-- PRITHVI PILOT SCHEMA v1.0
-- 5 tables: farmers, land, crops, costs, harvest
-- ================================================

CREATE TABLE IF NOT EXISTS farmers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    phone       VARCHAR(15) UNIQUE NOT NULL,
    alternate_phone VARCHAR(15),
    village     VARCHAR(100),
    gram_panchayat VARCHAR(100),
    tehsil      VARCHAR(100),
    district    VARCHAR(100),
    state       VARCHAR(100),
    postal_code VARCHAR(20),
    education_level VARCHAR(100),
    farming_experience_years INTEGER,
    land_acres  DECIMAL(8,2),
    language    VARCHAR(30) DEFAULT 'hindi',
    kyc_status  VARCHAR(30) DEFAULT 'pending',
    consent_status VARCHAR(30) DEFAULT 'pending',
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(100) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role          VARCHAR(20) NOT NULL DEFAULT 'farmer',
    farmer_id     INTEGER REFERENCES farmers(id),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS farmer_members (
    id                   SERIAL PRIMARY KEY,
    farmer_id            INTEGER REFERENCES farmers(id),
    name                 VARCHAR(150) NOT NULL,
    relation             VARCHAR(100) NOT NULL,
    age                  INTEGER,
    gender               VARCHAR(30),
    phone                VARCHAR(15),
    role_in_agriculture  VARCHAR(150) NOT NULL,
    primary_operator     BOOLEAN DEFAULT FALSE,
    decision_maker       BOOLEAN DEFAULT FALSE,
    created_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS land_parcels (
    id                 SERIAL PRIMARY KEY,
    farmer_id          INTEGER REFERENCES farmers(id),
    plot_name          VARCHAR(100) NOT NULL,
    parcel_code        VARCHAR(100),
    area_acres         DECIMAL(8,2) NOT NULL,
    cultivable_area_acres DECIMAL(8,2),
    irrigated_area_acres DECIMAL(8,2),
    location           TEXT,
    village            VARCHAR(100),
    tehsil             VARCHAR(100),
    survey_number      VARCHAR(100),
    soil_type          VARCHAR(100),
    irrigation_source  VARCHAR(100),
    ownership_type     VARCHAR(20) DEFAULT 'owned',
    road_access        BOOLEAN,
    power_access       BOOLEAN,
    fencing_status     VARCHAR(50),
    latitude           DECIMAL(10,7),
    longitude          DECIMAL(10,7),
    created_at         TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crops (
    id               SERIAL PRIMARY KEY,
    farmer_id        INTEGER REFERENCES farmers(id),
    parcel_id        INTEGER REFERENCES land_parcels(id),
    seed_source_id   INTEGER,
    seed_brand       VARCHAR(100),
    seed_lot_number  VARCHAR(100),
    crop_type        VARCHAR(100) NOT NULL,
    variety          VARCHAR(100),
    season           VARCHAR(20),
    year             INTEGER,
    sowing_method    VARCHAR(100),
    sowing_date      DATE,
    expected_harvest DATE,
    expected_yield_quintal FLOAT,
    current_stage    VARCHAR(30) DEFAULT 'sowing',
    crop_status      VARCHAR(30) DEFAULT 'planned',
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS input_costs (
    id          SERIAL PRIMARY KEY,
    crop_id     INTEGER REFERENCES crops(id),
    stage       VARCHAR(50),
    category    VARCHAR(100),
    supplier_id INTEGER,
    item_name   VARCHAR(150),
    invoice_number VARCHAR(100),
    quantity    DECIMAL(10,2),
    unit        VARCHAR(30),
    amount      DECIMAL(12,2) NOT NULL,
    transaction_mode VARCHAR(50),
    transaction_reference VARCHAR(100),
    transaction_date DATE,
    subsidized  BOOLEAN DEFAULT FALSE,
    receipt_id  INTEGER,
    entry_date  DATE DEFAULT CURRENT_DATE,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS harvest_records (
    id            SERIAL PRIMARY KEY,
    crop_id       INTEGER REFERENCES crops(id),
    harvest_date  DATE,
    yield_kg      DECIMAL(10,2),
    quality_grade VARCHAR(20),
    notes         TEXT,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS harvests (
    id SERIAL PRIMARY KEY,
    crop_id INTEGER REFERENCES crops(id),
    harvest_date DATE,
    yield_quintal FLOAT,
    yield_rejected_quintal FLOAT DEFAULT 0,
    quality_grade VARCHAR(50),
    moisture_percent FLOAT,
    bags_count INTEGER,
    storage_location VARCHAR(150),
    selling_price FLOAT,
    buyer TEXT,
    revenue FLOAT
);

CREATE TABLE IF NOT EXISTS deals (
    id SERIAL PRIMARY KEY,
    crop_id INTEGER REFERENCES crops(id),
    buyer_id INTEGER,
    buyer_type VARCHAR(100),
    sale_date DATE NOT NULL,
    quantity_quintal FLOAT NOT NULL,
    price_per_quintal FLOAT NOT NULL,
    buyer TEXT NOT NULL,
    gross_amount FLOAT NOT NULL,
    deductions_amount FLOAT NOT NULL DEFAULT 0,
    transport_cost FLOAT NOT NULL DEFAULT 0,
    mandi_fee FLOAT NOT NULL DEFAULT 0,
    net_amount FLOAT NOT NULL DEFAULT 0,
    amount_received FLOAT NOT NULL DEFAULT 0,
    payment_terms TEXT,
    due_date DATE,
    payment_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS buyer_registry (
    id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    buyer_type VARCHAR(100) NOT NULL,
    contact_person VARCHAR(150),
    phone VARCHAR(15),
    location VARCHAR(150),
    district VARCHAR(100),
    state VARCHAR(100),
    payment_reliability_score DECIMAL(5,2),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS input_suppliers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    supplier_type VARCHAR(100) NOT NULL,
    contact_person VARCHAR(150),
    phone VARCHAR(15),
    location VARCHAR(150),
    district VARCHAR(100),
    state VARCHAR(100),
    gst_number VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS soil_tests (
    id                      SERIAL PRIMARY KEY,
    farmer_id               INTEGER REFERENCES farmers(id),
    parcel_id               INTEGER REFERENCES land_parcels(id),
    sample_date             DATE NOT NULL,
    lab_name                VARCHAR(150),
    report_number           VARCHAR(100),
    ph                      DECIMAL(5,2),
    organic_carbon          DECIMAL(6,2),
    nitrogen                DECIMAL(10,2),
    phosphorus              DECIMAL(10,2),
    potassium               DECIMAL(10,2),
    recommendation_summary  TEXT,
    created_at              TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS yield_estimate_revisions (
    id SERIAL PRIMARY KEY,
    crop_id INTEGER REFERENCES crops(id),
    revision_date DATE NOT NULL,
    previous_estimate_quintal DECIMAL(10,2),
    new_estimate_quintal DECIMAL(10,2) NOT NULL,
    revision_reason TEXT,
    source VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS expense_receipts (
    id SERIAL PRIMARY KEY,
    farmer_id INTEGER REFERENCES farmers(id),
    crop_id INTEGER REFERENCES crops(id),
    input_cost_id INTEGER REFERENCES input_costs(id),
    supplier_id INTEGER REFERENCES input_suppliers(id),
    receipt_number VARCHAR(100),
    receipt_date DATE NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    payment_mode VARCHAR(50),
    file_url TEXT,
    verification_status VARCHAR(20) DEFAULT 'pending',
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS farmer_documents (
    id                   SERIAL PRIMARY KEY,
    farmer_id            INTEGER REFERENCES farmers(id),
    parcel_id            INTEGER REFERENCES land_parcels(id),
    crop_id              INTEGER REFERENCES crops(id),
    document_type        VARCHAR(100) NOT NULL,
    document_number      VARCHAR(100),
    issued_by            VARCHAR(150),
    issue_date           DATE,
    expiry_date          DATE,
    verification_status  VARCHAR(20) DEFAULT 'pending',
    file_url             TEXT,
    notes                TEXT,
    created_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS weather_snapshots (
    id SERIAL PRIMARY KEY,
    farmer_id INTEGER REFERENCES farmers(id),
    parcel_id INTEGER REFERENCES land_parcels(id),
    crop_id INTEGER REFERENCES crops(id),
    source_name VARCHAR(100) NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    forecast_window_hours INTEGER,
    rainfall_mm DECIMAL(10,2),
    temperature_min_c DECIMAL(8,2),
    temperature_max_c DECIMAL(8,2),
    humidity_percent DECIMAL(8,2),
    wind_speed_kmph DECIMAL(8,2),
    solar_radiation DECIMAL(10,2),
    heat_risk VARCHAR(20),
    flood_risk VARCHAR(20),
    drought_risk VARCHAR(20),
    hail_risk VARCHAR(20),
    lightning_risk VARCHAR(20),
    raw_payload TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mandi_price_snapshots (
    id SERIAL PRIMARY KEY,
    crop_type VARCHAR(100) NOT NULL,
    variety VARCHAR(100),
    market_name VARCHAR(150) NOT NULL,
    district VARCHAR(100),
    state VARCHAR(100),
    snapshot_date DATE NOT NULL,
    min_price DECIMAL(12,2),
    modal_price DECIMAL(12,2),
    max_price DECIMAL(12,2),
    arrival_quantity DECIMAL(12,2),
    source_name VARCHAR(100) NOT NULL,
    raw_payload TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS risk_alert_events (
    id SERIAL PRIMARY KEY,
    farmer_id INTEGER REFERENCES farmers(id),
    parcel_id INTEGER REFERENCES land_parcels(id),
    crop_id INTEGER REFERENCES crops(id),
    alert_type VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    confidence_score DECIMAL(5,2),
    title VARCHAR(200) NOT NULL,
    message TEXT NOT NULL,
    recommended_action TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'open',
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crops_farmer_id ON crops(farmer_id);
CREATE INDEX IF NOT EXISTS idx_input_costs_crop_id ON input_costs(crop_id);
CREATE INDEX IF NOT EXISTS idx_harvests_crop_id ON harvests(crop_id);
CREATE INDEX IF NOT EXISTS idx_crops_expected_harvest ON crops(expected_harvest);
CREATE INDEX IF NOT EXISTS idx_deals_crop_id ON deals(crop_id);
CREATE INDEX IF NOT EXISTS idx_app_users_farmer_id ON app_users(farmer_id);
CREATE INDEX IF NOT EXISTS idx_land_parcels_farmer_id ON land_parcels(farmer_id);
CREATE INDEX IF NOT EXISTS idx_farmer_members_farmer_id ON farmer_members(farmer_id);
CREATE INDEX IF NOT EXISTS idx_soil_tests_farmer_id ON soil_tests(farmer_id);
CREATE INDEX IF NOT EXISTS idx_soil_tests_parcel_id ON soil_tests(parcel_id);
CREATE INDEX IF NOT EXISTS idx_farmer_documents_farmer_id ON farmer_documents(farmer_id);
CREATE INDEX IF NOT EXISTS idx_yield_estimate_revisions_crop_id ON yield_estimate_revisions(crop_id);
CREATE INDEX IF NOT EXISTS idx_expense_receipts_farmer_id ON expense_receipts(farmer_id);
CREATE INDEX IF NOT EXISTS idx_expense_receipts_crop_id ON expense_receipts(crop_id);
CREATE INDEX IF NOT EXISTS idx_weather_snapshots_crop_id ON weather_snapshots(crop_id);
CREATE INDEX IF NOT EXISTS idx_weather_snapshots_parcel_id ON weather_snapshots(parcel_id);
CREATE INDEX IF NOT EXISTS idx_weather_snapshots_farmer_id ON weather_snapshots(farmer_id);
CREATE INDEX IF NOT EXISTS idx_mandi_price_snapshots_crop_type ON mandi_price_snapshots(crop_type);
CREATE INDEX IF NOT EXISTS idx_mandi_price_snapshots_date ON mandi_price_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_risk_alert_events_crop_id ON risk_alert_events(crop_id);
CREATE INDEX IF NOT EXISTS idx_risk_alert_events_farmer_id ON risk_alert_events(farmer_id);
CREATE INDEX IF NOT EXISTS idx_risk_alert_events_status ON risk_alert_events(status);

-- ================================================
-- SAMPLE DATA
-- ================================================

INSERT INTO farmers (name, phone, village, district, state, land_acres)
VALUES 
('Suresh Kumar', '9876543211', 'Rampur Kalan', 'Bhopal', 'Madhya Pradesh', 3.5),
('Ramesh Patel', '9876543299', 'Deogaon', 'Sehore', 'Madhya Pradesh', 2.0);

INSERT INTO land_parcels (farmer_id, plot_name, area_acres, location)
VALUES 
(1, 'North Field', 3.5, 'Near the river, north side of village'),
(2, 'South Field', 2.0, 'Near the well, south end of village');

INSERT INTO crops (farmer_id, parcel_id, crop_type, variety, season, year, sowing_date, expected_harvest, expected_yield_quintal, current_stage)
VALUES 
(1, 1, 'wheat', 'HD-2967', 'rabi', 2026, '2025-11-15', '2026-03-15', 63, 'growing'),
(2, 2, 'chana', 'JG-11',   'rabi', 2026, '2025-10-20', '2026-02-15', 28, 'growing');

-- Additional costs for Suresh
INSERT INTO input_costs (crop_id, stage, item_name, quantity, unit, amount)
VALUES
(1, 'sowing',    'land levelling',       1,   'event',  2800),
(1, 'sowing',    'land preparation',     1,   'event',  3500),
(1, 'growing',   'Pesticide spray',      2,   'litres',  800),
(1, 'growing',   'Second irrigation',    1,   'event',   400),
(1, 'growing',   'Weeding labour',       5,   'days',   1750),
(1, 'harvest',   'Harvesting labour',    7,   'days',   2450),
(1, 'harvest',   'Threshing machine',    1,   'event',  1200),
(1, 'logistics', 'Transport to mandi',   1,   'trip',   1100),

-- Additional costs for Ramesh
(2, 'growing',   'Urea',                40,   'kg',      240),
(2, 'growing',   'Pesticide spray',      1,   'litres',  400),
(2, 'growing',   'Weeding labour',       3,   'days',   1050),
(2, 'harvest',   'Harvesting labour',    4,   'days',   1400),
(2, 'harvest',   'Threshing machine',    1,   'event',   800),
(2, 'logistics', 'Transport to mandi',   1,   'trip',    700);
