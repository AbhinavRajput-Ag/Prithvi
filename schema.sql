-- ================================================
-- PRITHVI PILOT SCHEMA v1.0
-- 5 tables: farmers, land, crops, costs, harvest
-- ================================================

CREATE TABLE IF NOT EXISTS farmers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    phone       VARCHAR(15) UNIQUE NOT NULL,
    village     VARCHAR(100),
    district    VARCHAR(100),
    state       VARCHAR(100),
    land_acres  DECIMAL(8,2),
    language    VARCHAR(30) DEFAULT 'hindi',
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS land_parcels (
    id          SERIAL PRIMARY KEY,
    farmer_id   INTEGER REFERENCES farmers(id),
    plot_name   VARCHAR(100),
    area_acres  DECIMAL(8,2),
    location    TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crops (
    id               SERIAL PRIMARY KEY,
    farmer_id        INTEGER REFERENCES farmers(id),
    parcel_id        INTEGER REFERENCES land_parcels(id),
    crop_type        VARCHAR(100) NOT NULL,
    variety          VARCHAR(100),
    season           VARCHAR(20),
    year             INTEGER,
    sowing_date      DATE,
    expected_harvest DATE,
    expected_yield_quintal FLOAT,
    current_stage    VARCHAR(30) DEFAULT 'sowing',
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS input_costs (
    id          SERIAL PRIMARY KEY,
    crop_id     INTEGER REFERENCES crops(id),
    stage       VARCHAR(50),
    item_name   VARCHAR(150),
    quantity    DECIMAL(10,2),
    unit        VARCHAR(30),
    amount      DECIMAL(12,2) NOT NULL,
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
    selling_price FLOAT,
    buyer TEXT,
    revenue FLOAT
);

CREATE INDEX IF NOT EXISTS idx_crops_farmer_id ON crops(farmer_id);
CREATE INDEX IF NOT EXISTS idx_input_costs_crop_id ON input_costs(crop_id);
CREATE INDEX IF NOT EXISTS idx_harvests_crop_id ON harvests(crop_id);
CREATE INDEX IF NOT EXISTS idx_crops_expected_harvest ON crops(expected_harvest);

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
