-- ================================================
-- PRITHVI QUERIES v1.0
-- Run these in pgAdmin Query Tool anytime
-- ================================================


-- QUERY 1: See all farmers
SELECT * FROM farmers;


-- QUERY 2: See all crops
SELECT * FROM crops;


-- QUERY 3: Farmer + crop + total cost
SELECT 
    f.name          AS farmer,
    f.village,
    c.crop_type,
    c.variety,
    c.current_stage,
    SUM(i.amount)   AS total_cost
FROM farmers f
JOIN crops c ON c.farmer_id = f.id
JOIN input_costs i ON i.crop_id = c.id
GROUP BY f.name, f.village, c.crop_type, 
         c.variety, c.current_stage;


-- QUERY 4: All costs for Suresh (crop_id 1)
SELECT stage, item_name, quantity, unit, amount
FROM input_costs
WHERE crop_id = 1
ORDER BY id;


-- QUERY 5: Break-even price for Suresh
-- Wheat average yield = 18 quintal per acre
-- Suresh has 3.5 acres so expected yield = 63 quintal
SELECT 
    f.name                          AS farmer,
    c.crop_type,
    SUM(i.amount)                   AS total_cost,
    63                              AS expected_yield_quintal,
    ROUND(SUM(i.amount) / 63, 2)   AS breakeven_per_quintal
FROM farmers f
JOIN crops c ON c.farmer_id = f.id
JOIN input_costs i ON i.crop_id = c.id
WHERE f.id = 1
GROUP BY f.name, c.crop_type;


-- QUERY 6: Full cost breakdown for Suresh by item
SELECT 
    stage,
    item_name,
    amount
FROM input_costs
WHERE crop_id = 1
ORDER BY id;


-- QUERY 7: Total cost per stage for Suresh
SELECT 
    stage,
    SUM(amount) AS stage_total
FROM input_costs
WHERE crop_id = 1
GROUP BY stage
ORDER BY stage;


-- QUERY 8: FPO portfolio — both farmers side by side
-- (will show full data once Ramesh costs are added)
-- QUERY 8: FPO portfolio — both farmers side by side
SELECT 
    f.name                          AS farmer,
    f.village,
    c.crop_type,
    c.variety,
    c.current_stage,
    SUM(i.amount)                   AS total_cost,
    CASE 
        WHEN f.id = 1 THEN 63
        WHEN f.id = 2 THEN 28
    END                             AS expected_yield_quintal,
    ROUND(SUM(i.amount) / 
        CASE 
            WHEN f.id = 1 THEN 63
            WHEN f.id = 2 THEN 28
        END, 2)                     AS breakeven_per_quintal
FROM farmers f
JOIN crops c ON c.farmer_id = f.id
JOIN input_costs i ON i.crop_id = c.id
GROUP BY f.name, f.village, c.crop_type, 
         c.variety, c.current_stage, f.id
ORDER BY f.name;


-- QUERY 9: Fix duplicate cost entries
-- Run when a cost is accidentally entered twice
-- Change crop_id, stage, item_name to match the duplicate
DELETE FROM input_costs
WHERE id NOT IN (
    SELECT MIN(id)
    FROM input_costs
    GROUP BY crop_id, stage, item_name, amount
);
-- QUERY 10: Cost breakdown by stage for any farmer
-- Change crop_id number to see different farmers
-- crop_id 1 = Suresh, crop_id 2 = Ramesh
SELECT 
    stage,
    item_name,
    amount
FROM input_costs
WHERE crop_id = 1
ORDER BY id;


-- QUERY 11: Stage totals for any farmer
SELECT 
    stage,
    SUM(amount) AS stage_total
FROM input_costs
WHERE crop_id = 1
GROUP BY stage
ORDER BY stage;