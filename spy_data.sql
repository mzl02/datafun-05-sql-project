-- Start by deleting any tables if the exist already
-- We want to be able to re-run this script as needed.
-- DROP tables in reverse order of creation 
-- DROP dependent tables (with foreign keys) first

DROP TABLE IF EXISTS spy_data;


-- Create the spy table


CREATE TABLE IF NOT EXISTS spy_data (
    date TEXT PRIMARY KEY,  -- Date of the data point
    open REAL,              -- Opening price
    high REAL,              -- Highest price of the day
    low REAL,               -- Lowest price of the day
    close REAL,             -- Closing price
    volume INTEGER          -- Trading volume
);


