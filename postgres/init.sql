-- Drop existing tables if they exist
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS assets;
DROP TABLE IF EXISTS indicators;
DROP TABLE IF EXISTS metadata;

-- Create companies table
CREATE TABLE companies (
    company_id VARCHAR PRIMARY KEY,
    company_name VARCHAR,
    country VARCHAR
);

-- Create assets table
CREATE TABLE assets (
    asset_id SERIAL PRIMARY KEY,
    company_id VARCHAR,
    asset_name VARCHAR,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    FOREIGN KEY (company_id) REFERENCES companies (company_id)
);

-- Create indicators table
CREATE TABLE indicators (
    indicator_id SERIAL PRIMARY KEY,
    asset_id INTEGER,
    indicator_value VARCHAR,
    status VARCHAR,
    FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
);

-- Create metadata table
CREATE TABLE metadata (
    metadata_id SERIAL PRIMARY KEY,
    data_point_id INTEGER,
    data_source VARCHAR,
    additional_info VARCHAR,
    FOREIGN KEY (data_point_id) REFERENCES assets (asset_id)
);
