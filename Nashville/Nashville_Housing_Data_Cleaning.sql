/*--------------------------------------------
    DATA CLEANING
    1. Loading of CSV file to table
    2. Data validations
    3. Data transformations
*/--------------------------------------------

/* Create schema under the postgres database */
CREATE SCHEMA IF NOT EXISTS nashville;
SET search_path TO nashville;

/*
    Create nashville_housing_raw table
    Once table is created, import data from the CSV file

    Run the following via command line prompt:
    psql -d postgres --user=postgres -c "\copy nashville.nashville_housing_raw FROM '{path_to_csv_file}' DELIMITER ',' CSV HEADER"

    Output: COPY 56477
*/
DROP TABLE IF EXISTS nashville_housing_raw;
CREATE TABLE IF NOT EXISTS nashville_housing_raw (
    unique_id INTEGER,
    parcel_id TEXT,
    land_use TEXT,
    property_address TEXT,
    sale_date DATE,
    sale_price TEXT,
    legal_reference TEXT,
    sold_as_vacant TEXT,
    owner_name TEXT,
    owner_address TEXT,
    acreage DECIMAL,
    tax_district TEXT,
    land_value NUMERIC,
    building_value NUMERIC,
    total_value NUMERIC,
    year_built INTEGER,
    bedrooms INTEGER,
    full_bath INTEGER,
    half_bath INTEGER,
    PRIMARY KEY (unique_id)
);

/*
    Create the nashville_housing cleaned table:
    1. Identify duplicates and exclude
    2. Populate property address empty data
    3. Convert sale price to numeric
    4. Change Y and N to Yes and No in sold_as_vacant field
    5. Populate other null data
    6. Break out address into individual columns (Address, City, State)
*/
CREATE TABLE IF NOT EXISTS nashville_housing AS (
    WITH dup_check_cte AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    parcel_id,
                    property_address,
                    sale_price,
                    sale_date,
                    legal_reference
                ORDER BY
                    unique_id
            ) AS row_num
        FROM
            nashville.nashville_housing_raw
    ),
    property_address_filled AS (
        SELECT
            a.unique_id,
            a.parcel_id,
            COALESCE(a.property_address, b.property_address) AS property_address
        FROM
            nashville.nashville_housing_raw a
        JOIN
            nashville.nashville_housing_raw b
            ON a.parcel_id = b.parcel_id
            AND a.unique_id <> b.unique_id
        WHERE
            a.property_address IS NULL
    ),
    nashville_housing_1st_level AS (
        SELECT
            unique_id,
            parcel_id,
            land_use,
            CASE
                WHEN property_address = NULL
                    THEN (
                        SELECT
                            paf.property_address
                        FROM
                            property_address_filled paf
                        WHERE
                            unique_id = paf.unique_id
                            AND parcel_id = paf.parcel_id
                    )
                ELSE property_address
            END AS property_address,
            sale_date,
            REPLACE(REPLACE(sale_price, ',', ''), '$', '')::numeric AS sale_price,
            legal_reference,
            CASE
                WHEN sold_as_vacant = 'Y'
                    THEN 'Yes'
                WHEN sold_as_vacant = 'N'
                    THEN 'No'
                ELSE sold_as_vacant
            END AS sold_as_vacant,
            COALESCE(owner_name, 'No Data') AS owner_name,
            COALESCE(owner_address, 'No Data') AS owner_address,
            COALESCE(acreage, 0) AS acreage,
            COALESCE(tax_district, 'No Data') AS tax_district,
            COALESCE(land_value, 0) AS land_value,
            COALESCE(building_value, 0) AS building_value,
            COALESCE(total_value, 0) AS total_value,
            year_built,
            COALESCE(bedrooms, 0) AS bedrooms,
            COALESCE(full_bath, 0) AS full_bath,
            COALESCE(half_bath, 0) AS half_bath
        FROM
            dup_check_cte
        WHERE
            row_num <= 1
        ORDER BY
            property_address
    ),
    nashville_housing_2nd_level AS (
        SELECT
            unique_id,
            parcel_id,
            land_use,
            TRIM(SPLIT_PART(property_address, ',', 1)) AS property_address,
            TRIM(SPLIT_PART(property_address, ',', 2)) AS property_city,
            sale_date,
            sale_price,
            legal_reference,
            sold_as_vacant,
            owner_name,
            TRIM(SPLIT_PART(owner_address, ',', 1)) AS owner_address,
            CASE
                WHEN owner_address = 'No Data'
                    THEN 'No Data'
                ELSE TRIM(SPLIT_PART(owner_address, ',', 2))
            END AS owner_city,
            CASE
                WHEN owner_address = 'No Data'
                    THEN 'No Data'
                ELSE TRIM(SPLIT_PART(owner_address, ',', 3))
            END AS owner_state,
            acreage,
            tax_district,
            land_value,
            building_value,
            total_value,
            year_built,
            bedrooms,
            full_bath,
            half_bath
        FROM
            nashville_housing_1st_level
    )
    SELECT
        *
    FROM
        nashville_housing_2nd_level
);

/* Drop the raw table */
DROP TABLE IF EXISTS nashville_housing_raw;
