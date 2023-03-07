/*--------------------------------------------
    DATA CLEANING
    1. Loading of each CSV file to table
    2. Data validations
    3. Data transformations
*/--------------------------------------------

/* 
    Create attendance_raw table
    Once table is created, import data from the CSV file
*/
DROP TABLE IF EXISTS attendance_raw;
CREATE TABLE IF NOT EXISTS attendance_raw (
    user_id BIGINT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    "location" VARCHAR(50),
    "date" DATE,
    "time" TIME,
    timezone VARCHAR(10),
    "case" VARCHAR(10),
    source VARCHAR(10)
);

/* 
    Create users_raw table
    Once table is created, import data from the CSV file
*/
DROP TABLE IF EXISTS users_raw;
CREATE TABLE IF NOT EXISTS users_raw (
    user_id BIGINT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(10),
    date_birth DATE,
    date_hire DATE,
    date_leave DATE,
    employment VARCHAR(50),
    position VARCHAR(100),
    "location" VARCHAR(50),
    department VARCHAR(50),
    created_at TIMESTAMP
);

/* 
    Create payroll_raw table
    Once table is created, import data from the CSV file
*/
DROP TABLE IF EXISTS payroll_raw;
CREATE TABLE IF NOT EXISTS payroll_raw (
    user_id BIGINT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_start DATE,
    date_end DATE,
    ctc NUMERIC,
    net_pay NUMERIC,
    gross_pay NUMERIC,
    data_salary_basic_rate INTEGER,
    data_salary_basic_type VARCHAR(20),
    currency VARCHAR(10),
    status VARCHAR(10),
    created_at TIMESTAMP
);


/* 
    Create schedules_raw table
    Once table is created, import data from the CSV file

    Run the following via command line prompt:
    psql -d {db_name} --user={user_name} -c "\copy schedules_raw FROM '{path_to_csv_file}' DELIMITER ',' csv header"

    Output: COPY 4094
*/
DROP TABLE IF EXISTS schedules_raw;
CREATE TABLE IF NOT EXISTS schedules_raw (
    "type" VARCHAR(10),
    dates JSONB,
    time_start TIME,
    time_end TIME,
    timezone VARCHAR(10),
    time_planned INTEGER,
    break_time INTEGER,
    leave_type VARCHAR(20),
    user_id BIGINT[]
);

/* 
    Create leave_requests_raw table
    Once table is created, import data from the CSV file

    Run the following via command line prompt:
    psql -d {db_name} --user={user_name} -c "\copy leave_requests_raw FROM '{path_to_csv_file}' DELIMITER ',' csv header"

    Output: COPY 51
*/
DROP TABLE IF EXISTS leave_requests_raw;
CREATE TABLE IF NOT EXISTS leave_requests_raw (
    user_id BIGINT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    "type" VARCHAR(10),
    leave_type VARCHAR(20),
    dates JSONB,
    time_start TIME,
    time_end TIME,
    timezone VARCHAR(10),
    status VARCHAR(10),
    created_at TIMESTAMP
);



/* 
    Create new cleaned table attendance:
    - Exclude empty columns
    - Remove duplicates
    - Standardize column values
*/
DROP TABLE IF EXISTS attendance;
CREATE TABLE IF NOT EXISTS attendance AS (
    SELECT DISTINCT
        user_id,
        "location",
        "date",
        "time",
        timezone,
        "case",
        INITCAP(source) AS source
    FROM
        attendance_raw
);


/* 
    Create new cleaned table users:
    - Exclude empty columns
    - Remove duplicates
    - Standardize column values
*/
DROP TABLE IF EXISTS users;
CREATE TABLE IF NOT EXISTS users AS (
    SELECT DISTINCT
        user_id,
        INITCAP(gender) AS gender,
        date_birth,
        date_hire,
        date_leave,
        employment,
        position,
        "location",
        department,
        created_at
    FROM
        users_raw
);


/* 
    Create new cleaned table payroll:
    - Exclude empty columns
    - Remove duplicates
    - Standardize column values
*/
DROP TABLE IF EXISTS payroll;
CREATE TABLE IF NOT EXISTS payroll AS (
    SELECT DISTINCT
        user_id,
        date_start,
        date_end,
        ctc,
        net_pay,
        gross_pay,
        data_salary_basic_rate,
        INITCAP(data_salary_basic_type) AS data_salary_basic_type,
        currency,
        INITCAP(status) AS status,
        created_at
    FROM
        payroll_raw
);


/* 
    Create new cleaned table leave_requests:
    - Expand the "date" JSON array column into a set of date values
    - Exclude empty columns
    - Remove duplicates
    - Standardize column values
*/
DROP TABLE IF EXISTS leave_requests;
CREATE TABLE IF NOT EXISTS leave_requests AS (
    WITH leave_requests_simplified AS (
        SELECT
            user_id,
            INITCAP("type") AS "type",
            INITCAP(leave_type) AS leave_type,
            JSONB_ARRAY_ELEMENTS_TEXT(dates)::date AS "date",
            INITCAP(status) AS status,
            created_at
        FROM
            leave_requests_raw
    )
    SELECT
        DISTINCT *
    FROM
        leave_requests_simplified
    ORDER BY
        1, 4
);


/* 
    Create new cleaned table schedules:
    - 1st level: Expand the "type" bigint array column into
                 a set of bigint values
    - 2nd level: Expand the "date" JSON array column into a 
                 set of date values
    - Exclude empty columns
    - Remove duplicates
    - Standardize column values
*/
DROP TABLE IF EXISTS schedules;
CREATE TABLE IF NOT EXISTS schedules AS (
    WITH schedules_1st_level AS (
        SELECT
            INITCAP("type") AS "type",
            dates,
            time_start,
            time_end,
            timezone,
            time_planned,
            break_time,
            INITCAP(leave_type) AS leave_type,
            UNNEST(user_id) AS user_id
        FROM
            schedules_raw
    ),
    schedules_2nd_level AS (
        SELECT
            user_id,
            "type",
            JSONB_ARRAY_ELEMENTS_TEXT(dates)::date AS "date",
            time_start,
            time_end,
            timezone,
            time_planned,
            break_time,
            leave_type
        FROM
            schedules_1st_level
    )
    SELECT
        DISTINCT *
    FROM
        schedules_2nd_level
    ORDER BY
        1, 3
);
