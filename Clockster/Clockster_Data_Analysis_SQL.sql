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
    "source" VARCHAR(10)
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
        INITCAP("source") AS "source"
    FROM
        attendance_raw
    ORDER BY
        user_id, "date"
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
    ORDER BY
        user_id
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
    ORDER BY
        user_id
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
        user_id, "date"
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
        user_id, "date"
);


/* Drop the raw tables */
DROP TABLE IF EXISTS attendance_raw;
DROP TABLE IF EXISTS users_raw;
DROP TABLE IF EXISTS payroll_raw;
DROP TABLE IF EXISTS schedules_raw;
DROP TABLE IF EXISTS leave_requests_raw;


/*--------------------------------------------
    DATA EXPLORATION AND ANALYSIS
*/--------------------------------------------

/* Summary of tardiness by employee */
WITH attendance_merged AS (
    SELECT 
        att.user_id,
        att."location",
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position" AS "position",
        u.department,
        att."date" AS log_date,
        att."time" AS log_time,
        att.timezone AS log_timezone,
        att."case",
        att."source", 
        sch."type",
        sch.time_start,
        sch.time_end,
        sch.timezone AS schedule_timezone,
        sch.time_planned,
        sch.break_time
    FROM
        attendance att
    INNER JOIN
        schedules sch
        ON att.user_id = sch.user_id
        AND att."date" = sch."date"
    INNER JOIN
        users u
        ON u.user_id = att.user_id
        AND u.user_id = sch.user_id
),
attendance_merged_with_diffs AS (
    SELECT
        user_id,
        "location",
        gender,
        date_hire,
        date_leave,
        "position",
        department,
        log_date,
        log_time,
        log_timezone,
        "case",
        "source", 
        "type",
        time_start,
        time_end,
        time_planned,
        break_time,
        CASE
            WHEN "case" = 'IN'
                THEN date_part('hour', log_time - time_start)
            ELSE 0
        END AS login_diff_hours,
        CASE
            WHEN "case" = 'IN'
                THEN date_part('hour', log_time - time_start) * 60 + 
                     date_part('minute', log_time - time_start)
            ELSE 0
        END AS login_diff_minutes,
        CASE
            WHEN "case" = 'OUT'
                THEN date_part('hour', log_time - time_end)
            ELSE 0
        END AS logout_diff_hours,
        CASE
            WHEN "case" = 'OUT'
                THEN date_part('hour', log_time - time_end) * 60 + 
                     date_part('minute', log_time - time_end)
            ELSE 0
        END AS logout_diff_minutes
    FROM
        attendance_merged
),
attendance_merged_with_diffs_classified AS (
    SELECT
        user_id,
        "location",
        gender,
        date_hire,
        date_leave,
        "position",
        department,
        log_date,
        log_time,
        log_timezone,
        "case",
        "source", 
        "type",
        time_start,
        time_end,
        time_planned,
        break_time,
        login_diff_minutes,
        logout_diff_minutes,
        CASE
            WHEN (login_diff_minutes > 10 AND
                  login_diff_minutes <= 120) THEN 'Yes'
            ELSE 'No'
        END AS is_tardy
    FROM 
    attendance_merged_with_diffs
)
SELECT
    user_id,
    "position",
    COUNT(
        CASE
            WHEN is_tardy = 'Yes'
                THEN is_tardy
            END
    ) AS tardiness_count
FROM
    attendance_merged_with_diffs_classified
GROUP BY
    user_id,
    "position"
ORDER BY
    3 DESC;


/* Summary of tardiness by department */
WITH attendance_merged AS (
    SELECT 
        att.user_id,
        att."location",
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position" AS "position",
        u.department,
        att."date" AS log_date,
        att."time" AS log_time,
        att.timezone AS log_timezone,
        att."case",
        att."source", 
        sch."type",
        sch.time_start,
        sch.time_end,
        sch.timezone AS schedule_timezone,
        sch.time_planned,
        sch.break_time
    FROM
        attendance att
    INNER JOIN
        schedules sch
        ON att.user_id = sch.user_id
        AND att."date" = sch."date"
    INNER JOIN
        users u
        ON u.user_id = att.user_id
        AND u.user_id = sch.user_id
),
attendance_merged_with_diffs AS (
    SELECT
        user_id,
        "location",
        gender,
        date_hire,
        date_leave,
        "position",
        department,
        log_date,
        log_time,
        log_timezone,
        "case",
        "source", 
        "type",
        time_start,
        time_end,
        time_planned,
        break_time,
        CASE
            WHEN "case" = 'IN'
                THEN date_part('hour', log_time - time_start)
            ELSE 0
        END AS login_diff_hours,
        CASE
            WHEN "case" = 'IN'
                THEN date_part('hour', log_time - time_start) * 60 + 
                     date_part('minute', log_time - time_start)
            ELSE 0
        END AS login_diff_minutes,
        CASE
            WHEN "case" = 'OUT'
                THEN date_part('hour', log_time - time_end)
            ELSE 0
        END AS logout_diff_hours,
        CASE
            WHEN "case" = 'OUT'
                THEN date_part('hour', log_time - time_end) * 60 + 
                     date_part('minute', log_time - time_end)
            ELSE 0
        END AS logout_diff_minutes
    FROM
        attendance_merged
),
attendance_merged_with_diffs_classified AS (
    SELECT
        user_id,
        "location",
        gender,
        date_hire,
        date_leave,
        "position",
        department,
        log_date,
        log_time,
        log_timezone,
        "case",
        "source", 
        "type",
        time_start,
        time_end,
        time_planned,
        break_time,
        login_diff_minutes,
        logout_diff_minutes,
        CASE
            WHEN (login_diff_minutes > 10 AND
                  login_diff_minutes <= 120) THEN 'Yes'
            ELSE 'No'
        END AS is_tardy
    FROM 
    attendance_merged_with_diffs
)
SELECT
    department,
    COUNT(
        CASE
            WHEN is_tardy = 'Yes'
                THEN is_tardy
            END
    ) AS tardiness_count
FROM
    attendance_merged_with_diffs_classified
GROUP BY
    department
ORDER BY
    2 DESC;


/* Summary of leave counts by weekday */
WITH leave_counts_by_day AS (
    SELECT
        EXTRACT(ISODOW FROM "date") AS dow,
        TO_CHAR("date", 'Dy') AS leave_day,
        COUNT(leave_type) AS leave_count
    FROM
        leave_requests
    GROUP BY
        1, 2
    ORDER BY
        1
)
SELECT
    leave_day AS weekday,
    leave_count 
FROM
    leave_counts_by_day;


/* Summary of leave counts by month */
WITH leave_counts_by_month AS (
    SELECT
        EXTRACT(MONTH FROM "date") AS month_order,
        TO_CHAR("date", 'Mon') AS leave_month,
        COUNT(leave_type) AS leave_count
    FROM
        leave_requests
    GROUP BY
        1, 2
    ORDER BY
        1
)
SELECT
    leave_month AS "month",
    leave_count 
FROM
    leave_counts_by_month;


/* Summary of leave counts by employee */
WITH leave_counts_by_emp AS (
    SELECT 
        lr.user_id,
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position",
        u.department,
        leave_type,
        COUNT(leave_type) AS leave_count
    FROM
        leave_requests lr
    JOIN
        users u
        ON lr.user_id = u.user_id
    WHERE
        status = 'Accepted'
    GROUP BY
        1, 2, 3, 4
    ORDER BY
        1, 5 DESC
)
SELECT
    user_id,
    "position",
    SUM(leave_count) AS leave_count
FROM
    leave_counts_by_emp
GROUP BY
    1, 2
ORDER BY
    3 DESC;


/* Summary of leave counts by department */
WITH leave_counts_by_emp AS (
    SELECT 
        lr.user_id,
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position",
        u.department,
        leave_type,
        COUNT(leave_type) AS leave_count
    FROM
        leave_requests lr
    JOIN
        users u
        ON lr.user_id = u.user_id
    WHERE
        status = 'Accepted'
    GROUP BY
        1, 2, 3, 4
    ORDER BY
        1, 5 DESC
)
SELECT
    department,
    SUM(leave_count) AS leave_count
FROM
    leave_counts_by_emp
GROUP BY
    department
ORDER BY
    2 DESC;
