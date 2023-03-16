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
    - 1st level: Exclude empty columns, remove duplicates
                 and standardize column values
    - 2nd level: Reclassify rows based from "case", group and
                 join to obtain login and logout times for
                 each row
*/
DROP TABLE IF EXISTS attendance;
CREATE TABLE IF NOT EXISTS attendance AS (
    WITH attendance_cleaned AS (
        SELECT DISTINCT
            user_id,
            COALESCE("location", 'None') AS "location",
            "date",
            "time",
            timezone,
            "case",
            INITCAP("source") AS "source"
        FROM
            attendance_raw
        ORDER BY
            user_id,
            "date",
            "case"
    )
    SELECT
        login.user_id,
        login."date" AS log_date,
        login.location as login_location,
        login."time" AS login_time,
        login.timezone AS login_timezone,
        login."source" AS login_source,
        logout.location as logout_location,
        logout."time" AS logout_time,
        logout.timezone AS logout_timezone,
        logout."source" AS logout_source
    FROM (
        SELECT
            user_id,
            location,
            "date",
            timezone,
            "source",
            MIN("time") AS "time"
        FROM 
            attendance_cleaned
        WHERE
            "case" = 'IN'
        GROUP BY 
            user_id,
            location,
            "date",
            timezone,
            "source"
    ) AS login
    LEFT JOIN (
        SELECT
            user_id,
            location,
            "date",
            timezone,
            "source",
            MAX("time") AS "time"
        FROM 
            attendance_cleaned
        WHERE
            "case" = 'OUT'
        GROUP BY 
            user_id,
            location,
            "date",
            timezone,
            "source"
    ) AS logout
    ON
        login.user_id = logout.user_id
        AND login."date" = logout."date"
    ORDER BY
        1, 2, 4
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
        COALESCE(INITCAP(gender), 'Other') AS gender,
        date_birth,
        date_hire,
        date_leave,
        REPLACE(INITCAP(COALESCE(employment, 'full_time')), '_', ' ') AS employment,
        COALESCE("position", 'None') AS "position",
        COALESCE("location", 'None') AS "location",
        COALESCE(department, 'None') AS department,
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
        COALESCE(ctc, 0) AS ctc,
        COALESCE(net_pay, 0) AS net_pay,
        COALESCE(gross_pay, 0) AS gross_pay,
        data_salary_basic_rate,
        INITCAP(data_salary_basic_type) AS data_salary_basic_type,
        COALESCE(currency, 'None') AS currency,
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
            REPLACE(INITCAP(leave_type), '_', ' ') AS leave_type,
            JSONB_ARRAY_ELEMENTS_TEXT(dates)::date AS "date",
            INITCAP(status) AS status,
            created_at
        FROM
            leave_requests_raw
        WHERE
            user_id IS NOT NULL
    )
    SELECT
        DISTINCT *
    FROM
        leave_requests_simplified
    ORDER BY
        user_id,
        "date"
);


/*
    Create new cleaned table schedules:
    - 1st level: Expand the user_id bigint array column into
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
            COALESCE(time_planned, 0) AS time_planned,
            COALESCE(break_time, 0) AS break_time,
            REPLACE(INITCAP(COALESCE(leave_type, 'None')), '_', ' ') AS leave_type,
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
        user_id,
        "date"
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
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position" AS "position",
        u.department,
        att.log_date,
        MIN(att.login_time) AS login_time,
        MAX(att.logout_time) AS logout_time,
        att.login_timezone,
        att.logout_timezone,
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
        AND att.log_date = sch."date"
    INNER JOIN
        users u
        ON u.user_id = att.user_id
        AND u.user_id = sch.user_id
    WHERE
        sch."type" = 'Work'
    GROUP BY
        att.user_id,
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position",
        u.department,
        att.log_date,
        att.login_timezone,
        att.logout_timezone,
        sch."type",
        sch.time_start,
        sch.time_end,
        sch.timezone,
        sch.time_planned,
        sch.break_time
),
attendance_merged_with_diffs AS (
    SELECT
        user_id,
        gender,
        date_hire,
        date_leave,
        "position",
        department,
        log_date,
        login_time,
        logout_time,
        time_start,
        time_end,
        time_planned,
        break_time,
        "type",
        DATE_PART('hour', login_time - time_start) AS login_diff_hours,
        DATE_PART('hour', login_time - time_start) * 60 + 
            DATE_PART('minute', login_time - time_start) AS login_diff_minutes,
        DATE_PART('hour', logout_time - time_end) AS logout_diff_hours,
        DATE_PART('hour', logout_time - time_end) * 60 + 
            DATE_PART('minute', logout_time - time_end) AS logout_diff_minutes
    FROM
        attendance_merged
),
attendance_merged_with_diffs_classified AS (
    SELECT
        user_id,
        gender,
        date_hire,
        date_leave,
        "position",
        department,
        log_date,
        login_time,
        logout_time,
        time_start,
        time_end,
        time_planned,
        break_time,
        "type",
        login_diff_hours,
        login_diff_minutes,
        logout_diff_hours,
        logout_diff_minutes,
        CASE
            WHEN (login_diff_minutes > 10 AND
                  login_diff_minutes <= 120)
                THEN 'Yes'
            ELSE 'No'
        END AS is_tardy,
        CASE
            WHEN (logout_time IS NOT NULL AND
                  (logout_diff_minutes < 0 AND
                    logout_diff_minutes >= -120))
                THEN 'Yes'
            ELSE 'No'
        END AS is_undertime,
        CASE
            WHEN logout_time IS NULL
                THEN 'Yes'
            ELSE 'No'
        END AS no_logout
    FROM 
        attendance_merged_with_diffs
)
SELECT
    user_id,
    "position",
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
    user_id,
    "position",
    department
ORDER BY
    4 DESC;


/* Summary of tardiness by department */
WITH attendance_merged AS (
    SELECT
        att.user_id,
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position" AS "position",
        u.department,
        att.log_date,
        MIN(att.login_time) AS login_time,
        MAX(att.logout_time) AS logout_time,
        att.login_timezone,
        att.logout_timezone,
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
        AND att.log_date = sch."date"
    INNER JOIN
        users u
        ON u.user_id = att.user_id
        AND u.user_id = sch.user_id
    WHERE
        sch."type" = 'Work'
    GROUP BY
        att.user_id,
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position",
        u.department,
        att.log_date,
        att.login_timezone,
        att.logout_timezone,
        sch."type",
        sch.time_start,
        sch.time_end,
        sch.timezone,
        sch.time_planned,
        sch.break_time
),
attendance_merged_with_diffs AS (
    SELECT
        user_id,
        gender,
        date_hire,
        date_leave,
        "position",
        department,
        log_date,
        login_time,
        logout_time,
        time_start,
        time_end,
        time_planned,
        break_time,
        "type",
        DATE_PART('hour', login_time - time_start) AS login_diff_hours,
        DATE_PART('hour', login_time - time_start) * 60 + 
            DATE_PART('minute', login_time - time_start) AS login_diff_minutes,
        DATE_PART('hour', logout_time - time_end) AS logout_diff_hours,
        DATE_PART('hour', logout_time - time_end) * 60 + 
            DATE_PART('minute', logout_time - time_end) AS logout_diff_minutes
    FROM
        attendance_merged
),
attendance_merged_with_diffs_classified AS (
    SELECT
        user_id,
        gender,
        date_hire,
        date_leave,
        "position",
        department,
        log_date,
        login_time,
        logout_time,
        time_start,
        time_end,
        time_planned,
        break_time,
        "type",
        login_diff_hours,
        login_diff_minutes,
        logout_diff_hours,
        logout_diff_minutes,
        CASE
            WHEN (login_diff_minutes > 10 AND
                  login_diff_minutes <= 120)
                THEN 'Yes'
            ELSE 'No'
        END AS is_tardy,
        CASE
            WHEN (logout_time IS NOT NULL AND
                  (logout_diff_minutes < 0 AND
                    logout_diff_minutes >= -180))
                THEN 'Yes'
            ELSE 'No'
        END AS is_undertime,
        CASE
            WHEN logout_time IS NULL
                THEN 'Yes'
            ELSE 'No'
        END AS no_logout
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
        EXTRACT(ISODOW FROM lr."date") AS dow,
        TO_CHAR(lr."date", 'Dy') AS leave_day,
        COUNT(lr.leave_type) AS leave_count
    FROM
        leave_requests lr
    JOIN
        schedules sch
        ON lr.user_id = sch.user_id
        AND lr."type" = sch."type"
        AND lr."date" = sch."date"
    WHERE
        lr."type" = 'Leave'
        AND lr.status = 'Accepted'
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
WITH min_max_dates AS (
    SELECT
        DATE_TRUNC('month', MIN("date")) AS min, 
        DATE_TRUNC('month', MAX("date")) AS max
    FROM
        leave_requests
),
months_from_dates as (
    SELECT
        TO_CHAR(GENERATE_SERIES(min, max, '1 month'), 'Mon') AS "month",
        0 AS leave_count
    FROM
        min_max_dates
),
leave_counts_by_month AS (
    SELECT
        TO_CHAR(lr."date", 'Mon') AS "month",
        COUNT(lr.leave_type) AS leave_count
    FROM
        leave_requests lr
    JOIN
        schedules sch
        ON lr.user_id = sch.user_id
        AND lr."type" = sch."type"
        AND lr."date" = sch."date"
    WHERE
        lr."type" = 'Leave'
        AND lr.status = 'Accepted'
    GROUP BY
        1
),
leave_counts_by_month_joined AS (
    SELECT
        "month",
        leave_count 
    FROM
        leave_counts_by_month lcbm
    UNION ALL
    SELECT
        *
    FROM
        months_from_dates mfd
    WHERE NOT EXISTS (
        SELECT
            1
        FROM
            leave_counts_by_month lcbm
        WHERE
            lcbm."month" = mfd."month"
    )
)
SELECT
    *
FROM
    leave_counts_by_month_joined
ORDER BY
    EXTRACT(MONTH FROM TO_DATE("month", 'Mon'));


/* Summary of approved leave counts by employee */
WITH leave_counts_by_emp AS (
    SELECT 
        lr.user_id,
        u.gender,
        u.date_hire AS date_hired,
        u.date_leave AS date_left,
        u."position",
        u.department,
        lr."date" AS leave_date,
        lr.leave_type,
        lr.status,
        COUNT(lr.leave_type) AS leave_count
    FROM
        leave_requests lr
    JOIN
        schedules sch
        ON lr.user_id = sch.user_id
        AND lr."type" = sch."type"
        AND lr."date" = sch."date"
    JOIN
        users u
        ON lr.user_id = u.user_id
    WHERE
        lr."type" = 'Leave'
        AND lr.status = 'Accepted'
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9
    ORDER BY
        1, 10 DESC
)
SELECT
    user_id,
    "position",
    department,
    SUM(leave_count) AS leave_count
FROM
    leave_counts_by_emp
GROUP BY
    1, 2, 3
ORDER BY
    4 DESC;


/* Summary of approved leave counts by department */
WITH leave_counts_by_emp AS (
    SELECT 
        lr.user_id,
        u.gender,
        u.date_hire AS date_hired,
        u.date_leave AS date_left,
        u."position",
        u.department,
        lr."date" AS leave_date,
        lr.leave_type,
        lr.status,
        COUNT(lr.leave_type) AS leave_count
    FROM
        leave_requests lr
    JOIN
        schedules sch
        ON lr.user_id = sch.user_id
        AND lr."type" = sch."type"
        AND lr."date" = sch."date"
    JOIN
        users u
        ON lr.user_id = u.user_id
    WHERE
        lr."type" = 'Leave'
        AND lr.status = 'Accepted'
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9
    ORDER BY
        1, 10 DESC
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


/* Summary of approved leave counts by gender */
WITH leave_counts_by_emp AS (
    SELECT 
        lr.user_id,
        u.gender,
        u.date_hire AS date_hired,
        u.date_leave AS date_left,
        u."position",
        u.department,
        lr."date" AS leave_date,
        lr.leave_type,
        lr.status,
        COUNT(lr.leave_type) AS leave_count
    FROM
        leave_requests lr
    JOIN
        schedules sch
        ON lr.user_id = sch.user_id
        AND lr."type" = sch."type"
        AND lr."date" = sch."date"
    JOIN
        users u
        ON lr.user_id = u.user_id
    WHERE
        lr."type" = 'Leave'
        AND lr.status = 'Accepted'
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9
    ORDER BY
        1, 10 DESC
)
SELECT
    gender,
    leave_type,
    SUM(leave_count) AS leave_count
FROM
    leave_counts_by_emp
GROUP BY
    gender
ORDER BY
    3 DESC;


/* Summary of leave counts by approval status */
WITH leave_counts_by_emp AS (
    SELECT 
        lr.user_id,
        u.gender,
        u.date_hire AS date_hired,
        u.date_leave AS date_left,
        u."position",
        u.department,
        lr."date" AS leave_date,
        lr.leave_type,
        lr.status,
        COUNT(lr.leave_type) AS leave_count
    FROM
        leave_requests lr
    JOIN
        schedules sch
        ON lr.user_id = sch.user_id
        AND lr."type" = sch."type"
        AND lr."date" = sch."date"
    JOIN
        users u
        ON lr.user_id = u.user_id
    WHERE
        lr."type" = 'Leave'
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9
    ORDER BY
        1, 10 DESC
)
SELECT
    status,
    SUM(leave_count) AS leave_count
FROM
    leave_counts_by_emp
GROUP BY
    status
ORDER BY
    2 DESC;



/* Summary of log counts done on frontend by employee */
WITH attendance_merged AS (
    SELECT
        att.user_id,
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position" AS "position",
        u.department,
        att.log_date,
        att.login_location,
        att.login_time,
        att.login_timezone,
        att.login_source,
        att.logout_location,
        att.logout_time,
        att.logout_timezone,
        att.logout_source,
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
        AND att.log_date = sch."date"
    INNER JOIN
        users u
        ON u.user_id = att.user_id
        AND u.user_id = sch.user_id
    WHERE
        sch."type" = 'Work'
)
SELECT
    user_id,
    "position",
    department,
    COUNT(
        CASE
            WHEN login_source = 'Frontend'
                THEN login_source
            END
    ) AS frontend_login_count,
    COUNT(
        CASE
            WHEN logout_source = 'Frontend'
                THEN logout_source
            END
    ) AS frontend_logout_count
FROM
    attendance_merged
GROUP BY
    user_id,
    "position",
    department
ORDER BY
    4 DESC,
    5 DESC;


/* Summary of log counts done on frontend by department */
WITH attendance_merged AS (
    SELECT
        att.user_id,
        u.gender,
        u.date_hire,
        u.date_leave,
        u."position" AS "position",
        u.department,
        att.log_date,
        att.login_location,
        att.login_time,
        att.login_timezone,
        att.login_source,
        att.logout_location,
        att.logout_time,
        att.logout_timezone,
        att.logout_source,
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
        AND att.log_date = sch."date"
    INNER JOIN
        users u
        ON u.user_id = att.user_id
        AND u.user_id = sch.user_id
    WHERE
        sch."type" = 'Work'
)
SELECT
    department,
    COUNT(
        CASE
            WHEN login_source = 'Frontend'
                THEN login_source
            END
    ) AS frontend_login_count,
    COUNT(
        CASE
            WHEN logout_source = 'Frontend'
                THEN logout_source
            END
    ) AS frontend_logout_count
FROM
    attendance_merged
GROUP BY
    department
ORDER BY
    2 DESC,
    3 DESC;
