/*--------------------------------------------
    DATA CLEANING
    1. Loading of CSV file to table
    2. Data validations
    3. Data transformations
*/--------------------------------------------

/* 
    Create invoices table
    Once table is created, import data from the CSV file
*/
DROP TABLE IF EXISTS invoices;
CREATE TABLE IF NOT EXISTS invoices (
    country VARCHAR(50),
    customer_id VARCHAR(16),
    invoice_number BIGINT UNIQUE NOT NULL,
    invoice_generated_date DATE,
    invoice_due_date DATE,
    invoice_amount INTEGER,
    disputed BOOLEAN,
    dispute_lost BOOLEAN,
    invoice_settled_date DATE,
    days_to_settle INTEGER,
    days_late INTEGER
);

/*
    Verify if all rows from the CSV file were successfully
    loaded to the table
*/
SELECT
    COUNT(invoice_number)
FROM
    invoices;

/* Data validations for each field */
SELECT
    DISTINCT customer_id
FROM
    invoices;

SELECT
    DISTINCT country
FROM
    invoices;

SELECT
    invoice_number
FROM
    invoices
WHERE
    invoice_generated_date IS NULL;

SELECT
    invoice_number
FROM
    invoices
WHERE
    invoice_due_date IS NULL;

SELECT
    invoice_number
FROM
    invoices
WHERE
    invoice_settled_date IS NULL;

SELECT
    invoice_number
FROM
    invoices
WHERE
    invoice_amount = NULL;

SELECT
    MIN(days_to_settle),
    MAX(days_to_settle),
    MIN(days_late),
    MAX(days_late)
FROM
    invoices;


/* Additional business logic verification */
SELECT
    *
FROM
    invoices
WHERE
    disputed = false AND
    dispute_lost = true;


/* Prepare new columns to hold the transformed values */
ALTER TABLE
    invoices
ADD COLUMN
    invoice_status varchar(50),
ADD COLUMN
    invoice_dispute_resolution varchar(50);

/*
    Transform the dispute columns into less technical values
    * disputed (0/false, 1/true)
        -> invoice_status ('Accepted', 'Disputed')
    * dispute_lost (0/false, 1/true)
        -> invoice_dispute_resolution ('In favor of Yellevate',
            'In favor of Customer')
*/
UPDATE invoices
SET 
    invoice_status = 
    CASE
        WHEN disputed = true
            THEN 'Disputed'
        ELSE 'Accepted'
    END,
    invoice_dispute_resolution = 
    CASE
        WHEN dispute_lost = true
            THEN 'In favor of Customer'
        ELSE 'In favor of Yellevate'
    END;

/* Drop the initial boolean columns */
ALTER TABLE
    invoices
DROP COLUMN
    disputed,
DROP COLUMN
    dispute_lost;


/*--------------------------------------------
    DATA ANALYSIS GOALS
*/--------------------------------------------
/*
    1) The processing time in which invoices are settled
    (average # of days rounded to a whole number).
*/

/* (a) Processing time per quarter */
SELECT
    EXTRACT(year FROM invoice_settled_date) AS year,
    'Qtr' || EXTRACT(quarter FROM invoice_settled_date) AS quarter,
    ROUND(AVG(days_to_settle), 0) AS average_days_to_settle,
    MAX(days_to_settle) AS longest_day_to_settle
FROM
    invoices
GROUP BY
    1, 2
ORDER BY
    1, 2;

/* (b) Overall processing time statistics */
SELECT
    ROUND(AVG(days_to_settle), 0) AS average_days_to_settle,
    ROUND(MIN(days_to_settle), 0) AS shortest_day_to_settle,
    ROUND(MAX(days_to_settle), 0) AS longest_day_to_settle
FROM
    invoices;


/*
    2) The processing time for the company to settle disputes
    (average # of days rounded to a whole number).
*/

/* (a) Processing time per quarter */
SELECT
    EXTRACT(year FROM invoice_settled_date) AS year,
    'Qtr' || EXTRACT(quarter FROM invoice_settled_date) AS quarter,
    ROUND(AVG(days_to_settle), 0) AS average_days_to_settle,
    MAX(days_to_settle) AS longest_day_to_settle
FROM
    invoices
WHERE
    invoice_status = 'Disputed'
GROUP BY
    1, 2
ORDER BY
    1, 2;

/* (b) Overall processing time statistics */
SELECT
    ROUND(AVG(days_to_settle), 0) AS average_days_to_settle,
    ROUND(MIN(days_to_settle), 0) AS shortest_day_to_settle,
    ROUND(MAX(days_to_settle), 0) AS longest_day_to_settle
FROM
    invoices
WHERE
    invoice_status = 'Disputed';


/*
    3) Percentage of disputes received by the company that were lost 
    (within two decimal places).
*/
SELECT
    invoice_dispute_resolution,
    COUNT(invoice_number) as invoice_count,
    ROUND(
        (COUNT(invoice_number) * 100) / (SUM(COUNT(invoice_number)) OVER () )
    , 2) AS percentage_disputes_lost
FROM
    invoices
WHERE
    invoice_status = 'Disputed'
GROUP BY
    invoice_dispute_resolution;


/*
    4) Percentage of revenue lost from disputes
    (within two decimal places).
*/
SELECT
    invoice_dispute_resolution,
    SUM(invoice_amount) as invoice_amount,
    ROUND(
        (SUM(invoice_amount) * 100) / (SUM(SUM(invoice_amount)) OVER () )
    , 2) AS percentage_revenue_lost_disputes
FROM
    invoices
WHERE
    invoice_status = 'Disputed'
GROUP BY
    invoice_dispute_resolution;


/*
    5) The country where the company reached the highest losses
    from lost disputes (in USD).
*/
SELECT
    country,
    SUM(invoice_amount) AS invoice_amount,
    COUNT(invoice_number) AS number_of_invoices
FROM
    invoices
WHERE
    invoice_status = 'Disputed' AND
    invoice_dispute_resolution = 'In favor of Customer'
GROUP BY
    country
ORDER BY
    2 DESC;


/*--------------------------------------------
    DATA ANALYSIS GOALS Summary
    - SQL command to show all answers
    as single resultset
*/--------------------------------------------
WITH data_analysis_goals AS
(
    SELECT
        /*
        1) The processing time in which invoices are settled (average # 
        of days rounded to a whole number)
        */
        (
            WITH settlement_time AS (
                SELECT
                    ROUND(AVG(days_to_settle), 0) AS average, 
                    ROUND(MIN(days_to_settle), 0) AS minimum,
                    ROUND(MAX(days_to_settle), 0) AS maximum
                FROM
                    invoices
            )
            SELECT
                average AS average_invoice_settlement_time
            FROM
                settlement_time
        ),
        /*
        2) The processing time for the company to settle disputes (average 
        # of days rounded to a whole number)
        */
        (
            WITH dispute_settlement_time AS (
                SELECT
                    ROUND(AVG(days_to_settle), 0) AS average, 
                    ROUND(MIN(days_to_settle), 0) AS minimum,
                    ROUND(MAX(days_to_settle), 0) AS maximum
                FROM
                    invoices
                WHERE
                    invoice_status = 'Disputed'
            )
            SELECT
                average AS average_dispute_settlement_time
            FROM
                dispute_settlement_time
        ),
        /*
        3) Percentage of disputes received by the company that were lost 
        (within two decimal places)
        */
        (
            WITH disputes_count AS (
                SELECT
                    invoice_dispute_resolution,
                    COUNT(invoice_number) as invoice_count,
                    ROUND(
                        (COUNT(invoice_number) * 100) / (SUM(COUNT(invoice_number)) OVER () )
                    , 2) AS percentage_disputes_lost
                FROM
                    invoices
                WHERE
                    invoice_status = 'Disputed'
                GROUP BY
                    invoice_dispute_resolution
            )
            SELECT
                percentage_disputes_lost
            FROM
                disputes_count
            WHERE
                invoice_dispute_resolution = 'In favor of Customer'
        ),
        /*
        4) Percentage of revenue lost from disputes (within two decimal 
        places)
        */
        (
            WITH disputes_sum AS (
                SELECT
                    invoice_dispute_resolution,
                    SUM(invoice_amount) as invoice_amount,
                    ROUND(
                        (SUM(invoice_amount) * 100) / (SUM(SUM(invoice_amount)) OVER () )
                    , 2) AS percentage_revenue_lost_disputes
                FROM
                    invoices
                WHERE
                    invoice_status = 'Disputed'
                GROUP BY
                    invoice_dispute_resolution
            )
            SELECT
                percentage_revenue_lost_disputes
            FROM
                disputes_sum
            WHERE
                invoice_dispute_resolution = 'In favor of Customer'
        ),
        /*
        5) The country where the company reached the highest losses from 
        lost disputes (in USD)
        */
        (
            WITH revenue_lost AS (
                SELECT
                    country,
                    SUM(invoice_amount) AS invoice_amount,
                    COUNT(invoice_number) AS number_of_invoices
                FROM
                    invoices
                WHERE
                    invoice_status = 'Disputed' AND
                    invoice_dispute_resolution = 'In favor of Customer'
                GROUP BY
                    country
                ORDER BY
                    2 DESC
            )
            SELECT
                country AS top_country_revenue_loss
            FROM
                revenue_lost
            LIMIT 1
        )
)
SELECT * FROM data_analysis_goals;
