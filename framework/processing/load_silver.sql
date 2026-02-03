-- SILVER: parse + clean + quality flags
CREATE TABLE IF NOT EXISTS demo.silver_customers (
    ingest_ts TIMESTAMP,
    customer_id INT,
    name STRING,
    age INT,
    country STRING,
    email STRING,
    dq_is_name_null BOOLEAN,
    dq_is_underage BOOLEAN,
    dq_status STRING
);

INSERT INTO
    demo.silver_customers
SELECT
    CAST(ingest_ts AS TIMESTAMP) AS ingest_ts,
    CAST(
        get_json_object (raw_payload, '$.customer_id') AS INT
    ) AS customer_id,
    NULLIF(
        TRIM(
            get_json_object (raw_payload, '$.name')
        ),
        ''
    ) AS name,
    CAST(
        get_json_object (raw_payload, '$.age') AS INT
    ) AS age,
    NULLIF(
        TRIM(
            get_json_object (raw_payload, '$.country')
        ),
        ''
    ) AS country,
    NULLIF(
        TRIM(
            get_json_object (raw_payload, '$.email')
        ),
        ''
    ) AS email,
    CASE
        WHEN NULLIF(
            TRIM(
                get_json_object (raw_payload, '$.name')
            ),
            ''
        ) IS NULL THEN TRUE
        ELSE FALSE
    END AS dq_is_name_null,
    CASE
        WHEN CAST(
            get_json_object (raw_payload, '$.age') AS INT
        ) < 18 THEN TRUE
        ELSE FALSE
    END AS dq_is_underage,
    CASE
        WHEN NULLIF(
            TRIM(
                get_json_object (raw_payload, '$.name')
            ),
            ''
        ) IS NULL THEN 'FAIL_NAME_NULL'
        WHEN CAST(
            get_json_object (raw_payload, '$.age') AS INT
        ) < 18 THEN 'WARN_UNDERAGE'
        ELSE 'PASS'
    END AS dq_status
FROM demo.bronze_customers;