-- SILVER: adaptado al schema existente (sin migración)

INSERT INTO
    demo.silver_customers
SELECT CAST(ingest_ts AS TIMESTAMP) AS ingest_ts,

-- Mantener STRING (no INT)
get_json_object (raw_payload, '$.customer_id') AS customer_id,

-- name → customer_name
NULLIF(
    TRIM(
        get_json_object (raw_payload, '$.name')
    ),
    ''
) AS customer_name,

-- email
NULLIF( TRIM( get_json_object (raw_payload, '$.email') ), '' ) AS email,

-- status derivado de reglas de calidad
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
END AS status,

-- payload: guardamos el raw completo (trazabilidad)
raw_payload AS payload,

-- Quality flags
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