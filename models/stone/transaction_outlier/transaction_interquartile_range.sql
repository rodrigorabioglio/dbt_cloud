{{
    config(
        materialized='table'
    )
}}


SELECT
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) AS iqr
FROM {{ ref('transaction_dist') }} 
