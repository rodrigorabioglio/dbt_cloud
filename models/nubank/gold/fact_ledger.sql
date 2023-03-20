SELECT *
FROM {{ ref('fact_transactions_p2p') }}

UNION ALL

SELECT
    id
    ,account_id
    ,requested_at
    ,completed_at
    ,status
    ,product
    ,transaction_type
    ,amount
FROM {{ ref('fact_investments')}
