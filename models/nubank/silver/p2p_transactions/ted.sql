SELECT
    id
    ,account_id
    ,transaction_requested_at AS requested_at
    ,transaction_completed_at AS completed_at
    ,status
    ,'ted' AS transaction_method
    ,'credit' AS transaction_type
    ,amount
FROM {{ source('raw','transfer_ins') }}

UNION ALL

SELECT
    id
    ,account_id
    ,transaction_requested_at
    ,transaction_completed_at
    ,status
    ,'ted' AS transaction_method
    ,'debit' AS transaction_type
    ,amount
FROM {{ source('raw','transfer_outs') }}