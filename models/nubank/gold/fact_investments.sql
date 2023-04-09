SELECT
    transaction_id AS id
    ,account_id
    ,investment_requested_at_timestamp AS requested_at_timestamp
    ,investment_requested_at AS requested_at
    ,investment_completed_at AS completed_at
    ,status
    ,'investment' AS product
    ,CASE
        WHEN type = 'investment_transfer_out' THEN 'debit'
        WHEN type = 'investment_transfer_in' THEN 'credit'
    END AS transaction_type
    ,amount 
FROM {{ source('raw','investments') }}