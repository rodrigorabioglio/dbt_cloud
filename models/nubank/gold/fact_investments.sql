SELECT
    transaction_id AS id
    ,account_id
    ,investment_requested_at_timestamp AS requested_at_timestamp
    ,investment_requested_at AS requested_at
    ,investment_completed_at AS completed_at
    ,status
    ,CASE
        WHEN investment_transfer_out = 'pix_out' THEN 'debit'
        WHEN investment_transfer_in = 'pix_in' THEN 'credit'
    END AS transaction_type
    ,amount 
FROM {{ source('raw','investments') }}