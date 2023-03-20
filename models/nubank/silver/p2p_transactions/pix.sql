SELECT
    id
    ,account_id
    ,pix_requested_at AS requested_at
    ,pix_completed_at AS completed_at
    ,status
    ,'pix' AS product
    ,CASE
        WHEN in_or_out = 'pix_out' THEN 'debit'
        WHEN in_or_out = 'pix_in' THEN 'credit'
    END AS transaction_type
    ,amount 
FROM {{ source('raw','pix_movements') }}