SELECT
    id
    ,account_id
    ,pix_requested_at
    ,pix_completed_at
    ,status
    ,'pix' AS transaction_method
    ,CASE
        WHEN pix_movements.in_or_out = 'pix_out' THEN 'debit'
        WHEN pix_movements.in_or_out = 'pix_in' THEN 'credit'
    END AS transaction_type
    ,amount 
FROM {{ source('raw','pix_movements') }}