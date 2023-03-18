SELECT
    account_id
    ,customer_id
    ,created_at
    ,status
    ,account_branch
    ,account_number
    ,account_check_digit
FROM {{ source('raw','account')}} AS city