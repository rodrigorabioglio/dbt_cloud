SELECT
    transactions.id
    ,transactions.account_id
    ,customer.customer_id
    ,requested_time.action_timestamp AS requested_at
    ,completed_time.action_timestamp AS completed_at
    ,transactions.status
    ,transactions.transaction_method
    ,transactions.transaction_type
    ,transactions.amount
    ,account.account_branch
    ,account.account_number
    ,account.account_check_digit
    ,city.city AS customer_city
    ,city.state AS customer_state
    ,city.country AS customer_country
FROM {{ ref('fact_transactions_p2p') }} AS transactions
JOIN {{ ref('dim_account') }} AS account
    ON account.account_id = transactions.account_id
JOIN {{ ref('dim_customer') }} AS customer
    ON customer.customer_id = account.customer_id
JOIN {{ ref('dim_city') }} AS city
    ON customer.city_id = city.city_id
JOIN {{ ref('dim_time') }} AS requested_time
    ON requested_time.time_id = transactions.requested_at
JOIN {{ ref('dim_time') }} AS completed_time
    ON completed_time.time_id = transactions.requested_at