SELECT
    customer_id
    ,first_name
    ,last_name
    ,customer_city
    ,cpf
FROM {{ source('raw','customers') }}