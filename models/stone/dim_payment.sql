{{ config(materialized='table',
    dist='all'
    ) }}

WITH base_payment AS (
    SELECT DISTINCT
        metodo_captura
        ,metodo_pagamento
        ,bandeira_cartao
    FROM {{ ref('base_transaction') }}    
)

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['metodo_captura','metodo_pagamento','bandeira_cartao']
    ) }} AS payment_id
    ,metodo_captura
    ,metodo_pagamento
    ,bandeira_cartao
FROM base_payment