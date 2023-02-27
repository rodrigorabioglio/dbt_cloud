{{
    config(
        materialized='incremental',
        unique_key='codigo_transacao',
        sort='payment_id',
        dist='all'
    )
}}

WITH base_payment AS (
    SELECT DISTINCT
        metodo_captura
        ,metodo_pagamento
        ,bandeira_cartao
    FROM {{ ref('base_transaction') }}
    WHERE TRUE
        {% if is_incremental() %}
        AND DATEADD(day, -120, dt_transacao) >= (SELECT MAX(dt_transacao) FROM {{ this }})
        {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['metodo_captura','metodo_pagamento','bandeira_cartao']
    ) }} AS payment_id
    ,metodo_captura
    ,metodo_pagamento
    ,bandeira_cartao
    FROM base_payment