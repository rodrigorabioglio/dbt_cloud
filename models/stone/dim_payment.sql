{{
    config(
        materialized='incremental',
        unique_key='payment_id',
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
        AND dt_transacao >= (SELECT MAX(dt_transacao) FROM {{ ref('base_transaction') }})
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