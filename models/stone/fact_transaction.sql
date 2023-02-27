{{
    config(
        materialized='incremental',
        unique_key='codigo_transacao',
        sort=['dt_transacao','codigo_usuario'],
        dist='auto'
    )
}}

SELECT
    codigo_transacao
    ,dt_transacao
    ,valor_transacao
    ,estado_transacao
    ,codigo_usuario
    ,{{ dbt_utils.generate_surrogate_key(
        ['metodo_captura','metodo_pagamento','bandeira_cartao']
    ) }} AS payment_id
FROM {{ ref('base_transaction') }} AS base_transaction
WHERE TRUE
    {% if is_incremental() %}
    AND DATEADD(day, -120, dt_transacao) >= (SELECT MAX(dt_transacao) FROM {{ this }})
    {% endif %}
