{{ config(
    materialized='table',
    dist='auto',
    sort=['dt_transacao','codigo_usuario']
    ) }}

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
