{{
    config(
        materialized='incremental',
        unique_key='codigo_transacao',
        sort='dt_transacao',
        dist='auto'
    )
}}

WITH deduped_transactions AS (
    SELECT
        "codigo da transacao" AS codigo_transacao
    FROM public.transactions
    WHERE TRUE
        AND "estado da transacao" IN ('PAID', 'REFUSED', 'REFUNDED', 'CHARGEDBACK')
        {% if is_incremental() %}
        AND DATEADD(day, 120, "data e hora da transacao") >= (SELECT MAX(dt_transacao) FROM {{ this }})
        {% endif %} 
    GROUP BY 1
    HAVING COUNT(*) > 1
)

SELECT
    t."codigo da transacao" AS codigo_transacao, 
	CONVERT_TIMEZONE('America/Sao_Paulo',t."data e hora da transacao") AS dt_transacao, 
	t."metodo de captura" AS metodo_captura, 
	t."bandeira do cartao" AS bandeira_cartao, 
	t."metodo de pagamento" AS metodo_pagamento, 
	t."estado da transacao" AS estado_transacao , 
	t."valor da transacao"::float AS valor_transacao, 
	t."codigo do usuario" AS codigo_usuario, 
	t."estado do usuario" AS uf_usuario, 
	t."cidade do usuario" AS cidade_usuario
FROM public.transactions t
LEFT JOIN deduped_transactions dt
    ON t."codigo da transacao" = dt.codigo_transacao
WHERE TRUE
    AND dt.codigo_transacao IS NULL
    AND "estado da transacao" IN ('PAID', 'REFUSED', 'REFUNDED', 'CHARGEDBACK')
    {% if is_incremental() %}
    AND DATEADD(day, 120, CONVERT_TIMEZONE('America/Sao_Paulo',t."data e hora da transacao")) >= (SELECT MAX(dt_transacao) FROM {{ this }})
    {% endif %}
