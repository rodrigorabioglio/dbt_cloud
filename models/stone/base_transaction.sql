{{ config(materialized='table') }}

SELECT
    "codigo da transacao" AS codigo_transacao, 
	"data e hora da transacao" AS dt_transacao, 
	"metodo de captura" AS metodo_captura, 
	"bandeira do cartao" AS bandeira_cartao, 
	"metodo de pagamento" AS metodo_pagamento, 
	"estado da transacao" AS estado_transacao , 
	"valor da transacao"::float AS valor_transacao, 
	"codigo do usuario" AS codigo_usuario, 
	"estado do usuario" AS uf_usuario, 
	"cidade do usuario" AS cidade_usuario
FROM public.transactions
WHERE TRUE
    AND "estado da transacao" IN ('PAID', 'REFUSED', 'REFUNDED', 'CHARGEDBACK') 