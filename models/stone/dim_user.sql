{{ config(materialized='table'
    dist='auto',
    sort='codigo_usuario'
    ) }}

SELECT DISTINCT
    codigo_usuario
    ,cidade_usuario AS cidade 
    ,uf_usuario AS uf
FROM {{ ref('base_transaction') }}