{{ 
    config(
        materialized='incremental',
        unique_key='location_id',
        dist='location_id',
        sort='location_id'
    ) 
}}

SELECT DISTINCT
    CONVERT_TIMEZONE('America/Sao_Paulo',GETDATE()) AS last_build_dt
    ,{{ dbt_utils.generate_surrogate_key(
        ['cidade_usuario','uf_usuario']
    ) }} AS location_id
    ,cidade_usuario AS cidade 
    ,uf_usuario AS uf
FROM {{ ref('base_transaction') }}
WHERE TRUE
    {% if is_incremental() %}
    AND dt_transacao >= (SELECT MAX(last_build_dt) FROM {{ this }})
    {% endif %}