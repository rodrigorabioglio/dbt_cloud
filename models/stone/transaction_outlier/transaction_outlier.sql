{{
    config(
        materialized='incremental',
        unique_key='reference_week',
    )
}}

SELECT *
FROM (
    SELECT
        DATE_TRUNC('week', dt_transacao) AS reference_week
        {%- for p in range(0,10) %}
        ,PERCENTILE_CONT(0.{{p}}) WITHIN GROUP (ORDER BY valor_transacao) AS percentile_{{p}}0
        {%- endfor %}
        ,MAX(valor_transacao) AS percentile_100
    FROM {{ ref('base_transaction') }}
    WHERE TRUE
        {% if is_incremental() %}
        AND DATE_TRUNC('week', dt_transacao) >= (SELECT MAX(reference_week) FROM {{ this }})
        {% endif %} 
    GROUP BY 1) 
    UNPIVOT (
        value for percentile in ({%- for p in range(0,11) %}
            {%- if p<10 %}
                percentile_{{p}}0,
            {%- else %}
                percentile_{{p}}0
            {%- endif %}
    {%- endfor %})
    )