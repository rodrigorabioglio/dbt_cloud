{{ config(materialized="incremental", unique_key="inc_key") }}

WITH
    active_months as (
        SELECT DISTINCT 
            codigo_usuario
            ,date_trunc('month', dt_transacao) active_month
        FROM {{ ref('base_transaction') }}
        WHERE TRUE
            AND dt_transacao < (SELECT MAX(DATE_TRUNC('month', dt_transacao)) FROM {{ ref('base_transaction') }})
            -- job mensal roda no final do 1 dia do novo mês
            {% if is_incremental() %}
            AND DATE_TRUNC('month', dt_transacao) > (SELECT MAX(build_control) FROM {{ this }})
            {% endif %}
    )

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['codigo_usuario','date_add(\'month\', 1, active_month)']
    ) }} AS inc_key
    ,active_month AS build_control
    ,codigo_usuario
    ,date_add('month', 1, active_month) AS reference_month
    ,NOT NVL(LEAD(active_month) OVER (PARTITION BY codigo_usuario ORDER BY active_month), '1990-01-01')
    = dateadd(month, 1, active_month) churned
FROM active_months
