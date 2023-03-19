SELECT
    city_id
    ,city.city
    ,state.state
    ,country.country
FROM {{ source('raw','city')}} AS city
JOIN {{ source('raw','state')}} AS state
    ON city.state_id = state.state_id
JOIN {{ source('raw','country')}} AS country
    ON country.country_id = state.country_id