SELECT *
FROM {{ ref('pix') }}

UNION ALL

SELECT *
FROM {{ ref('ted') }}