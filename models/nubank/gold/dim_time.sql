SELECT
    d_time.time_id
    ,d_time.action_timestamp
    ,d_week.action_week
    ,d_weekday.action_weekday
    ,d_month.action_month
    ,d_year.action_year
FROM {{ source('raw','d_time') }} d_time
JOIN {{ source('raw','d_week') }} d_week
    ON d_week.week_id = d_time.week_id
JOIN {{ source('raw','d_weekday') }} d_weekday
    ON d_weekday.weekday_id = d_time.weekday_id
JOIN {{ source('raw','d_month') }} d_month
    ON d_month.month_id = d_time.month_id
JOIN {{ source('raw','d_year') }} d_year
    ON d_year.year_id = d_time.year_id