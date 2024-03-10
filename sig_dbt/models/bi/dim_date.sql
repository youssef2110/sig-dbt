SELECT date_generated::DATE,
       EXTRACT(YEAR FROM date_generated)                                             AS year,
       EXTRACT(QUARTER FROM date_generated)                                          AS quarter,
       EXTRACT(MONTH FROM date_generated)                                            AS month,
       TO_CHAR(date_generated, 'Month')                                              AS month_name,
       EXTRACT(WEEK FROM date_generated)                                             AS week_of_year,
       EXTRACT(DAY FROM date_generated)                                              AS day_of_month,
       EXTRACT(DOW FROM date_generated)                                              AS day_of_week,
       TO_CHAR(date_generated, 'Day')                                                AS day_name,
       CASE WHEN EXTRACT(DOW FROM date_generated) IN (6, 0) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series('2023-01-01'::date, '2030-12-31', '1 day'::interval) date_generated