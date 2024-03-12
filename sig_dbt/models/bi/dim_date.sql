SELECT date_generated::DATE,
       EXTRACT(YEAR FROM date_generated)                                             AS year,
       EXTRACT(QUARTER FROM date_generated)                                          AS quarter,
       EXTRACT(MONTH FROM date_generated)                                            AS month,

        CASE
                                  WHEN EXTRACT(MONTH FROM date_generated) <= 6 THEN 1
                                  ELSE 2
        END                             AS semestre,
        
        CASE
                                  WHEN EXTRACT(MONTH FROM date_generated) <= 4 THEN 1
                                  WHEN EXTRACT(MONTH FROM date_generated) <= 8 THEN 2
                                  ELSE 3
        END                              AS trimestre,
        
       TO_CHAR(date_generated, 'Month')                                              AS month_name,
       EXTRACT(WEEK FROM date_generated)                                             AS week_of_year,
       EXTRACT(DAY FROM date_generated)                                              AS day_of_month,
       EXTRACT(DOW FROM date_generated)                                              AS day_of_week,
       TO_CHAR(date_generated, 'Day')                                                AS day_name,
       CASE WHEN EXTRACT(DOW FROM date_generated) IN (6, 0) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series('2023-01-01'::date, '2030-12-31', '1 day'::interval) date_generated