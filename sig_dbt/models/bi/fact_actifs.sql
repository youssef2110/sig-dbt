WITH fact_abonnements AS (SELECT a.gym_id,
                                 COALESCE(a.forfait_id, a.gym_id*1000)                                                     AS id_forfait,
                                 COUNT(id_abonnement) FILTER ( WHERE status = 'ACTIVE')                        AS actifs,
                                 COUNT(id_abonnement)
                                 FILTER ( WHERE status = 'EXPIRED' AND NOW() - expiresat < '1month'::interval) AS expired_1mois
                          FROM {{source('public', 'abonnement_')}} a
                          GROUP BY a.gym_id, ROLLUP (a.forfait_id)
                          ORDER BY a.gym_id, COALESCE(a.forfait_id, 0))
SELECT a.gym_id, a.id_forfait, f.libelle AS forfait, actifs, expired_1mois
FROM fact_abonnements a
         LEFT JOIN {{ref('dim_forfait')}} f ON f.id_forfait = a.id_forfait