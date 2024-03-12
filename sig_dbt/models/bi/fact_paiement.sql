WITH fact_paiement AS (SELECT p.datepaiement,
                              p.gym_id,
                              p.id_forfait,

                              COALESCE(SUM(p.total) FILTER ( WHERE p.type = 'REFUND' ), 0)          AS total_remboursement,
                              COALESCE(SUM(p.adherent_id) FILTER ( WHERE p.type = 'REFUND' ), 0)    AS adherent_remboursement,

                              COALESCE(SUM(p.total) FILTER ( WHERE p.type = 'SESSION' ), 0)         AS total_session,
                              COALESCE(COUNT(p.adherent_id) FILTER ( WHERE p.type = 'SESSION' ), 0) AS adherent_session,

                              COALESCE(SUM(p.total_real) FILTER ( WHERE p.type = 'FORFAIT'), 0)     AS total_forfait,
                              COALESCE(COUNT(p.adherent_id) FILTER ( WHERE p.type = 'FORFAIT'), 0)  AS adherent_forfait,


                              COALESCE(SUM(p.total_real) FILTER ( WHERE p.type = 'FORFAIT' AND p.moisforfait = 1 ),
                                       0)                                                           AS total_forfait_1mois,
                              COALESCE(COUNT(p.adherent_id) FILTER ( WHERE p.type = 'FORFAIT' AND p.moisforfait = 1 ),
                                       0)                                                           AS adherent_forfait_1mois,


                              COALESCE(SUM(p.total_real) FILTER ( WHERE p.type = 'FORFAIT' AND p.moisforfait = 3 ),
                                       0)                                                           AS total_forfait_3mois,
                              COALESCE(COUNT(p.adherent_id) FILTER ( WHERE p.type = 'FORFAIT' AND p.moisforfait = 3 ),
                                       0)                                                           AS adherent_forfait_3mois,

                              COALESCE(SUM(p.total_real) FILTER ( WHERE p.type = 'FORFAIT' AND p.moisforfait = 6 ),
                                       0)                                                           AS total_forfait_6mois,
                              COALESCE(COUNT(p.adherent_id) FILTER ( WHERE p.type = 'FORFAIT' AND p.moisforfait = 6 ),
                                       0)                                                           AS adherent_forfait_6mois,

                              COALESCE(SUM(p.total_real) FILTER ( WHERE p.type = 'FORFAIT' AND p.moisforfait = 12 ),
                                       0)                                                           AS total_forfait_12mois,
                              COALESCE(COUNT(p.adherent_id) FILTER ( WHERE p.type = 'FORFAIT' AND p.moisforfait = 12 ),
                                       0)                                                           AS adherent_forfait_12mois
                       FROM {{ref('dim_paiement')}} p
                       GROUP BY p.gym_id, p.datepaiement, p.id_forfait),
     fact_paiement2 AS (SELECT dd.date_generated,
                               g.id_gym,
                               COALESCE(f.id_forfait, g.id_gym*1000)                 AS id_forfait,
                               COALESCE(SUM(fp.total_remboursement), 0)  AS total_remboursement,
                               COALESCE(SUM(fp.total_session), 0)        AS total_session,
                               COALESCE(SUM(fp.total_forfait), 0)        AS total_forfait,
                               COALESCE(SUM(fp.total_forfait_1mois), 0)  AS total_forfait_1mois,
                               COALESCE(SUM(fp.total_forfait_3mois), 0)  AS total_forfait_3mois,
                               COALESCE(SUM(fp.total_forfait_6mois), 0)  AS total_forfait_6mois,
                               COALESCE(SUM(fp.total_forfait_12mois), 0) AS total_forfait_12mois,
                               COALESCE(SUM(fp.adherent_remboursement), 0)  AS adherent_remboursement,
                               COALESCE(SUM(fp.adherent_session), 0)        AS adherent_session,
                               COALESCE(SUM(fp.adherent_forfait), 0)        AS adherent_forfait,
                               COALESCE(SUM(fp.adherent_forfait_1mois), 0)  AS adherent_forfait_1mois,
                               COALESCE(SUM(fp.adherent_forfait_3mois), 0)  AS adherent_forfait_3mois,
                               COALESCE(SUM(fp.adherent_forfait_6mois), 0)  AS adherent_forfait_6mois,
                               COALESCE(SUM(fp.adherent_forfait_12mois), 0) AS adherent_forfait_12mois
                        FROM {{ref('dim_date')}} dd
                                 LEFT JOIN {{source('public', 'gym_')}} g ON TRUE
                                 LEFT JOIN {{source('public', 'forfait_')}} f ON f.gym_id = g.id_gym
                                 LEFT JOIN fact_paiement fp ON fp.gym_id = g.id_gym AND f.id_forfait = fp.id_forfait AND
                                                               dd.date_generated = fp.datepaiement
                        GROUP BY dd.date_generated, g.id_gym, ROLLUP ( f.id_forfait)
                        ORDER BY g.id_gym, dd.date_generated, COALESCE(f.id_forfait, 0))
SELECT date_generated,
       f.id_gym,
       f.id_forfait,
       g.libelle                     AS gym,
       ff.libelle AS forfait,
       total_remboursement,
       total_session,
       total_forfait,
       total_forfait_1mois,
       total_forfait_3mois,
       total_forfait_6mois,
       total_forfait_12mois,

       adherent_remboursement,
       adherent_session,
       adherent_forfait,
       adherent_forfait_1mois,
       adherent_forfait_3mois,
       adherent_forfait_6mois,
       adherent_forfait_12mois
FROM fact_paiement2 f
         LEFT JOIN {{source('public', 'gym_')}} g ON g.id_gym = f.id_gym
         LEFT JOIN {{ref('dim_forfait')}} ff ON f.id_forfait = ff.id_forfait