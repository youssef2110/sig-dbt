with fact_paiement as (select date_generated,
                              id_gym,
                              total_remboursement,
                              total_session,
                              total_forfait,
                              total_forfait_3mois,
                              total_forfait_6mois,
                              total_forfait_12mois
                       from {{ref('fact_paiement')}} where forfait = 'TOTAL'),
     fact_coaching as (select dd.date_generated, dc.gym_id, coalesce(sum(dc.prixcoaching), 0) as total_coaching
                       from {{ref('dim_date')}} dd
                                left join {{ref('dim_coaching')}} dc ON dc.createdon = dd.date_generated
                       group by dd.date_generated, dc.gym_id
                       )
select dd.date_generated,
       g.id_gym,
       f.total_remboursement,
       f.total_session,
       f.total_forfait,
       coalesce(c.total_coaching, 0) as total_coaching
from {{ref('dim_date')}} dd
         LEFT JOIN gym_ g ON TRUE
         LEFT JOIN fact_paiement f ON f.date_generated = dd.date_generated and g.id_gym = f.id_gym
         left join fact_coaching c ON c.date_generated = dd.date_generated and g.id_gym = c.gym_id