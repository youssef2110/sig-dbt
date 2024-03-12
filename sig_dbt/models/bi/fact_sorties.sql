with dim_virement2 as (select sum(v.montant) as montant,
                              v.moisapayer,
                              v.gym_id
                       from {{ref('dim_virement')}} v
                       group by v.moisapayer, v.gym_id),
    dim_depense2 as (select sum(d.montant) as montant,
                              d.datedepense,
                              d.gym_id
                       from {{ref('dim_depense')}} d
                       group by d.datedepense, d.gym_id)
select dd.date_generated,
       g.id_gym,
       coalesce(sum(v.montant), 0)                  as salaires,
       coalesce(sum(d.montant), 0)                  as depenses,
       coalesce(sum(d.montant), 0)  +  coalesce(sum(v.montant), 0) as total
from {{ref('dim_date')}} dd
         left join {{source('public', 'gym_')}} g ON TRUE
         left join dim_virement2 v ON v.moisapayer = dd.date_generated and v.gym_id = g.id_gym
         left join dim_depense2 d ON d.datedepense = dd.date_generated and d.gym_id = g.id_gym
group by dd.date_generated, g.id_gym