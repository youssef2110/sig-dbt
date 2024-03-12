select p.id_paiement,
       a.id_abonnement,
       a.adherent_id,
       f.id_forfait,
       f.libelle       as forfait,
       CASE
           WHEN p.refunded_paiement_id is not null THEN 'REFUND'
           WHEN ps.id_paiement_session is not null THEN 'SESSION'
           WHEN dpf.id_paiement_forfait is not null THEN 'FORFAIT'
           ELSE 'OTHER' END
                       as type,
       p.status,
       p.modereglement,

       -- session
       ps.total        as total_session,
       ps.nombresessions,

       -- forfait
       dpf.prixforfait as total_forfait,
       dpf.total_frais,
       dpf.moisforfait,
       dpf.datedebut,
       dpf.datefin,

       -- paiement
       p.total,
       p.resteapayer,
       p.prixremise,
       CASE
           WHEN p.status = 'PAID_INCOMPLETE' THEN p.total - p.resteapayer
           ELSE p.total END AS total_real,

       p.datepaiement,
       p.datenextpaiement,

       p.gym_id


from {{source('public', 'paiement_')}} p
         left join {{source('public', 'abonnement_')}} a ON a.id_abonnement = p.abonnement_id
         left join {{source('public', 'forfait_')}} f ON f.id_forfait = a.forfait_id
         left join {{source('public', 'paiement_session_')}} ps ON ps.paiement_id = p.id_paiement
         left join {{ref('dim_paiement_forfait')}} dpf ON dpf.paiement_id = p.id_paiement

where p.status != 'CANCELLED'