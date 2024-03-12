select pc.id_paiement_coaching,
       pc.coach_id,
       pc.gym_id,
       concat(c.nom, ' ', c.prenom) as coach,
       pc.prixcoaching,
       pc.moiscoaching,
       pc.datedebut,
       pc.datefin,
       pc.createdon::DATE
from {{source('public', 'paiement_coaching_')}} pc
         left join {{source('public', 'employe_')}} c ON c.id_employe = pc.coach_id