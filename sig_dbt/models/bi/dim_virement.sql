select v.id_virement_salaire,
       e.id_employe,
       concat(e.nom, ' ', e.prenom) as employe,
       v.montant,
       v.modereglement,
       v.moisapayer,
       v.datevirement,
       v.gym_id
from {{source('public', 'virement_salaire_')}} v
         left join {{source('public', 'employe_')}} e ON e.id_employe = v.employe_id