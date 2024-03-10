select d.id_depense,
       d.libelle,
       d.montant,
       d.datedepense,
       d.gym_id
from {{source('public', 'depense_')}} d