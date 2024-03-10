select pf.id_paiement_forfait,
        pf.paiement_id,
        pf.forfait_id,
        pf.moisforfait,
        pf.datedebut,
        pf.datefin,
        pf.createdon::DATE,
        pf.prixforfait,
       coalesce(sum(pff.prixfrais), 0) as total_frais
from {{source('public', 'paiement_forfait_')}} pf
         left join {{source('public', 'paiement_frais_')}} pff ON pff.paiement_id = pf.paiement_id
group by pf.id_paiement_forfait