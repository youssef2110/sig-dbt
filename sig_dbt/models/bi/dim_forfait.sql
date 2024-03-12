-- dim_forfait
SELECT f.id_forfait, f.libelle, f.code, f.gym_id
FROM forfait_ f
UNION ALL
SELECT g.id_gym*1000, 'TOTAL' as libelle, 'TOTAL' as code, g.id_gym as gym_id  FROM gym_ g