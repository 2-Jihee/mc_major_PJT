from db.mois_population import *
from db.mois_population_etc import *

get_mois_population_resident(['3000000000'], 2017, 1)
get_mois_population_resident(['3100000000', '3600000000', '4100000000', '4200000000', '4300000000', '4400000000', '4500000000', '4600000000', '4700000000', '4800000000', '5000000000'])
get_mois_population_resident(list(jr_admin_div_num_dict.keys()))