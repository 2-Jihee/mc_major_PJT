from db.mois_population import *
from db.mois_population_etc import *

get_mois_birth(['4600000000'], 2017, 3)
get_mois_birth(['4700000000', '4800000000', '5000000000'])
get_mois_death(admin_div_num_list)
get_mois_household_all(admin_div_num_list)
get_mois_household_resident(admin_div_num_list)

get_mois_birth(list(jr_admin_div_num_dict.keys()))
get_mois_death(list(jr_admin_div_num_dict.keys()))
get_mois_household_all(list(jr_admin_div_num_dict.keys()))
get_mois_household_resident(list(jr_admin_div_num_dict.keys()))