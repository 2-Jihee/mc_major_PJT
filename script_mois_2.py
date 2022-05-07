from data.mois_population import *
from data.mois_population_etc import *

# get_mois_birth(admin_div_num_list)
get_mois_death(admin_div_num_list)
get_mois_household_all(admin_div_num_list)
get_mois_household_resident(admin_div_num_list)

get_mois_birth(list(jr_admin_div_num_dict.keys()))
get_mois_death(list(jr_admin_div_num_dict.keys()))
get_mois_household_all(list(jr_admin_div_num_dict.keys()))
get_mois_household_resident(list(jr_admin_div_num_dict.keys()))