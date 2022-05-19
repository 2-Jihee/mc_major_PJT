from data.mois import *

get_mois_population_all(admin_div_code_list)
get_mois_population_resident(admin_div_code_list)
get_mois_population_unknown(admin_div_code_list)
get_mois_population_overseas(admin_div_code_list)

get_mois_birth(admin_div_code_list)
get_mois_death(admin_div_code_list)
get_mois_household_all(admin_div_code_list)
get_mois_household_resident(admin_div_code_list)

get_mois_population_all(list(jr_admin_div_code_dict.keys()))
get_mois_population_resident(list(jr_admin_div_code_dict.keys()))
get_mois_population_unknown(list(jr_admin_div_code_dict.keys()))
get_mois_population_overseas(list(jr_admin_div_code_dict.keys()))

get_mois_population_all(['0000000000'], True)
get_mois_population_resident(['0000000000'], True)
get_mois_population_unknown(['0000000000'], True)
get_mois_population_overseas(['0000000000'], True)
get_mois_birth(['0000000000'], True)
get_mois_death(['0000000000'], True)
get_mois_household_all(['0000000000'], True)
get_mois_household_resident(['0000000000'], True)

upload_mois_data(['0000000000'], 'B', None, True)
upload_mois_data(['0000000000'], 'D', None, True)

upload_mois_data(['0000000000'], 'P', '-', True)
upload_mois_data(['0000000000'], 'P', 'R', True)
upload_mois_data(['0000000000'], 'P', 'O', True)
upload_mois_data(['0000000000'], 'P', 'U', True)

upload_mois_data(['0000000000'], 'P', 'R')
upload_mois_data(['0000000000'], 'P', '-')
upload_mois_data(['0000000000'], 'P', 'O')
upload_mois_data(['0000000000'], 'P', 'U')

upload_mois_data(['0000000000'], 'H', '-', True)
upload_mois_data(['0000000000'], 'H', 'R', True)