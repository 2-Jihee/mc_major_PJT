from pop.load import *

admin_div_codes = ['00', '11', '26', '27', '28', '29', '30', '31', '36', '41', '42', '43', '44', '45', '46', '47', '48', '50']

pop_conn = db_connect(pop_db)

# # past data

boy_pyramids = {}
for admin_div_code in admin_div_codes:
    boy_pyramids[admin_div_code] = {}
    for year in range(2013, 2023):
        boy_pyramids[admin_div_code][year] = get_mois_pyramid(admin_div_code, year - 1, month=12, pop_conn=pop_conn)

move_matrix_pyramids = {}
for from_admin_div_code in admin_div_codes:
    if from_admin_div_code == '00':
        continue
    move_matrix_pyramids[from_admin_div_code] = {}
    for to_admin_div_code in admin_div_codes:
        if to_admin_div_code in ['00', from_admin_div_code]:
            continue
        move_matrix_pyramids[from_admin_div_code][to_admin_div_code] = {}
        for year in range(2013, 2022):
            move_matrix_pyramids[from_admin_div_code][to_admin_div_code][year] = get_kosis_move_pyramid(from_admin_div_code, to_admin_div_code, year, pop_conn=pop_conn)

move_rate_matrix_pyramids = {}
for from_admin_div_code in admin_div_codes:
    if from_admin_div_code == '00':
        continue
    move_rate_matrix_pyramids[from_admin_div_code] = {}
    for to_admin_div_code in admin_div_codes:
        if to_admin_div_code in ['00', from_admin_div_code]:
            continue
        move_rate_matrix_pyramids[from_admin_div_code][to_admin_div_code] = {}
        for year in range(2013, 2022):
            move_rate_matrix_pyramids[from_admin_div_code][to_admin_div_code][year] = boy_pyramids[from_admin_div_code][year].calc_rate(move_matrix_pyramids[from_admin_div_code][to_admin_div_code][year])

flow_pyramids = {}
for admin_div_code in admin_div_codes:
    if admin_div_code == '00':
        continue
    flow_pyramids[admin_div_code] = {}
    for year in range(2013, 2022):
        flow_pyramids[admin_div_code][year] = {}

        temp_outflow_pyramid = Pyramid(last_age=LAST_AGE, fill_zeros=True)
        for to_admin_div_code in move_matrix_pyramids[admin_div_code].keys():
            temp_outflow_pyramid.add_pyramid(move_matrix_pyramids[admin_div_code][to_admin_div_code][year])
        flow_pyramids[admin_div_code][year]['outflow'] = temp_outflow_pyramid

        temp_inflow_pyramid = Pyramid(last_age=LAST_AGE, fill_zeros=True)
        for from_admin_div_code in move_matrix_pyramids.keys():
            if from_admin_div_code != admin_div_code:
                temp_inflow_pyramid.add_pyramid(move_matrix_pyramids[from_admin_div_code][admin_div_code][year])
        flow_pyramids[admin_div_code][year]['inflow'] = temp_inflow_pyramid

        temp_netflow_pyramid = Pyramid(last_age=LAST_AGE, is_pos_only=False, fill_zeros=True)
        temp_netflow_pyramid.add_pyramid(temp_inflow_pyramid)
        temp_netflow_pyramid.subtract_pyramid(temp_outflow_pyramid)
        flow_pyramids[admin_div_code][year]['netflow'] = temp_netflow_pyramid

post_move_pyramids = {}
for admin_div_code in admin_div_codes:
    post_move_pyramids[admin_div_code] = {}
    for year in range(2013, 2022):
        if admin_div_code == '00':
            post_move_pyramids[admin_div_code][year] = boy_pyramids[admin_div_code][year].copy()
        else:
            temp_post_move_pyramid = Pyramid(last_age=LAST_AGE, is_pos_only=False, fill_zeros=True)
            temp_post_move_pyramid.add_pyramid(boy_pyramids[admin_div_code][year])
            temp_post_move_pyramid.add_pyramid(flow_pyramids[admin_div_code][year]['netflow'])
            post_move_pyramids[admin_div_code][year] = temp_post_move_pyramid

death_pyramids = {}
for admin_div_code in admin_div_codes:
    death_pyramids[admin_div_code] = {}
    for year in range(2013, 2021):
        death_pyramids[admin_div_code][year] = get_kosis_death_pyramid(admin_div_code, year, pop_conn=pop_conn)

death_rate_pyramids = {}
for admin_div_code in admin_div_codes:
    death_rate_pyramids[admin_div_code] = {}
    for year in range(2013, 2021):
        death_rate_pyramids[admin_div_code][year] = post_move_pyramids[admin_div_code][year].calc_rate(death_pyramids[admin_div_code][year])

post_death_pyramids = {}
for admin_div_code in admin_div_codes:
    post_death_pyramids[admin_div_code] = {}
    for year in range(2013, 2021):
        temp_post_death_pyramid = post_move_pyramids[admin_div_code][year].copy()
        temp_post_death_pyramid.subtract_pyramid(death_pyramids[admin_div_code][year])
        post_death_pyramids[admin_div_code][year] = temp_post_death_pyramid

birth_data = {}
for admin_div_code in admin_div_codes:
    birth_data[admin_div_code] = {}
    for year in range(2013, 2021):
        temp_birth_dict = get_kosis_birth(admin_div_code, year, pop_conn=pop_conn)
        birth_data[admin_div_code][year] = temp_birth_dict

birth_mother_pyramids = {}
for admin_div_code in admin_div_codes:
    birth_mother_pyramids[admin_div_code] = {}
    for year in range(2013, 2021):
        birth_mother_pyramids[admin_div_code][year] = get_kosis_birth_mother_pyramid(admin_div_code, year, pop_conn=pop_conn)

birth_rate_mother_pyramids = {}
for admin_div_code in admin_div_codes:
    birth_rate_mother_pyramids[admin_div_code] = {}
    for year in range(2013, 2021):
        birth_rate_mother_pyramids[admin_div_code][year] = post_death_pyramids[admin_div_code][year].calc_rate_vs_female(birth_mother_pyramids[admin_div_code][year])

eoy_pyramids = {}
for admin_div_code in admin_div_codes:
    eoy_pyramids[admin_div_code] = {}
    for year in range(2013, 2021):
        temp_eoy_pyramid = post_death_pyramids[admin_div_code][year].copy()
        temp_eoy_pyramid.add_one_year_and_birth(age_0_total=birth_data[admin_div_code][year]['total'], age_0_male=birth_data[admin_div_code][year]['male'], age_0_female=birth_data[admin_div_code][year]['female'])
        eoy_pyramids[admin_div_code][year] = temp_eoy_pyramid

# # future assumptions
future_year = 2050
for from_admin_div_code in admin_div_codes:
    if from_admin_div_code == '00':
        continue
    for to_admin_div_code in admin_div_codes:
        if to_admin_div_code in ['00', from_admin_div_code]:
            continue
        for year in range(2022, future_year+1):
            # 최신 값을 미래에 적용
            temp_move_rate_matrix_pyramid = move_rate_matrix_pyramids[from_admin_div_code][to_admin_div_code][2021].copy()
            move_rate_matrix_pyramids[from_admin_div_code][to_admin_div_code][year] = temp_move_rate_matrix_pyramid

for admin_div_code in admin_div_codes:
    for year in range(2021, future_year+1):
        # 최신 값을 미래에 적용
        temp_death_rate_pyramid = death_rate_pyramids[admin_div_code][2020].copy()
        death_rate_pyramids[admin_div_code][year] = temp_death_rate_pyramid

for admin_div_code in admin_div_codes:
    for year in range(2021, future_year+1):
        # 최신 값을 미래에 적용
        temp_birth_rate_mother_pyramid = birth_rate_mother_pyramids[admin_div_code][2020].copy()
        birth_rate_mother_pyramids[admin_div_code][year] = temp_birth_rate_mother_pyramid

# # run projections
for year in range(2022, future_year + 1):
    # generate move_matrix_pyramids for year
    for from_admin_div_code in admin_div_codes:
        if from_admin_div_code == '00':
            continue
        for to_admin_div_code in admin_div_codes:
            if to_admin_div_code in ['00', from_admin_div_code]:
                continue
            move_matrix_pyramids[from_admin_div_code][to_admin_div_code][year] = boy_pyramids[from_admin_div_code][year].multiply_rate(move_rate_matrix_pyramids[from_admin_div_code][to_admin_div_code][year])
    # generate flow_pyramids for year
    for admin_div_code in admin_div_codes:
        if admin_div_code == '00':
            continue
        flow_pyramids[admin_div_code][year] = {}

        temp_outflow_pyramid = Pyramid(last_age=LAST_AGE, fill_zeros=True)
        for to_admin_div_code in move_matrix_pyramids[admin_div_code].keys():
            temp_outflow_pyramid.add_pyramid(move_matrix_pyramids[admin_div_code][to_admin_div_code][year])
        flow_pyramids[admin_div_code][year]['outflow'] = temp_outflow_pyramid

        temp_inflow_pyramid = Pyramid(last_age=LAST_AGE, fill_zeros=True)
        for from_admin_div_code in move_matrix_pyramids.keys():
            if from_admin_div_code != admin_div_code:
                temp_inflow_pyramid.add_pyramid(move_matrix_pyramids[from_admin_div_code][admin_div_code][year])
        flow_pyramids[admin_div_code][year]['inflow'] = temp_inflow_pyramid

        temp_netflow_pyramid = Pyramid(last_age=LAST_AGE, is_pos_only=False, fill_zeros=True)
        temp_netflow_pyramid.add_pyramid(temp_inflow_pyramid)
        temp_netflow_pyramid.subtract_pyramid(temp_outflow_pyramid)
        flow_pyramids[admin_div_code][year]['netflow'] = temp_netflow_pyramid
    # generate post_move_pyramids for year
    for admin_div_code in admin_div_codes:
        if admin_div_code == '00':
            post_move_pyramids[admin_div_code][year] = boy_pyramids[admin_div_code][year].copy()
        else:
            temp_post_move_pyramid = Pyramid(last_age=LAST_AGE, is_pos_only=False, fill_zeros=True)
            temp_post_move_pyramid.add_pyramid(boy_pyramids[admin_div_code][year])
            temp_post_move_pyramid.add_pyramid(flow_pyramids[admin_div_code][year]['netflow'])
            post_move_pyramids[admin_div_code][year] = temp_post_move_pyramid
    # generate death_pyramids for year
    for admin_div_code in admin_div_codes:
        death_pyramids[admin_div_code][year] = post_move_pyramids[admin_div_code][year].multiply_rate(death_rate_pyramids[admin_div_code][year])
    # generate post_death_pyramids for year
    for admin_div_code in admin_div_codes:
        temp_post_death_pyramid = post_move_pyramids[admin_div_code][year].copy()
        temp_post_death_pyramid.subtract_pyramid(death_pyramids[admin_div_code][year])
        post_death_pyramids[admin_div_code][year] = temp_post_death_pyramid
    # generate birth_pyramids for year
    for admin_div_code in admin_div_codes:
        birth_mother_pyramids[admin_div_code][year] = post_death_pyramids[admin_div_code][year].multiply_rate_on_female(birth_rate_mother_pyramids[admin_div_code][year])
    # generate eoy_pyramids for year
    for admin_div_code in admin_div_codes:
        temp_eoy_pyramid = post_death_pyramids[admin_div_code][year].copy()
        temp_eoy_pyramid.add_one_year_and_birth(age_0_male=birth_mother_pyramids[admin_div_code][year].get_male(), age_0_female=birth_mother_pyramids[admin_div_code][year].get_female())
        eoy_pyramids[admin_div_code][year] = temp_eoy_pyramid
    # copy boy_pyramids for next year
    for admin_div_code in admin_div_codes:
        boy_pyramids[admin_div_code][year+1] = eoy_pyramids[admin_div_code][year].copy()
