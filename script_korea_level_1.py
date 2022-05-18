from pop.load import *

admin_div_codes = ['00', '11', '26', '27', '28', '29', '30', '31', '36', '41', '42', '43', '44', '45', '46', '47', '48', '50']

pyramid_boy = {}
for admin_div_code in admin_div_codes:
    pyramid_boy[admin_div_code] = {}
    for year in range(2013, 2022):
        pyramid_boy[admin_div_code][year] = get_mois_pyramid(admin_div_code, year-1, month=12)

outflow_pyramid = {}
for admin_div_code in admin_div_codes:
    if admin_div_code == '00':
        continue
    outflow_pyramid[admin_div_code] = {}
    for into_admin_div_code in admin_div_codes:
        if into_admin_div_code in ['00', admin_div_code]:
            continue
        outflow_pyramid[admin_div_code][into_admin_div_code] = {}
        for year in range(2013, 2022):
            outflow_pyramid[admin_div_code][into_admin_div_code][year] = get_kosis_outflow_matrix(admin_div_code, into_admin_div_code, year)

outflow_rate_pyramid = {}
for admin_div_code in admin_div_codes:
    if admin_div_code == '00':
        continue
    outflow_rate_pyramid[admin_div_code] = {}
    for into_admin_div_code in admin_div_codes:
        if into_admin_div_code in ['00', admin_div_code]:
            continue
        outflow_rate_pyramid[admin_div_code][into_admin_div_code] = {}
        for year in range(2013, 2022):
            outflow_rate_pyramid[admin_div_code][into_admin_div_code][year] = pyramid_boy[admin_div_code][year].calc_rate(outflow_pyramid[admin_div_code][into_admin_div_code][year])

flow_pyramid = {}
for admin_div_code in admin_div_codes:
    if admin_div_code == '00':
        continue
    flow_pyramid[admin_div_code] = {}
    for year in range(2013, 2022):
        flow_pyramid[admin_div_code][year] = {}

        outflow = Pyramid(last_age=last_age)
        outflow.fill_zeros()
        for into_admin_div_code in outflow_pyramid[admin_div_code].keys():
            outflow.add_pyramid(outflow_pyramid[admin_div_code][into_admin_div_code][year])
        flow_pyramid[admin_div_code][year]['outflow'] = outflow

        inflow = Pyramid(last_age=last_age)
        inflow.fill_zeros()
        for from_admin_div_code in outflow_pyramid.keys():
            if from_admin_div_code == admin_div_code:
                continue
            inflow.add_pyramid(outflow_pyramid[from_admin_div_code][admin_div_code][year])
        flow_pyramid[admin_div_code][year]['inflow'] = inflow

        netflow = Pyramid(last_age=last_age, is_pos_only=False)
        netflow.fill_zeros()
        netflow.add_pyramid(inflow)
        netflow.subtract_pyramid(outflow)
        flow_pyramid[admin_div_code][year]['netflow'] = netflow

pyramid_post_move = {}
for admin_div_code in admin_div_codes:
    pyramid_post_move[admin_div_code] = {}
    for year in range(2013, 2022):
        pyramid_post_move[admin_div_code][year] = pyramid_boy[admin_div_code][year].copy().add_pyramid(flow_pyramid[admin_div_code][year]['netflow'])

death_pyramid = {}
for admin_div_code in admin_div_codes:
    death_pyramid[admin_div_code] = {}
    for year in range(2013, 2021):
        death_pyramid[admin_div_code][year] = get_kosis_death_pyramid(admin_div_code, year)

death_rate_pyramid = {}
for admin_div_code in admin_div_codes:
    death_rate_pyramid[admin_div_code] = {}
    for year in range(2013, 2021):
        death_rate_pyramid[admin_div_code][year] = pyramid_post_move[admin_div_code][year].calc_rate(death_pyramid[admin_div_code][year])

pyramid_post_death = {}
for admin_div_code in admin_div_codes:
    pyramid_post_death[admin_div_code] = {}
    for year in range(2013, 2022):
        pyramid_post_death[admin_div_code][year] = pyramid_post_move[admin_div_code][year].copy().subtract_pyramid(death_pyramid[admin_div_code][year])

# death_rate_pyramid = {}
# for admin_div_code in admin_div_codes:
#     death_rate_pyramid[admin_div_code] = {}
#     for year in death_pyramid[admin_div_code].keys():
#         death_rate_pyramid[admin_div_code][year] = pyramid_avg[admin_div_code][year].calc_rate(death_pyramid[admin_div_code][year])






    # seoul_2019 = get_mois_pyramid('11', 2019)
# seoul_2020 = get_mois_pyramid('11', 2020)
# seoul_2020_mid = avg_pyramids(seoul_2019, seoul_2020)
#
# seoul_2020_death = get_kosis_death_pyramid('11', 2020)
# seoul_2020_death_rate = seoul_2020_mid.calc_rate(seoul_2020_death)
#
# seoul_2020_outflow = get_kosis_outflow_pyramid('11', 2020)
# seoul_2020_outflow_rate = seoul_2020_mid.calc_rate(seoul_2020_outflow)
#
#
# korea_2019 = get_mois_pyramid('00', 2019)
# korea_2020 = get_mois_pyramid('00', 2020)
# ex_seoul_2019 = calc_pyramid(korea_2019, '-', seoul_2019, True, True)
# ex_seoul_2020 = calc_pyramid(korea_2020, '-', seoul_2020, True, True)
# ex_seoul_2020_mid = avg_pyramids(ex_seoul_2019, ex_seoul_2020)
#
# seoul_2020_inflow = get_kosis_inflow_pyramid('11', 2020)
# seoul_2020_inflow_rate = ex_seoul_2020_mid.calc_rate(seoul_2020_inflow)
