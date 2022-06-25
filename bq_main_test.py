"""
Shogo
BigQuery - main
"""

import BigQuery_upload as up
import BigQuery_process as pro
import pandas as pd
import numpy as np
from collections import defaultdict


#df_mediabase = pd.read_csv(r"C:\Users\sh_uchida\Desktop\tech_centre\report_variable\All_variable1.csv")
#df_titlebase = pd.read_csv(r"C:\Users\sh_uchida\Desktop\tech_centre\report_variable\All_variable2.csv")

# for debug
df_mediabase = pd.read_csv(r"C:\Users\sh_uchida\Desktop\tech_centre\report_variable\All_variable1_debug.csv")
df_titlebase = pd.read_csv(r"C:\Users\sh_uchida\Desktop\tech_centre\report_variable\All_variable2_debug.csv")

SDK = df_mediabase.SDK
isactive = df_mediabase.active
title_list = df_mediabase.title
media_name = df_mediabase.media_name
media_cpi_list = df_mediabase.mediaCPI
total_number = title_list.size

title_SDK = df_titlebase.SDK
title_isactive = df_titlebase.active
title_title_list = df_titlebase.title
title_event1 = df_titlebase.ad_event1
title_event2 = df_titlebase.ad_event2
title_event3 = df_titlebase.ad_event3
title_event4 = df_titlebase.ad_event4
title_event5 = df_titlebase.ad_event5
url_list = df_titlebase.dashboard
cpi_list = df_titlebase.CPI
client_d1rr_KPI = df_titlebase.Client_d1rr_KPI
budget_list = df_titlebase.Budget
title_total_number = title_list.size


af_media_list = defaultdict(list)
ad_media_list = defaultdict(list)
af_list = defaultdict(list)
ad_list = defaultdict(list)
AD_event_list = defaultdict(list)


for i in range(total_number):
    if isactive[i] == "active":
        if SDK[i] == "Appsflyer":
            af_media_list[title_list[i]].append(media_name[i])
        elif SDK[i] == "Adjust":
            ad_media_list[title_list[i]].append(media_name[i])

for j in range(title_total_number):
    if title_isactive[j] == "active":
        if title_SDK[j] == "Appsflyer":
            af_list[title_title_list[j]].append(cpi_list[j])
            af_list[title_title_list[j]].append(client_d1rr_KPI[j])
            af_list[title_title_list[j]].append(budget_list[j])

        elif title_SDK[j] == "Adjust":
            ad_list[title_title_list[j]].append(cpi_list[j])
            ad_list[title_title_list[j]].append(client_d1rr_KPI[j])
            ad_list[title_title_list[j]].append(budget_list[j])
            AD_event_list[title_title_list[j]].append(title_event1[j])
            AD_event_list[title_title_list[j]].append(title_event2[j])
            AD_event_list[title_title_list[j]].append(title_event3[j])
            AD_event_list[title_title_list[j]].append(title_event4[j])
            AD_event_list[title_title_list[j]].append(title_event5[j])
            AD_event_list[title_title_list[j]] = [i for i in AD_event_list[title_title_list[j]] if i != "none"]


print(ad_media_list)
print(af_media_list)
print(ad_list)
print(af_list)
print(AD_event_list)



# --------------------------- execution ---------------------------#
location = r"C:\Users\sh_uchida\Desktop\Perry_VDB\AFFISE\Past\September.csv"
#destination = r"C:\Users\sh_uchida\Desktop\Perry_VDB\Adjust\Past\harrypotter_ios_jp\Cohort\feb_processed.csv"
#dest_folder = r"C:\Users\sh_uchida\Desktop\Perry_VDB\Adjust\Past\battlecats_ios_us\Deliverable\jan_processed_"
#ad_destination = dest_folder + "Deliverable-adgroup-event.csv"
#ad_event_destination = dest_folder + "Deliverable-adgroup.csv"
#cpn_destination = dest_folder + "Deliverable-campaign.csv"
#cpn_event_destination = dest_folder + "Deliverable-campaign-event.csv"

######################### Adjust cohort data past data in #############################
################## careful for adgroup and creative w pub and siteID~!
#pro.cohort(location, destination, 4, 0, 1000, "harrypotter_ios_jp")
#up.one_upload_files(destination, r"Adjust", "Cohort")

######################### Adjust deliverable past data in #############################
#pro.deliverable(location, cpn_destination, ad_destination, cpn_event_destination, ad_event_destination, 3, 0.2, 15000, "battlecats_ios_us")
#up.one_upload_files(ad_destination, r"Adjust", "Deliverable-adgroup")
#up.one_upload_files(ad_event_destination, r"Adjust", "Deliverable-adgroup-event")
#up.one_upload_files(cpn_destination, r"Adjust", "Deliverable-campaign")
#up.one_upload_files(cpn_event_destination, r"Adjust", "Deliverable-campaign-event")


######################## APPSFLYER install event past data in ##########################
#pro.install(location, destination, 350, 0, 3000000, "qoo10_ios_jp")
#pro.event(location, destination, 350, 0, 3000000, "qoo10_ios_jp")
#up.one_upload_files(destination, r"Appsflyer", "Events")


################### WHEN adding July and August PLEASE DO BELOW ##########
############ 1, copy sub2 to sub 6
############ change YYYY-MM-DD HH:MM in click and install time both! #############
up.one_upload_files(location, r"Affise", r"CVreport_raw")



