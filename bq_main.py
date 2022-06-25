"""
Shogo
BigQuery - uploading the data in regular base
"""

import BigQuery_upload as up
import BigQuery_process as pro
import pandas as pd
import numpy as np
from collections import defaultdict
import glob
import os
import logging
from datetime import datetime, timedelta
todaynow = datetime.now()
import gspread
from oauth2client.service_account import ServiceAccountCredentials
logging.basicConfig(filename=r'C:\Users\sh_uchida\Desktop\tech_centre/logger.log', level=logging.DEBUG)

logging.debug("--------------------------------- upload bq -------------------------------")
logging.debug(todaynow)


# GoogleSpreadSheet connect
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\sh_uchida\Desktop\tech_centre\product-perry-2d6c73e8c579.json', scope)
gc = gspread.authorize(credentials)

SPREADSHEET_KEY = '1mBIRlljypZH736jd6x6NJWesm3ySRQwDjYGVOJkdVuw'
worksheet_media = gc.open_by_key(SPREADSHEET_KEY).worksheet('media_base')
worksheet_title = gc.open_by_key(SPREADSHEET_KEY).worksheet('client_base')

#worksheet_media = gc.open_by_key(SPREADSHEET_KEY).worksheet('media_base_debug')
#worksheet_title = gc.open_by_key(SPREADSHEET_KEY).worksheet('client_base_debug')


SDK = worksheet_media.col_values(1)
isactive = worksheet_media.col_values(2)
title_list = worksheet_media.col_values(3)
media_name = worksheet_media.col_values(4)
media_cpi_list = worksheet_media.col_values(6)
total_number = len(title_list)

title_SDK = worksheet_title.col_values(1)
title_isactive = worksheet_title.col_values(2)
title_title_list = worksheet_title.col_values(3)
title_event1 = worksheet_title.col_values(5)
title_event2 = worksheet_title.col_values(6)
title_event3 = worksheet_title.col_values(7)
title_event4 = worksheet_title.col_values(8)
title_event5 = worksheet_title.col_values(9)
url_list = worksheet_title.col_values(10)
cpi_list = worksheet_title.col_values(11)
client_d1rr_KPI = worksheet_title.col_values(12)
budget_list = worksheet_title.col_values(13)
title_total_number = len(title_title_list)






af_media_list = defaultdict(list)
ad_media_list = defaultdict(list)
af_list = defaultdict(list)
ad_list = defaultdict(list)
AD_event_list = defaultdict(list)
AF_event_list = {}

print(title_total_number)
print(total_number)

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




# ---------------------------- adjust --------------------------------#
def deliverable_upload():
    pro.ad_bq_process_deliverable(ad_list)
    logging.debug('-----------ad deliverable moving file done------------')
    up.upload_files_deliverable(ad_list)
    logging.debug('-----------ad deliverable uploading file done------------')

def cohort_upload():
    pro.ad_bq_process_cohort(ad_list)
    logging.debug('-----------ad cohort moving file done------------')
    up.upload_files_cohort(ad_list)
    logging.debug('-----------ad cohort uploading file done------------')

# ---------------------------- appsflyer --------------------------------#
def install_event_upload():
    pro.af_bq_process_installevent(af_list)
    logging.debug('-----------af install event moving file done------------')
    up.upload_files_installevent(af_list)
    logging.debug('-----------af uploading install event file done------------')

def retention_upload():
    pro.af_bq_process_retention(af_list)
    logging.debug('-----------af retention moving file done------------')
    up.upload_files_retention(af_list)
    logging.debug('-----------af uploading retention file done------------')


# appsflyer
def appsflyer():
    try:
        install_event_upload()
    except:
        logging.debug('-----------af uploading Install Event appsflyer file ERROR ------------')
    try:
        retention_upload()
    except:
        logging.debug('-----------af uploading appsflyer file ERROR ------------')
    logging.debug('-----------af uploading appsflyer file all done------------')


# adjust
def adjust():
    try:
        deliverable_upload()
    except:
        logging.debug('-----------af uploading Deliverable adjust file ERROR ------------')
    try:
        cohort_upload()
    except:
        logging.debug('-----------af uploading Cohort adjust file ERROR ------------')
    logging.debug('-----------af uploading adjust file all done------------')


def affise():
    try:
        pro.affise_upload()
    except:
        logging.debug('-----------af uploading Affise file ERROR------------')
    logging.debug('-----------af uploading Affise file All done------------')


def survey():
    try:
        pro.survey_upload()
    except:
        logging.debug('-----------af uploading 24 metrics file ERROR-----------')
    logging.debug('-----------af uploading 24 metrics file all done------------')


#--------------------------------------------- main --------------------------------------------------------#
def main():
    #appsflyer()
    #adjust()
    affise()
    #survey()

main()
logging.debug("--------------------------------- upload bq all done -------------------------------")






# ----------------------- set the variable from csv -------------------------------#

#df_mediabase = pd.read_csv(r"C:\Users\sh_uchida\Desktop\tech_centre\report_variable\All_variable1.csv")
#df_titlebase = pd.read_csv(r"C:\Users\sh_uchida\Desktop\tech_centre\report_variable\All_variable2.csv")


# for debug
#df_mediabase = pd.read_csv(r"C:\Users\sh_uchida\Desktop\tech_centre\report_variable\All_variable1_debug.csv")
#df_titlebase = pd.read_csv(r"C:\Users\sh_uchida\Desktop\tech_centre\report_variable\All_variable2_debug.csv")

#SDK = df_mediabase.SDK
#isactive = df_mediabase.active
#title_list = df_mediabase.title
#media_name = df_mediabase.media_name
#media_cpi_list = df_mediabase.mediaCPI
#total_number = title_list.size

#title_SDK = df_titlebase.SDK
#title_isactive = df_titlebase.active
#title_title_list = df_titlebase.title
#title_event1 = df_titlebase.ad_event1
#title_event2 = df_titlebase.ad_event2
#title_event3 = df_titlebase.ad_event3
#title_event4 = df_titlebase.ad_event4
#title_event5 = df_titlebase.ad_event5
#url_list = df_titlebase.dashboard
#cpi_list = df_titlebase.CPI
#client_d1rr_KPI = df_titlebase.Client_d1rr_KPI
#budget_list = df_titlebase.Budget
#title_total_number = title_title_list.size

