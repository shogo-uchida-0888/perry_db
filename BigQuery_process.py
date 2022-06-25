"""
Shogo
BigQuery - data process
"""
import BigQuery_upload as up
from datetime import datetime, timedelta, date
from time import time
import glob
import pandas as pd
import numpy as np
import os
import logging
logging.basicConfig(filename=r'C:\Users\sh_uchida\Desktop\tech_centre/logger.log', level=logging.DEBUG)
today = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
yesterday = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(1)

global today_ymd
today_ymd = today.strftime('%y%m%d')
print(today)
print(yesterday)



# ---------------------------- AFFISE --------------------------------#
def affise_upload():
    oritinal_location = r"C:\Users\sh_uchida\Desktop\SDKreport\RPAfolder\AFFISE\All\cv_report" + today_ymd + ".csv"
    print(oritinal_location)
    location = r"C:\Users\sh_uchida\Desktop\DB\files\Affise\affise_bq" + today_ymd + ".csv"
    report = pd.read_csv(oritinal_location)
    report["Created at"] = pd.to_datetime(report["Created at"], format="%Y-%m-%d %H:%M:%S")
    report = report[(report["Created at"] >= yesterday) & (report["Created at"] < today)]
    report.to_csv(location, index=False)
    up.one_upload_files(location, r"Affise", r"CVreport_raw")

# ---------------------------- 24 metrics --------------------------------#
def survey_upload():
    folder = r"C:\Users\sh_uchida\Desktop\SDKreport\RPAfolder\Survey"
    filepath = max(glob.glob(os.path.join(folder, "*.csv")), key=os.path.getctime)
    print(filepath)
    location = r"C:\Users\sh_uchida\Desktop\DB\files\24metrics\24metrics_bq" + today_ymd + ".csv"
    report = pd.read_csv(filepath)
    report["request_date"] = pd.to_datetime(report["request_date"], format="%Y-%m-%d %H:%M:%S")
    print(report.shape)
    report = report[report["product"] != "N/A"]
    print(report.shape)
    report = report.dropna(subset=['product'])
    print(report.shape)
    report["product"] = report["product"].astype(int)
    report["affiliate"] = report["affiliate"].astype(int)
    report = report[(report["request_date"] >= yesterday) & (report["request_date"] < today)]
    report.to_csv(location, index=False)
    up.one_upload_files(location, r"24metrics", r"CVreport_raw")



# ---------------------------- adjust --------------------------------#


def deliverable(delfolder, cpn_destination, ad_destination, cpn_event_destination, ad_event_destination, gross_cpi, kpi, budget, ad_name):
    print(max(glob.glob(os.path.join(delfolder,  "*.csv")), key=os.path.getctime))
    df_full = pd.read_csv(max(glob.glob(os.path.join(delfolder, "*.csv")), key=os.path.getctime), encoding='utf-8-sig')
    #df_full = pd.read_csv(delfolder)
    df_full = df_full[df_full['Campaign'].str.contains('Perry', na=False)]
    df_full = df_full[(df_full['OS Name'] == 'android') | (df_full['OS Name'] == 'ios')]
    df_full["Date"] = pd.to_datetime(df_full["Date"])
    df_full = df_full[(df_full["Date"] >= yesterday) & (df_full["Date"] < today)]
    max_len = len(df_full.columns)
    print(max_len)
    print(list(df_full))
    drop_list = ['Tracker', 'Network', 'Country', 'cohort_revenue', 'Conversion']
    for drop_column in drop_list:
        if drop_column in list(df_full):
            df_full = df_full.drop(columns=[drop_column])
    #if max_len > 12:
    #    df_full = df_full.iloc[:, np.r_[0, 3, 4, 5, 7, 8, 9, 11, 12:max_len]]
    #else:
    #    df_full = df_full.iloc[:, np.r_[0, 3, 4, 5, 7, 8, 9, 11]]
    print(list(df_full))
    df_campaign = df_full.drop(columns=['Adgroup'])
    df_campaign = df_campaign.groupby(['Date', 'Campaign', 'OS Name'], as_index=False).sum()
    headers = list(df_campaign)
    print(headers)
    event_list = []
    for j in range(int((max_len - 11)/2)-1, -1, -1):
        print(j)
        event_list.append(headers[j * 2 + 5])
        df_campaign = df_campaign.drop(df_campaign.columns[j * 2 + 6], axis=1)
    headers = list(df_campaign)
    print(headers)
    print(event_list)
    df_campaign["event_name"] = "original"
    df_campaign["event_count"] = "original"
    new_df_campaign = df_campaign
    new_df_campaign = new_df_campaign.drop(event_list, axis=1)

    for event_name in event_list:
        each_df = df_campaign
        each_df["event_name"] = event_name
        each_df["event_count"] = each_df[event_name]
        each_df = each_df.drop(event_list, axis=1)
        new_df_campaign = pd.concat([new_df_campaign, each_df])

    df_campaign = df_campaign[['Date', 'Campaign', 'OS Name', 'Clicks', 'Installs']]
    df_campaign["title"] = ad_name
    df_campaign["Installs"] = df_campaign["Installs"].astype(int)
    df_campaign["Clicks"] = df_campaign["Clicks"].astype(int)
    df_campaign.to_csv(cpn_destination, index=False, encoding='utf-8-sig')
    new_df_campaign = new_df_campaign[new_df_campaign["event_name"] != "original"]
    new_df_campaign = new_df_campaign.drop(['Clicks', 'Installs'], axis=1)
    new_df_campaign["event_count"] = new_df_campaign["event_count"].astype(int)
    new_df_campaign["gross_cpi"] = gross_cpi
    new_df_campaign["kpi"] = kpi
    new_df_campaign["budget"] = budget
    new_df_campaign["title"] = ad_name
    new_df_campaign["budget"] = new_df_campaign["budget"].astype(int)
    new_df_campaign.to_csv(cpn_event_destination, index=False, encoding='utf-8-sig')
    #df_campaign.reset_index().to_csv(cpn_destination, index=False, encoding='utf-8-sig')

    # adgroup level
    df_adgroup = df_full.groupby(['Date', 'Campaign', 'OS Name', "Adgroup", "Creative"], as_index=False).sum()
    df_adgroup = df_adgroup.drop("Clicks", axis=1)
    df_adgroup = df_adgroup[df_adgroup["Installs"] != 0]
    headers = list(df_adgroup)
    print(headers)
    event_list = []
    for j in range(int((max_len - 11)/2)-1, -1, -1):
        event_list.append(headers[j * 2 + 6])
        df_adgroup = df_adgroup.drop(df_adgroup.columns[j * 2 + 7], axis=1)
    headers = list(df_adgroup)
    print(headers)
    print(event_list)
    df_adgroup["event_name"] = "original"
    df_adgroup["event_count"] = "original"
    new_df_adgroup = df_adgroup
    new_df_adgroup = new_df_adgroup.drop(event_list, axis=1)
    for event_name in event_list:
        each_df = df_adgroup
        each_df["event_name"] = event_name
        each_df["event_count"] = each_df[event_name]
        each_df = each_df.drop(event_list, axis=1)
        new_df_adgroup = pd.concat([new_df_adgroup, each_df])

    df_adgroup = df_adgroup[['Date', 'Campaign', 'OS Name', 'Adgroup', 'Creative', 'Installs']]
    df_adgroup["title"] = ad_name
    df_adgroup["Installs"] = df_adgroup["Installs"].astype(int)
    df_adgroup.to_csv(ad_destination, index=False, encoding='utf-8-sig')
    new_df_adgroup = new_df_adgroup[new_df_adgroup["event_name"] != "original"]
    new_df_adgroup = new_df_adgroup.drop('Installs', axis=1)
    new_df_adgroup["event_count"] = new_df_adgroup["event_count"].astype(int)
    new_df_adgroup["gross_cpi"] = gross_cpi
    new_df_adgroup["kpi"] = kpi
    new_df_adgroup["budget"] = budget
    new_df_adgroup["title"] = ad_name
    new_df_adgroup["budget"] = new_df_adgroup["budget"].astype(int)
    new_df_adgroup.to_csv(ad_event_destination, index=False, encoding='utf-8-sig')

    return cpn_destination, ad_destination, cpn_event_destination, ad_event_destination


def cohort(cohort_folder, file_destination, gross_cpi, kpi, budget, ad_name):
    print(max(glob.glob(os.path.join(cohort_folder,  "*.csv")), key=os.path.getctime))
    cohort_report = pd.read_csv(max(glob.glob(os.path.join(cohort_folder, "*.csv")), key=os.path.getctime), encoding='utf-8-sig')
    #cohort_report = pd.read_csv(cohort_folder)
    try:
        cohort_report = cohort_report[["Date", "Network", "Campaign", "Adgroup", "Creative", "Country", "OS Name", "Days after Install", "Cohort Size", "Retained Users", "Revenue Total", "Revenue Events Total",  "Time Spent per User", "Time Spent per Session", ]]
    except:
        cohort_report['Revenue Events Total'] = 0
        cohort_report['Revenue Total'] = 0
        cohort_report = cohort_report[["Date", "Network", "Campaign", "Adgroup", "Creative", "Country", "OS Name", "Days after Install","Cohort Size", "Retained Users", "Revenue Total", "Revenue Events Total", "Time Spent per User","Time Spent per Session", ]]
    cohort_report = cohort_report[cohort_report["Days after Install"] < 31]
    cohort_report["Date"] = pd.to_datetime(cohort_report["Date"])
    cohort_report = cohort_report[cohort_report['Date'] <= today]
    cohort_report = cohort_report[cohort_report['Date'] != today]

    for i in range(31):
        each_date = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(i+1)
        drop_index = cohort_report.index[(cohort_report['Date'] == each_date) & (cohort_report['Days after Install'] != i)]
        cohort_report = cohort_report.drop(drop_index)

    cohort_report["Days after Install"] = cohort_report["Days after Install"].astype(int)
    cohort_report["Cohort Size"] = cohort_report["Cohort Size"].fillna(0)
    cohort_report["Cohort Size"] = cohort_report["Cohort Size"].astype(int)
    cohort_report["Retained Users"] = cohort_report["Retained Users"].fillna(0)
    cohort_report["Retained Users"] = cohort_report["Retained Users"].astype(int)
    cohort_report["gross_cpi"] = gross_cpi
    cohort_report["kpi"] = kpi
    cohort_report["budget"] = budget
    cohort_report["title"] = ad_name
    cohort_report.to_csv(file_destination, index=False, encoding='utf-8-sig')
    return cohort_report


def install(folder, destination, gross_cpi, kpi, budget, af_name):
    print(folder)
    print(max(glob.glob(os.path.join(folder,  "*.csv")), key=os.path.getctime))
    install_report = pd.read_csv(max(glob.glob(os.path.join(folder, "*.csv")), key=os.path.getctime), encoding='utf-8-sig')
    #install_report = pd.read_csv(folder)
    install_report = install_report[["Original URL", "Attributed Touch Time", "Install Time", "Media Source", "Campaign", "Campaign ID", "Site ID", "Sub Param 1", "Sub Param 2", "Sub Param 3", "Sub Param 4", "Sub Param 5", "Country Code", "IP", "Language", "Device Type", "OS Version", "App Version", "SDK Version", "App Name", "Bundle ID", "Attribution Lookback", "Device Download Time", "Device Category", "Device Model"]]
    install_report["clickID"] = install_report['Original URL'].str.rpartition('clickid=')[2]
    install_report["clickID"] = install_report["clickID"].str.partition('&')[0]
    install_report = install_report.drop("Original URL", axis=1)
    install_report["Attributed Touch Time"] = pd.to_datetime(install_report["Attributed Touch Time"])
    install_report["Install Time"] = pd.to_datetime(install_report["Install Time"])
    install_report = install_report[(install_report["Install Time"] >= yesterday) & (install_report["Install Time"] < today)]
    install_report["gross_cpi"] = gross_cpi
    install_report["kpi"] = kpi
    install_report["budget"] = budget
    install_report["title"] = af_name
    install_report.to_csv(destination, index=False, encoding='utf-8-sig')
    return install_report

def event(folder, destination, gross_cpi, kpi, budget, af_name):
    print(max(glob.glob(os.path.join(folder,  "*.csv")), key=os.path.getctime))
    event_report = pd.read_csv(max(glob.glob(os.path.join(folder, "*.csv")), key=os.path.getctime), encoding='utf-8-sig')
    #event_report = pd.read_csv(folder)
    event_report = event_report[["Original URL", "Attributed Touch Time", "Install Time", "Event Time", "Event Revenue", "Event Revenue Currency", "Media Source", "Campaign", "Campaign ID", "Site ID", "Sub Param 1", "Sub Param 2", "Sub Param 3", "Sub Param 4", "Sub Param 5", "Country Code", "IP", "Language", "Device Type", "OS Version", "App Version", "SDK Version", "App Name", "Bundle ID", "Attribution Lookback", "Device Download Time", "Device Category", "Device Model"]]
    event_report["clickID"] = event_report['Original URL'].str.rpartition('clickid=')[2]
    event_report["clickID"] = event_report["clickID"].str.partition('&')[0]
    event_report = event_report.drop("Original URL", axis=1)
    event_report["Attributed Touch Time"] = pd.to_datetime(event_report["Attributed Touch Time"])
    event_report["Install Time"] = pd.to_datetime(event_report["Install Time"])
    event_report["Event Time"] = pd.to_datetime(event_report["Event Time"])
    event_report = event_report[(event_report["Event Time"] >= yesterday) & (event_report["Event Time"] < today)]
    event_report["gross_cpi"] = gross_cpi
    event_report["kpi"] = kpi
    event_report["budget"] = budget
    event_report["title"] = af_name
    event_report.to_csv(destination, index=False, encoding='utf-8-sig')
    return event_report



def retention(folder, destination, gross_cpi, kpi, budget, af_name):
    print(max(glob.glob(os.path.join(folder,  "*.csv")), key=os.path.getctime))
    retention_report = pd.read_csv(max(glob.glob(os.path.join(folder, "*.csv")), key=os.path.getctime), encoding='utf-8-sig')
    col_number = len(retention_report.columns)
    max_day = col_number - 5
    cols = ["Date", "Campaign", "Sub 1", "Sub 2", "Install Day", "Days after Install", "Retained Users"]
    combined = pd.DataFrame(index=[], columns=cols)

    for i in range(max_day):
        each_date = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(i+3)
        each_date = each_date.strftime("%Y-%m-%d")
        each_df = retention_report
        each_df = each_df[each_df['Date'] == each_date]
        each_df["Days after Install"] = i+1
        each_df["Retained Users"] = each_df.iloc[:,i+5]
        each_df = each_df[["Date", "Campaign", "Sub 1", "Sub 2", "Install Day", "Days after Install", "Retained Users"]]
        combined = pd.concat([combined, each_df], axis=0)

    combined["gross_cpi"] = gross_cpi
    combined["kpi"] = kpi
    combined["budget"] = budget
    combined["title"] = af_name
    combined.to_csv(destination, index=False, encoding='utf-8-sig')
    return combined



# ---------------------------------adjust -------------------------------#


def ad_bq_process_deliverable(ad_list):
    for ad_name in ad_list:
        print(ad_name)
        """3 reports 1 Cohort, 2 Deliverables"""
        gross_cpi = ad_list[ad_name][0]
        kpi = ad_list[ad_name][1]
        budget = ad_list[ad_name][2]
        # Deliverable:-
        cpn_output, ad_output, cpn_event_output, ad_event_output = deliverable(
            delfolder=r'C:\Users\sh_uchida\Desktop\SDKreport\RPAfolder\Adjust\\' + ad_name + r'\Deliverables\this month',
            cpn_destination=r'C:\Users\sh_uchida\Desktop\DB\files\Adjust\\' + ad_name + r'\Deliverable-campaign\bq_Deliverables' + today_ymd + '.csv',
            ad_destination=r'C:\Users\sh_uchida\Desktop\DB\files\Adjust\\' + ad_name + r'\Deliverable-adgroup\bq_Deliverables' + today_ymd + '.csv',
            cpn_event_destination=r'C:\Users\sh_uchida\Desktop\DB\files\Adjust\\' + ad_name + r'\Deliverable-campaign-event\bq_Deliverables' + today_ymd + '.csv',
            ad_event_destination=r'C:\Users\sh_uchida\Desktop\DB\files\Adjust\\' + ad_name + r'\Deliverable-adgroup-event\bq_Deliverables' + today_ymd + '.csv',
            gross_cpi=gross_cpi,
            kpi=kpi,
            budget=budget,
            ad_name=ad_name)



def ad_bq_process_cohort(ad_list):
    for ad_name in ad_list:
        print(ad_name)
        """3 reports 1 Cohort, 2 Deliverables"""
        gross_cpi = ad_list[ad_name][0]
        kpi = ad_list[ad_name][1]
        budget = ad_list[ad_name][2]
        #Cohorts:-
        cohort_output = cohort(
            cohort_folder=r'C:\Users\sh_uchida\Desktop\SDKreport\RPAfolder\Adjust\\' + ad_name + r'\Cohorts',
            file_destination=r'C:\Users\sh_uchida\Desktop\DB\files\Adjust\\' + ad_name + r'\Cohort\bq_cohort' + today_ymd + '.csv',
            gross_cpi=gross_cpi,
            kpi=kpi,
            budget=budget,
            ad_name=ad_name)


# ---------------------------------appsflyer -------------------------------#


def af_bq_process_installevent(af_list):
    for af_name in af_list:
        print(af_name)
        """3 report, insatll, event and retention """
        gross_cpi = af_list[af_name][0]
        kpi = af_list[af_name][1]
        budget = af_list[af_name][2]
        # install-
        install_report = install(
            folder=r'C:\Users\sh_uchida\Desktop\SDKreport\RPAfolder\Appsflyer\\' + af_name + r'\iOS\Installs\\',
            destination=r'C:\Users\sh_uchida\Desktop\DB\files\Appsflyer\\' + af_name + r'\Installs\bq_install_' + today_ymd + '.csv',
            gross_cpi=gross_cpi,
            kpi=kpi,
            budget=budget,
            af_name=af_name)
        logging.debug("---------------------------" + af_name + " install done---------------------------")

        #event:-
        event_report = event(
            folder=r'C:\Users\sh_uchida\Desktop\SDKreport\RPAfolder\Appsflyer\\' + af_name + r'\iOS\Events',
            destination=r'C:\Users\sh_uchida\Desktop\DB\files\Appsflyer\\' + af_name + r'\Events\bq_event_' + today_ymd + '.csv',
            gross_cpi=gross_cpi,
            kpi=kpi,
            budget=budget,
            af_name=af_name)
        logging.debug("---------------------------" + af_name + " event done---------------------------")

def af_bq_process_retention(af_list):
    for af_name in af_list:
        print(af_name)
        """3 report, insatll, event and retention """
        gross_cpi = af_list[af_name][0]
        kpi = af_list[af_name][1]
        budget = af_list[af_name][2]
        # retention
        retention_report = retention(
            folder=r'C:\Users\sh_uchida\Desktop\SDKreport\RPAfolder\Appsflyer\\' + af_name + r'\iOS\Retention',
            destination=r'C:\Users\sh_uchida\Desktop\DB\files\Appsflyer\\' + af_name + r'\Retention\bq_retention_' + today_ymd + '.csv',
            gross_cpi=gross_cpi,
            kpi=kpi,
            budget=budget,
            af_name=af_name)
        logging.debug("---------------------------" + af_name + " retention done---------------------------")



# ----------------------------------for past data------------------------------#

def af_bq_process_past(af_list, location):
    for af_name in af_list:
        print(af_name)
        """3 report, insatll, event and retention """
        gross_cpi = af_list[af_name][0]
        kpi = af_list[af_name][1]
        budget = af_list[af_name][2]
        # install-
        install_report = install(
            folder=location,
            destination=r'C:\Users\sh_uchida\Desktop\DB\files\Appsflyer\\' + af_name + r'\Installs\Installs_Nov' + today_ymd + '.csv',
            gross_cpi=gross_cpi,
            kpi=kpi,
            budget=budget,
            af_name=af_name)

        #event:-
        event_report = event(
            folder=location,
            destination=r'C:\Users\sh_uchida\Desktop\DB\files\Appsflyer\\' + af_name + r'\Events\Events_Oct' + today_ymd + '.csv',
            gross_cpi=gross_cpi,
            kpi=kpi,
            budget=budget,
            af_name=af_name)

        # retention
        #retention_report = retention(
        #    folder=location,
        #    destination=r'C:\Users\sh_uchida\Desktop\DB\files\Appsflyer\\' + af_name + r'\Retention\Retention_Nov' + today_ymd + '.csv',
        #    gross_cpi=gross_cpi,
        #    kpi=kpi,
        #    budget=budget,
        #    af_name=af_name)





def past_retention(folder, destination, gross_cpi, kpi, budget, af_name):
    print(max(glob.glob(os.path.join(folder,  "*.csv")), key=os.path.getctime))
    retention_report = pd.read_csv(max(glob.glob(os.path.join(folder, "*.csv")), key=os.path.getctime), encoding='utf-8-sig')
    col_number = len(retention_report.columns)
    max_day = col_number - 5
    cols = ["Date", "Campaign", "Sub 1", "Sub 2", "Install Day", "Days after Install", "Retained Users"]
    combined = pd.DataFrame(index=[], columns=cols)


    for i in range(max_day):
        each_date = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(i+3)
        each_date = each_date.strftime("%Y-%m-%d")
        each_df = retention_report
        each_df = each_df[each_df['Date'] == each_date]
        for j in range(i):
            each_each_df = each_df
            each_each_df["Days after Install"] = j+2
            each_each_df["Retained Users"] = each_each_df.iloc[:,j+6]
            each_each_df = each_each_df[["Date", "Campaign", "Sub 1", "Sub 2", "Install Day", "Days after Install", "Retained Users"]]
            combined = pd.concat([combined, each_each_df], axis=0)

    combined["gross_cpi"] = gross_cpi
    combined["kpi"] = kpi
    combined["budget"] = budget
    combined["title"] = af_name
    combined.to_csv(destination, index=False, encoding='utf-8-sig')
    return combined