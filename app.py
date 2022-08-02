#!/usr/bin/env python
# coding: utf-8

import sys
import pandas as pd
import numpy as np

from google.oauth2 import service_account
from googleapiclient.discovery import build

from functions import get_refresh_token, get_page, reformat_duration, \
    split_partnership_code, split_reporting_period, \
    fill_partnership_code, find_excel_column_letter


print("Starting..")
# GOOGLE SHEET CONFIG
SERVICE_ACCOUNT_FILE = 'wgprtm-google-service-account.json'
GOOGLE_SHEETS_ID = '173-GNIKtlgtuf8nqYP2wYVOxKroeD565ArIXbJ-Vv4Q'
worksheet_name = 'Sheet1'
sheet_id = '780014659'

# FLOW CONFIG
# test using seap
# instance = "seap"
# sid = 286500912
# fid = 308161007

# 2scale UII 2 - SHF form
instance = "2scale"
fid = 40200005
sid = 11320004

# Get refresh token
print("Get authentication..")
f = open('token.txt', 'r')
refresh_token = f.read()
f.close()
if not refresh_token:
    refresh_token = get_refresh_token()

if not refresh_token:
    print("Error get refresh token")
    sys.exit()

# Get data
try:
    print("Downloading data..")
    data = get_page(instance=instance, survey_id=sid,
                    form_id=fid, token=refresh_token)
except(Exception):
    print("Get new token for authentication..")
    refresh_token = get_refresh_token()
    print("Downloading data..")
    data = get_page(instance=instance, survey_id=sid,
                    form_id=fid, token=refresh_token)

# Create data frame
print("Processing data..")
df = pd.DataFrame(data)
df = df.reset_index()
df = df.set_index('index')
df = df.replace(np.nan, '', regex=True)

# Rename id to instance
# Rename surveyal time to duration
df = df.rename(columns={'id': 'instance', 'surveyal time': 'duration'})

# convert duration seconds into h:m:s format
df['duration'] = df['duration'].apply(reformat_duration)

# create Partnership Code: - Country Name &
# Partnership Code: - Partnership Code column
df['partnership code: - country name'] = df[
    'partnership code:'].apply(split_partnership_code, index=0)
df['partnership code: - partnership code'] = df[
    'partnership code:'].apply(split_partnership_code, index=1)

# create reporting period - year & reporting period - period column
df['reporting period - year'] = df[
    'reporting period'].apply(split_reporting_period, index=0)
df[
    'reporting period - period'] = df[
        'reporting period'].apply(split_reporting_period, index=1)

# create report year as blank column
df['report year'] = ''


# GOOGLE SHEETS
print("Preparing data for gsheets..")
credentials = service_account.Credentials.from_service_account_file(
    filename=SERVICE_ACCOUNT_FILE)
service_sheets = build('sheets', 'v4', credentials=credentials)
# Read data from google sheets
result = service_sheets.spreadsheets().values().get(
    spreadsheetId=GOOGLE_SHEETS_ID,
    range=worksheet_name,
).execute()
rows = result.get('values', [])
get_columns = rows[0]
columns_order = [c.lower().strip() for c in get_columns]
columns_order = [x for x in columns_order if x != '']
print('{0} rows retrieved.'.format(len(rows)))

# Re-order df by google sheet template
df = df[columns_order]

# Transform / fill partnership code for repeat groups answer
# replace 'name' with partnership code column name
# target_col = 'name'
target_col = 'partnership code: - partnership code'
df[target_col] = df.apply(
    fill_partnership_code, axis=1, df=df, target_col=target_col)

# prepare data to insert into GOOGLE SHEETS
columns = tuple(df.columns)
values = []
# rename columns by template column name
renamed_columns = [get_columns[idx] for idx, x in enumerate(columns)]
# values.append(columns)
values.append(renamed_columns)
for index, row in df.iterrows():
    tmp = []
    for c in columns:
        try:
            # there's 2 columns reporting period - year & reporting period
            tmp.append(row.get(c).iloc[0])
        except(AttributeError):
            tmp.append(row.get(c))
    values.append(tuple(tmp))

# Clear gsheets
print("Clear gsheets..")
request_body = {
    'requests': [
        {
            'deleteDimension': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'ROWS',
                    'start_index': 1,
                    'endIndex': len(rows) + 1
                }
            }
        },
        {
            'deleteDimension': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'start_index': 1,
                    'endIndex': len(renamed_columns) + 1
                }
            }
        }
    ]
}
if len(rows) > 1:
    service_sheets.spreadsheets().batchUpdate(
        spreadsheetId=GOOGLE_SHEETS_ID,
        body=request_body
    ).execute()

# Add data
print("Writing gsheets..")
cell_range_insert = '!A1:{0}{1}'.format(
    find_excel_column_letter(renamed_columns), len(values))

value_range_body = {
    'majorDimension': 'ROWS',
    'values': values
}

service_sheets.spreadsheets().values().update(
    spreadsheetId=GOOGLE_SHEETS_ID,
    valueInputOption='USER_ENTERED',
    range=worksheet_name + cell_range_insert,
    body=value_range_body
).execute()
print("Done !")
