
import logging
import os
import sys
import json
import requests
import io
import pickle
import collections
import pandas as pd
import numpy as np


# import mira.constants as constants
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# spreadsheet id of fitness pseudobulk
SAMPLE_SPREADSHEET_ID = '1veY6s3r-aNu8w7Y4GDZrn0yh6rf2Pe7hQdGbtQePmsk'
SAMPLE_RANGE_NAME = 'alhena!A:I'

CREDENTIALS_PATH = ""

# primary function to be imported and called, pulls alhena metadata from the fitness_pseudobulk google sheet
# from metadata_loader import get_save_alhena_google_data

#5.2 start here!
def get_save_alhena_google_data():
    data = get_alhena_google_data()
    metadata_df = make_dataframe(data)
    save_metadata(metadata_df)


def get_alhena_google_data():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('/home/nguyenk1/alhena-loader/scripts/token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values')

    if not values:
        print('No data found.')
    else:
        print("Alhena Google Sheet Values Found!")
        # print(values)
        # for row in values:
        # Print columns A and E, which correspond to indices 0 and 4.
        # print(row)
    return values


'''
creates dataFrame from metadata, pandas group by alhena_id, 
renames list of jira_tickets to libraries per current specs

'''


def make_dataframe(data):

    metadata_df = pd.DataFrame(data[1:], columns=data[0])
    metadata_df = metadata_df.groupby('alhena_id').agg(list)
    metadata_df.rename(columns={'jira_ticket': 'libraries'}, inplace=True)
    return metadata_df


'''
saves and puts them in /dat/alhena + /merged + /DASHBOARD_ID.json
will overwrite whatever dashboard_id.json in /merged, nice!
'''


def save_metadata(df):

    currdir = "/dat/alhena" + "/merged"
    for i in df.index:
        curr_metadata = df.loc[i]
        curr_metadata["dashboard_id"] = i
        currfile_path = currdir + "/" + i + ".json"
        curr_metadata.to_json(currfile_path)


def main():
    get_save_alhena_google_data()


if __name__ == "__main__":
    main()
