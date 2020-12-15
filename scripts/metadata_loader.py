# generates metadata.json files for every analysis in Fitness, as specified by
# Alhena sheet in the Fitness google sheet

# credentials.json and token.pickle should be placed in the same repo as /scripts

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


from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# spreadsheet id of fitness pseudobulk
SAMPLE_SPREADSHEET_ID = '1veY6s3r-aNu8w7Y4GDZrn0yh6rf2Pe7hQdGbtQePmsk'
SAMPLE_RANGE_NAME = 'alhena!A:I'


def get_save_alhena_google_data(directory):
    data = get_alhena_google_data()
    metadata_df = make_dataframe(data)
    save_metadata(metadata_df, directory)


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
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values')

    if not values:
        print('No data found.')

    return values


def make_dataframe(data):

    metadata_df = pd.DataFrame(data[1:], columns=data[0])
    metadata_df = metadata_df.groupby('alhena_id').agg(
        lambda x: x.unique().tolist())

    metadata_df.rename(columns={'jira_ticket': 'libraries'}, inplace=True)
    if "description" not in metadata_df.columns:
        metadata_df["description"] = ""
    return metadata_df


def save_metadata(df, directory):
    for i in df.index:
        curr_metadata = df.loc[i]
        curr_metadata["dashboard_id"] = i
        curr_filename = i + ".json"
        currfile_path = os.path.join(directory, curr_filename)
        curr_metadata.to_json(currfile_path)


def main(directory):
    get_save_alhena_google_data(directory)


if __name__ == "__main__":
    directory = sys.argv[1]
    main(directory)
