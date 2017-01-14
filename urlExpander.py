__author__ = 'alexi'

import httplib2
import os
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from apiclient import discovery
import requests
import json
import sys
import threading


try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


class SpreadSheet:

    APPLICATION_NAME = 'urlExpander'
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    CLIENT_SECRET_FILE = 'client_secret.json'
    app_json = 'sheets.googleapis.com-urlExpander.json'

    def __init__(self, spreadsheetId=None, sh_col=None, lng_col=None):
        self.spreadsheetId = spreadsheetId
        self.sh_col = sh_col
        self.lng_col = lng_col
        current_path = os.getcwd()
        credential_dir = os.path.join(current_path, "creds")   # Current dir creds path
        credential_path = os.path.join(credential_dir, self.app_json)

        if not os.path.exists(credential_dir):
            print("creating credentials directory")
            os.makedirs(credential_dir)

        store = Storage(credential_path)
        self.credentials = store.get()

        if not self.credentials or self.credentials.invalid:
            flow = client.flow_from_clientsecrets(SpreadSheet.CLIENT_SECRET_FILE, SpreadSheet.SCOPES)
            flow.user_agent = SpreadSheet.APPLICATION_NAME
            if flags:
                self.credentials = tools.run_flow(flow, store, flags)
            else: # Needed only for compatibility with Python 2.6
                self.credentials = tools.run(flow, store)
            print('Storing credentials to ' + credential_path)

        self.http = self.credentials.authorize(httplib2.Http())

    def get_urls(self):
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
        service = discovery.build('sheets', 'v4', http=self.http,
                              discoveryServiceUrl=discoveryUrl)

        spreadsheetId = self.spreadsheetId

        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheetId, range=self.sh_col).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')

        for row in values:
            if row:
                yield row[0]

    def write_urls(self, urls):
        if not urls:
            print("No Urls to write")
            return

        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
        service = discovery.build('sheets', 'v4', http=self.http,
                              discoveryServiceUrl=discoveryUrl)

        body = {
        'values': urls,
        "majorDimension": "COLUMNS"
        }
        result = service.spreadsheets().values().update(
        spreadsheetId = self.spreadsheetId, range=self.lng_col,
        valueInputOption = "RAW", body=body).execute()
        return result

def unshorten_url(url):
    long_url = requests.head(url, allow_redirects=True).url
    return long_url


def read_config(settings):
    for x in settings["SpreadSheets"]:
        yield x['Name'], x['SpreadSheetID'], x['ShortUrlColumn'], x['LongUrlColumn']

def factory(spname, spid, shcol, lncol):
    print("Start reading " +spname)
    sp = SpreadSheet(spid, shcol, lncol)
    urls = sp.get_urls()

    if not urls:
        print("Nothing to write to" + spname + "Exiting.")
        print(urls)
    rez = []
    rez.append([unshorten_url(uu) for uu in urls])
    sp.write_urls(rez)
    print("writing " + spname + " done")
    return

if __name__ == "__main__":
    print("Start reading sheet")
    print("Reading configuration..")
    with open('UrlExpanderSettings_NEW.json') as json_data:
        Settings = json.load(json_data)
    print("Done.")
     # create threads
    threads = [threading.Thread(target=factory, args=args) for args in read_config(Settings)]

    # start threads
    for t in threads:
        t.start()

    # wait for threads to finish
    for t in threads:
        t.join()

    print("Job is done.")