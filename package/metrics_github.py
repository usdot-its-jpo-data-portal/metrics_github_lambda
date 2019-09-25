import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import httplib2
import os
import time
import datetime
import json
import yaml

from googleapiclient.http import MediaFileUpload
from googleapiclient import discovery
from google.oauth2 import service_account
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

from sesemail import sendEmail

value_range_body = {'values':[]}

def get_credentials():
    service_account_info = json.loads(os.environ['google_api_credentials'])
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return credentials

def make_request(repository, username, password, cur):
    r = requests.get("https://api.github.com/repos/usdot-its-jpo-data-portal/{}/traffic/views".format(repository), auth=HTTPBasicAuth(username,password))
    r = r.json()
    today = datetime.date.today() - datetime.timedelta(days=1)
    notoday = True
    for row in r["views"]:
        timestamp = row["timestamp"]
        timestamp = datetime.datetime.strptime(timestamp,"%Y-%m-%dT%H:%M:%SZ").date()
        if timestamp == today:
            notoday = False
            count = row["count"]
            unqiues = row["uniques"]
            cur.execute("INSERT INTO ipdh_metrics.github_metrics (repository,datetime,count,uniques) VALUES (%s,%s,%s,%s)", (repository,timestamp,count,unqiues))
    if notoday:
        cur.execute("INSERT INTO ipdh_metrics.github_metrics (repository,datetime,count,uniques) VALUES (%s,%s,0,0)", (repository,today))
        # print("not sure what this means...")

def get_monthly(repository,cur,service):
    today = datetime.datetime.combine(datetime.date.today(),datetime.time(tzinfo=datetime.timezone(datetime.timedelta(0))))
    last_month = today - datetime.timedelta(days=28)
    cur.execute("SELECT repository,datetime,count,uniques FROM ipdh_metrics.github_metrics WHERE repository = %s AND datetime >= %s",(repository,last_month))
    results = cur.fetchall()
    for record in results:
        row = []
        row.append(record[0])
        row.append(record[1].strftime("%Y-%m-%d %H:%M:%S"))
        row.append(record[2])
        row.append(record[3])
        value_range_body['values'].append(row)

def lambda_handler(event, context):
    
    try:
        with open("config.yml", 'r') as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)

        #Set username and password for GitHub account
        username = os.environ["github_username"]
        password = os.environ["github_password"]
        # conn = psycopg2.connect(config["pg_connection_string"])
        conn = psycopg2.connect(os.environ['pg_connection_string'])
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'UTC'")
        make_request("sandbox", username, password, cur)
        
        
        make_request("microsite", username, password, cur)
        conn.commit()
        credentials = get_credentials()
        # http = credentials.authorize(httplib2.Http())
        service = discovery.build('sheets', 'v4', credentials=credentials)

        # return {'statusCode': 200, 'body': 'A thing'}

        # value_range_body = {'values':[]}
        get_monthly("sandbox", cur, service)
        get_monthly("microsite", cur, service)
        cur.close()
        conn.close()
        #Enter spreadsheet id from Google Sheets object
        spreadsheet_id = os.environ["spreadsheet_id_github"]
        spreadsheetRange = "A2:E57"
        value_input_option = 'USER_ENTERED'
        request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=spreadsheetRange, valueInputOption=value_input_option, body=value_range_body)
        response = request.execute()
        print(response)
        print("End of execution - success")

    except Exception as e:
        sendEmail("Github - Lambda", str(e))