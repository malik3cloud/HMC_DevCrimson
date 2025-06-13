# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# <mark>**Code for future deployement and Work for Populating a table with all Pipeline Schedule**</mark>

# CELL ********************



import requests
import pandas as pd

# Authenticate and get token
auth_url = "https://login.microsoftonline.com/{tenant_id}/oauth2/token"
auth_data = {
    'grant_type': 'client_credentials',
    'client_id': '{client_id}',
    'client_secret': '{client_secret}',
    'resource': 'https://api.fabric.microsoft.com/'
}




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

auth_response = requests.post(auth_url, data=auth_data)
access_token = auth_response.json()['access_token']



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get all pipelines and their schedules
headers = {'Authorization': f'Bearer {access_token}'}
workspace_id = '{your_workspace_id}'
url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items'

response = requests.get(url, headers=headers)
pipelines = [item for item in response.json()['value'] if item['type'] == 'Pipeline']

schedule_data = []
for pipeline in pipelines:
    pipeline_id = pipeline['id']
    schedule_url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/schedules'
    schedule_response = requests.get(schedule_url, headers=headers)
    schedules = schedule_response.json().get('value', [])
    
    for schedule in schedules:
        schedule_data.append({
            'Pipeline Name': pipeline['displayName'],
            'Pipeline ID': pipeline_id,
            'Schedule ID': schedule['id'],
            'Schedule Status': schedule['status'],
            'Schedule Type': schedule['type'],
            'Frequency': schedule.get('frequency', ''),
            'Interval': schedule.get('interval', ''),
            'Start Time': schedule.get('startTime', ''),
            'End Time': schedule.get('endTime', ''),
            'Time Zone': schedule.get('timeZone', '')
        })

# Convert to DataFrame/Table
df = pd.DataFrame(schedule_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
