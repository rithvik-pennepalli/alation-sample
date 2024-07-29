import pandas as pd
import requests
import psycopg2
import csv

# Function to make API call to retreve data in Alation
def get_table():
    # create an empty dictionary for the data
    table_oid_mapping = {}
    try:
        # connect to alation using its host api and port
        connection_string = psycopg2.connect(
            database='alation_analytics_v2', user='g684814', password='Pennepalli1', host='xalationp2.genmills.com',
            port='25432')
        cursor = connection_string.cursor()
        sql = f"select s.name || '.' || t.name || ':' || t.table_id from public.rdbms_tables t,public.rdbms_schemas s, public.rdbms_datasources d where t.schema_id=s.schema_id and t.ds_id=d.ds_id and t.deleted is false"
        cursor.execute(sql)
        data_dictionary = cursor.fetchall()
        connection_string.close()
        # add to dictionary
        for data in data_dictionary:
            records = str(data)
            (key, val) = (records.strip("(',)").split(":"))
            table_oid_mapping[key] = val
        print("Dictionary has number of elements :" + str(len(table_oid_mapping)))
    except Exception as e:
        print("Connection to AAV2 Database has failed")
        print(e)

    return table_oid_mapping 

# Function to read csv file with table info extracted from GCP
def read_csv(filename):
    # store data locally
    data = []
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row.get('upload_assigned_dmp', '').upper() == 'Y':
                data.append(row)
    return data

# Function to upload and/or update information in Alation 
def upload_to_alation(table, dmp):
    # connect to alation api using your token
    urls = "https://alation.genmills.com/integration/v2/custom_field_value/async/"
    header = {"Token": "pc2eAKpFcyKqqjaLgt--Zflzh_FhwLyKslip7f0K71E"}
    # create payload for data upload request
    payload = [
        {
        "field_id": 10099,
        "otype": "table",
        "oid": table,
        "value": [dmp]
    }
        ]
    # upload to alation
    response = requests.put(url=urls, headers=header, json=payload, verify=False)
    
    if response.status_code == 200:
        print("Data uploaded successfully to Alation.")
    else:
        print("Error uploading data to Alation:", response.text)

# Main Code
# read through csv of tables
filename = 'complete_set_related_dmp_cdf.csv'
relevant_data = read_csv(filename)

# set dmp value and table id, upload to alation
table = get_table()
table_id = ''
table_name = ''
for row in relevant_data:
    table_name = row['downstream_dataset'][1:-1]
    if table_name in table:
        table_id = table.pop(table_name)
    else:
        continue

    dmp = row['dmp_unit']

    upload_to_alation(table_id,dmp)




