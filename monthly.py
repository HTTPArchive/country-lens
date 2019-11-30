from google.cloud import storage,exceptions
from queries import *
import json

with open('config.json', 'r') as f:
    config = json.load(f)


project_id = config['project_id']
common_dataset_id = config['common_dataset_id']
country_id = config['country_id']
crux_month = get_crux_latest_month(project_id,country_id)
ha_date = get_ha_latest_month(project_id)
origin_appearances_table= config['origin_appearances_table']
bucket_id=config['bucket_id']
origins_view='origins'

def upload_to_gs(dataframe,filename):
    client = storage.Client()
    # https://console.cloud.google.com/storage/browser/[bucket-id]/
    try:
        bucket = client.get_bucket(bucket_id)
    except exceptions.NotFound:
        bucket = client.create_bucket(bucket_id)
        bucket.make_public(True, True)

    # Then do other things...
    csv=bucket.blob(filename+'.csv')
    csv.upload_from_string(dataframe.to_csv(),content_type="text/csv")
    csv.make_public()

    json=bucket.blob(filename+'.json')
    json.upload_from_string(dataframe.to_json(),content_type="application/json")
    json.make_public()

#

upload_to_gs(generate_monthly_lighthouse(project_id,country_id,origins_view),'lighthouse_monthly')
upload_to_gs(generate_efconn_monthly(project_id,country_id,origins_view), 'efconn_monthly')
upload_to_gs(generate_fcp_monthly(project_id,country_id,origins_view),'fcp_monthly')
upload_to_gs(generate_weight_monthly(project_id,country_id,origins_view),'page_weight_monthly')
upload_to_gs(generate_weight_median_monthly(project_id,country_id,origins_view),'page_weight_med_monthly')
upload_to_gs(generate_lighthouse_audits(project_id,country_id,origins_view,ha_date),'lighthouse_audits')

for technology in config["technologies"]:
    print("Generate data for ", technology)
    table_id=technology.lower().replace('.','_').replace(' ','_')
    upload_to_gs(generate_monthly_lighthouse(project_id,country_id,table_id),'lighthouse_monthly_'+table_id)
    upload_to_gs(generate_efconn_monthly(project_id,country_id,table_id), 'efconn_monthly_'+table_id)
    upload_to_gs(generate_lighthouse_audits(project_id,country_id,table_id,ha_date),'lighthouse_audits_'+table_id)
    upload_to_gs(generate_fcp_monthly(project_id,country_id,table_id),'fcp_monthly_'+table_id)
    upload_to_gs(generate_weight_monthly(project_id,country_id,table_id),'page_weight_monthly_'+table_id)
    upload_to_gs(generate_weight_median_monthly(project_id,country_id,table_id),'page_weight_med_monthly_'+table_id)
