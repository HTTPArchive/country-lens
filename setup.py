from google.cloud import bigquery
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

"""# Identify Local Websites
We're defining local websites as the websites that only accessed by users in a country and not exist in other countries dataset. Using that definition than we need to check all the website that exist in each country datasets.

### Count origin apperances
"""



client = bigquery.Client(project=project_id)
common_dataset=project_id+'.'+common_dataset_id;
global_appearance_table=common_dataset+'.'+origin_appearances_table;

# Make sure we have common dataset
try:
  dataset=client.get_dataset(common_dataset)
except:
  # Construct a full Dataset object to send to the API.
  datasetref = client.dataset(common_dataset_id)
  dataset = bigquery.Dataset(datasetref)
  # Specify the geographic location where the dataset should reside.
  dataset.location = "US"

  # Send the dataset to the API for creation.
  # Raises google.api_core.exceptions.Conflict if the Dataset already
  # exists within the project.
  dataset = client.create_dataset(dataset)  # API request
  print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

# Create appearance data
job_config = bigquery.QueryJobConfig()
job_config.write_disposition='WRITE_TRUNCATE';

table_ref = client.dataset(common_dataset_id).table(origin_appearances_table)
job_config.destination = table_ref

sql=f'''
  SELECT
    origin,
    count(country) AS appearances
  FROM `chrome-ux-report.materialized.country_summary`
  WHERE
    yyyymm = '{crux_month}'
  GROUP BY origin
  '''

query_job = client.query(sql,location='US',job_config=job_config)
# API request - starts the query

query_job.result()  # Waits for the query to finish
print('View global appearance created to table {}'.format(table_ref.path))


"""### Create the country dataset"""

# Set dataset_id to the ID of the dataset to create.
country_dataset_id = f'{client.project}.{country_id}'
try:
  dataset=client.get_dataset(country_dataset_id)
  print("Dataset {}.{} exist!".format(client.project, dataset.dataset_id))
except:
  # Construct a full Dataset object to send to the API.
  datasetref = client.dataset(country_id)
  dataset = bigquery.Dataset(datasetref)
  # Specify the geographic location where the dataset should reside.
  dataset.location = "US"

  # Send the dataset to the API for creation.
  # Raises google.api_core.exceptions.Conflict if the Dataset already
  # exists within the project.
  dataset = client.create_dataset(dataset)  # API request
  print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

"""### Create view for selecting the origins those only accessed from a country"""



# Set the destination table
table_ref = client.dataset(country_id).table('origins')
job_config.destination = table_ref

sql=f'''
    SELECT
        DISTINCT origin
    FROM `chrome-ux-report.country_{country_id}.{crux_month}`
      WHERE origin in (
        SELECT origin
        FROM `{global_appearance_table}`
        WHERE appearances = 1
      )
'''

query_job = client.query(
sql,
# Location must match that of the dataset(s) referenced in the query
# and of the destination table.
location='US',
job_config=job_config)  # API request - starts the query

query_job.result()  # Waits for the query to finish
print('Country local websites results loaded to table {}'.format(table_ref.path))

for technology in config["technologies"]:
    table_id=technology.lower().replace('.','_').replace(' ','_')

    table_ref = client.dataset(country_id).table(table_id)
    job_config.destination = table_ref

    sql=f'''
        SELECT DISTINCT origin
        FROM
            `chrome-ux-report.country_{country_id}.{crux_month}` c
        LEFT JOIN
            `httparchive.technologies.{ha_date}_*` t
            ON
                c.origin = RTRIM(t.url,'/')
        WHERE origin in (
            SELECT origin
            FROM `{global_appearance_table}`
            WHERE appearances = 1
        ) AND
        app = '{technology}'
    '''
    query_job = client.query(
    sql,
    # Location must match that of the dataset(s) referenced in the query
    # and of the destination table.
    location='US',
    job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Country technology spesific website results loaded to table {}'.format(table_ref.path))
