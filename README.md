# country-state-of-the-web
A python script to generate the country state of the web based on Chrome UX Report and HTTP Archive

## Requirement
This script need a working Google Cloud Billing with active Google Cloud project credentials in the machine. 

## Setup
1. Check config file at `config.json` and update the value for country code and which month dataset you want to generate.
2. If this is a first time you run the script under your Google Cloud Project, then you need to run `python setup.py` first. This script will generate some data that will need for monthly script.
3. Run the monthly script `python monthly.py` to generate your monthly data. You need to run the script every month whenever you want to update the data with the latest one.
4. The script will automatically upload the generated data to Google Cloud Storage in CSV and JSON format with public access setting so you can use it later for your app or Google Data Studio. Check your Google Cloud Storage bucket to see all the generated files.

## Generated Data
Below are the generated data that you will see in your Google Cloud Storage bucket
1. lighthouse_monthly: Lighthouse scores median trends
2. efconn_monthly: Effective connections monthly trends
3. lighthouse_audits: Details audit from latest Lighthouse audit
4. fcp_monthly: First Contentful Paint monthly trends
5. page_weight_monthly: Page weight monthly trends in 25th, median, 75th, 85th, and 95th percentiles
6. page_weight_med_monthly': Page weight trends details, break down to each type of resources.

## Generate Specific Technology Dataset
Use the generated data above, you can generate the data only for specific technology only. Just add the technology name that listed under HTTP Archive technology dataset to `config.json`. Each technology
