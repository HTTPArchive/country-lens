# Country Lens
A python script to generate the country lens based on Chrome UX Report and HTTP Archive

## Requirement
This project is using Python 3.7 so please make sure you have a same Python version setup. Follow the steps below to install all required components.

1. Setup a virtual environtment for your project by install virtualenv 

    `pip install virtualenv`

2. Activate your virtual environtment 

    `virtualenv <your-env>`

3. Install required libraries
    1. Install panda data manipulation library
    
        `pip install pandas` 
    2. Install panda data manipulation library
        
        `pip install google-cloud-bigquery`  
    3. install Google Cloud Storage library
    
        `pip install google-cloud-storage` 

## Setup
1. Check config file at `config.json` and update the value for country code and which month dataset you want to generate.
2. Run `python setup.py` first. This script will generate some data that will update the latest local origins in a country.
3. Run the monthly script `python monthly.py` to generate your monthly data. You need to run the script every month whenever you want to update the data with the latest one.
4. The script will automatically upload the generated data to Google Cloud Storage in CSV and JSON format with public access setting so you can use it later for your app or Google Data Studio. Check your [Google Cloud Storage bucket](https://console.cloud.google.com/storage/browser) to see all the generated files.

## Generated Data
Below are the generated data that you will see in your Google Cloud Storage bucket
1. lighthouse_monthly: Lighthouse scores median trends
2. efconn_monthly: Effective connections monthly trends
3. lighthouse_audits: Details audit from latest Lighthouse audit
4. fcp_monthly: First Contentful Paint monthly trends
5. page_weight_monthly: Page weight monthly trends in 25th, median, 75th, 85th, and 95th percentiles
6. page_weight_med_monthly': Page weight trends details, break down to each type of resources.

## Generate Specific Technology Dataset
Use the generated data above, you can generate the data only for specific technology only. Just add the technology name that listed under HTTP Archive technology dataset to `config.json`. Each technology will run 6 queries above, generate the data in CSV and JSON, then upload it to Google Storage with public access. 

## Consume The Data 
You can use the data with [Google Data Studio](https://datastudio.google.com) and import the CSV data as data source. Or build a dashboard by your self and use the JSON data as data source. Browse the generated data [with Google Cloud Storage Console](https://console.cloud.google.com/storage)
