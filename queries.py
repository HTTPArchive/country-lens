"""# Use BigQuery through google-cloud-bigquery

See [BigQuery documentation](https://cloud.google.com/bigquery/docs) and [library reference documentation](https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/usage.html).

### Declare the Cloud project ID and other variables which will be used throughout this notebook
"""
from google.cloud import bigquery
import pandas as pd

def get_crux_latest_month(project_id,country_id):
    client = bigquery.Client(project=project_id)
    latest_month_query=f'''
      SELECT _TABLE_SUFFIX as month
      FROM `chrome-ux-report.country_{country_id}.*`
      ORDER by month DESC LIMIT 1
    '''

    rows = client.query(latest_month_query).to_dataframe()
    return rows.at[0,'month']

def get_ha_latest_month(project_id):
    client = bigquery.Client(project=project_id)
    latest_month_query=f'''
      SELECT SUBSTR(_TABLE_SUFFIX,0,10) as month
      FROM `httparchive.lighthouse.*`
      ORDER by month DESC LIMIT 1
    '''

    rows = client.query(latest_month_query).to_dataframe()
    return rows.at[0,'month']


"""# Get the PWA score trends in the country

The first thing we can do is to understand how good the experiences from country websites, what kind of experiences they're offer to the users. Please note the query below will process >5 TB data.
"""
def generate_monthly_lighthouse(project_id,country_id,origins_view):
    client = bigquery.Client(project=project_id)
    lighthouse_monthly=f'''
      SELECT
        SUBSTR(_TABLE_SUFFIX,1,10) AS month,
        count(url) as url_counts,
        (APPROX_QUANTILES(SAFE_CAST(JSON_EXTRACT(report, '$.categories.performance.score') as NUMERIC), 100)[SAFE_ORDINAL(50)] * 100) AS Performance,
        (APPROX_QUANTILES(SAFE_CAST(JSON_EXTRACT(report, '$.categories.accessibility.score') as NUMERIC), 100)[SAFE_ORDINAL(50)] * 100) AS Accessibility,
        (APPROX_QUANTILES(SAFE_CAST(JSON_EXTRACT(report, '$.categories.best-practices.score') as NUMERIC), 100)[SAFE_ORDINAL(50)] * 100) AS BestPractices,
        (APPROX_QUANTILES(SAFE_CAST(JSON_EXTRACT(report, '$.categories.seo.score') as NUMERIC), 100)[SAFE_ORDINAL(50)] * 100) AS SEO,
        (APPROX_QUANTILES(SAFE_CAST(JSON_EXTRACT(report, '$.categories.pwa.score') as NUMERIC), 100)[SAFE_ORDINAL(50)] * 100) AS PWA
      FROM
        `httparchive.lighthouse.*`
      WHERE
        url IN (SELECT CONCAT(origin,'/') FROM `{project_id}.{country_id}.{origins_view}`) AND
        REGEXP_CONTAINS(_TABLE_SUFFIX, r"01_mobile$") AND
        JSON_EXTRACT(report, '$.categories.seo.score') IS NOT NULL AND
        JSON_EXTRACT(report, '$.categories.pwa.score') IS NOT NULL AND
        JSON_EXTRACT(report, '$.categories.performance.score') IS NOT NULL AND
        JSON_EXTRACT(report, '$.categories.best-practices.score') IS NOT NULL AND
        JSON_EXTRACT(report, '$.categories.accessibility.score') IS NOT NULL
      GROUP BY
        month
      ORDER BY
        month
    '''

    df_lighthouse_monthly = client.query(lighthouse_monthly).to_dataframe()
    return pd.melt(df_lighthouse_monthly, id_vars=['month'], value_vars=['Performance','Accessibility','BestPractices','SEO','PWA'],var_name='category', value_name='score')


"""### Check the audits to see how it affecting the scores."""
def generate_lighthouse_audits(project_id,country_id,origins_view,ha_date):
    client = bigquery.Client(project=project_id)
    lh_audits = f'''
    SELECT
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['first-contentful-paint'].score") AS NUMERIC),0)>0) AS first_contentful_paint,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['first-meaningful-paint'].score") AS NUMERIC),0)>0) AS first_meaningful_paint,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['first-cpu-idle'].score") AS NUMERIC),0)>0) AS first_cpu_idle,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['interactive'].score") AS NUMERIC),0)>0) AS interactive,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['speed-index'].score") AS NUMERIC),0)>0) AS speed_index,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['estimated-input-latency'].score") AS NUMERIC),0)>0) AS estimated_input_latency,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['service-worker'].score") AS NUMERIC),0)>0) AS service_worker,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['works-offline'].score") AS NUMERIC),0)>0) AS work_offline,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['without-javascript'].score") AS NUMERIC),0)>0) AS without_javascript,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['is-on-https'].score") AS NUMERIC),0)>0) AS is_on_https,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['redirects-http'].score") AS NUMERIC),0)>0) AS redirect_http,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['load-fast-enough-for-pwa'].score") AS NUMERIC),0)>0) AS load_fast_enough_for_pwa,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['webapp-install-banner'].score") AS NUMERIC),0)>0) AS webapp_install_banner,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['splash-screen'].score") AS NUMERIC),0)>0) AS splash_screen,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['themed-omnibox'].score") AS NUMERIC),0)>0) AS themed_omnibox,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['viewport'].score") AS NUMERIC),0)>0) AS viewport,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['appcache-manifest'].score") AS NUMERIC),0)>0) AS appcache_manifest,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['no-websql'].score") AS NUMERIC),0)>0) AS no_websql,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['uses-http2'].score") AS NUMERIC),0)>0) AS uses_http2,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['uses-passive-event-listeners'].score") AS NUMERIC),0)>0) AS uses_passive_event_listeners,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['no-mutation-events'].score") AS NUMERIC),0)>0) AS no_mutation_events,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['no-document-write'].score") AS NUMERIC),0)>0) AS no_document_write,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['geolocation-on-start'].score") AS NUMERIC),0)>0) AS geolocation_on_start,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['no-vulnerable-libraries'].score") AS NUMERIC),0)>0) AS no_vulnerable_libraries,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['notification-on-start'].score") AS NUMERIC),0)>0) AS notification_on_start,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['deprecations'].score") AS NUMERIC),0)>0) AS deprecations,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['manifest-short-name-length'].score") AS NUMERIC),0)>0) AS manifest_short_name_length,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['password-inputs-can-be-pasted-into'].score") AS NUMERIC),0)>0) AS password_inputs_can_be_pasted_into,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['errors-in-console'].score") AS NUMERIC),0)>0) AS errors_in_console,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['uses-responsive-images'].score") AS NUMERIC),0)>0) AS uses_responsive_images,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['uses-webp-images'].score") AS NUMERIC),0)>0) AS uses_webp,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['offscreen-images'].score") AS NUMERIC),0)>0) AS offscreen_images,
      COUNTIF(IFNULL(SAFE_CAST(JSON_EXTRACT(LH.report, "$.audits['image-aspect-ratio'].score") AS NUMERIC),0)>0) AS image_aspect_ratio
    FROM
      `httparchive.latest.lighthouse_mobile` as LH
     WHERE
        RTRIM(url,'/') IN (SELECT origin FROM `{project_id}.{country_id}.{origins_view}`)
    '''

    df_audits = client.query(lh_audits).to_dataframe()
    return pd.melt(df_audits, value_vars=[ 'first_contentful_paint','first_meaningful_paint','first_cpu_idle','interactive','speed_index','estimated_input_latency','service_worker','work_offline','without_javascript','is_on_https','redirect_http','load_fast_enough_for_pwa','webapp_install_banner','splash_screen','themed_omnibox','viewport','appcache_manifest','no_websql','uses_http2','uses_passive_event_listeners','no_mutation_events','no_document_write','geolocation_on_start','no_vulnerable_libraries','notification_on_start','deprecations','manifest_short_name_length','password_inputs_can_be_pasted_into','errors_in_console','uses_responsive_images','uses_webp','offscreen_images','image_aspect_ratio'],var_name='audit', value_name='count')

"""# Analyze The Performance and See What's Affecting It

### Get the First Contentful Paint(FCP) monthly trends
"""
def generate_fcp_monthly(project_id,country_id,origins_view):
    client = bigquery.Client(project=project_id)
    fcp_monthly = f'''
    SELECT
      yyyymm AS month,
      SUM(fast_fcp)/COUNT(DISTINCT origin) AS fast,
      SUM(avg_fcp)/COUNT(DISTINCT origin) AS avg,
      SUM(slow_fcp)/COUNT(DISTINCT origin) AS slow
    FROM
      `chrome-ux-report.materialized.metrics_summary`
    WHERE
      origin IN (SELECT origin FROM `{project_id}.{country_id}.origins`)
    GROUP BY
      month
    ORDER BY
      month
    '''
    df_fcp = client.query(fcp_monthly).to_dataframe()
    return pd.melt(df_fcp, id_vars=['month'], value_vars=['fast','avg','slow'],var_name='fcp', value_name='pct')



"""### Get the effective connection type to see how it affected the FCP"""
def generate_efconn_monthly(project_id,country_id,origins_view):
    client = bigquery.Client(project=project_id)
    effectiveConnections = f'''
    SELECT
      _TABLE_SUFFIX AS month,
      effective_connection_type.name AS name,
      COUNT(effective_connection_type.name) AS counts
    FROM
      `chrome-ux-report.country_{country_id}.*`
    WHERE
      origin IN (SELECT origin FROM `{project_id}.{country_id}.{origins_view}`)
    GROUP BY
      month, name
    '''
    return client.query(effectiveConnections).to_dataframe()

"""### Get page weights monthly trends"""
def generate_weight_monthly(project_id,country_id,origins_view):
    client = bigquery.Client(project=project_id)
    page_weight_monthly = f'''
    SELECT
      _TABLE_SUFFIX AS month,
      APPROX_QUANTILES(bytesTotal/1024, 100)[SAFE_ORDINAL(25)] AS `Pct25th`,
      APPROX_QUANTILES(bytesTotal/1024, 100)[SAFE_ORDINAL(50)] AS `Median`,
      APPROX_QUANTILES(bytesTotal/1024, 100)[SAFE_ORDINAL(75)] AS `Pct75th`,
      APPROX_QUANTILES(bytesTotal/1024, 100)[SAFE_ORDINAL(85)] AS `Pct85th`,
      APPROX_QUANTILES(bytesTotal/1024, 100)[SAFE_ORDINAL(95)] AS `Pct95th`
    FROM
     `httparchive.summary_pages.2019_*`
    WHERE
      url IN (SELECT CONCAT(origin,'/') FROM `{project_id}.{country_id}`.{origins_view}) AND
      REGEXP_CONTAINS(_TABLE_SUFFIX, r"01_mobile$")
    GROUP BY
      month
    ORDER BY
      month
      '''
    df_weight_monthly = client.query(page_weight_monthly).to_dataframe()
    return pd.melt(df_weight_monthly, id_vars=['month'], value_vars=['Pct25th','Median','Pct75th','Pct85th','Pct95th'],var_name='percentile', value_name='weight')

"""To see the details of page weight we check the median weight from reach resources"""
def generate_weight_median_monthly(project_id,country_id,origins_view):
    client = bigquery.Client(project=project_id)
    weight_median_monthly = f'''
    SELECT
      _TABLE_SUFFIX AS month,
      APPROX_QUANTILES(bytesHtml/1024, 100)[SAFE_ORDINAL(50)] AS HTML,
      APPROX_QUANTILES(bytesCSS/1024, 100)[SAFE_ORDINAL(50)] AS CSS,
      APPROX_QUANTILES(bytesJS/1024, 100)[SAFE_ORDINAL(50)] AS JavaScript,
      APPROX_QUANTILES(bytesImg/1024, 100)[SAFE_ORDINAL(50)] AS Images,
      APPROX_QUANTILES(bytesFont/1024, 100)[SAFE_ORDINAL(50)] AS Fonts,
      APPROX_QUANTILES((bytesHtmlDoc+bytesFlash+bytesJson+bytesOther)/1024, 100)[SAFE_ORDINAL(50)] AS Others
    FROM
     `httparchive.summary_pages.*`
    WHERE
      url IN (SELECT CONCAT(origin,'/') FROM `{project_id}.{country_id}`.{origins_view}) AND
      REGEXP_CONTAINS(_TABLE_SUFFIX, r"01_mobile$")
    GROUP BY
      month
    ORDER BY
      month
    '''
    df_weight_median_monthly = client.query(weight_median_monthly).to_dataframe()
    return pd.melt(df_weight_median_monthly, id_vars=['month'], value_vars=['HTML','CSS','JavaScript','Images','Fonts','Others'],var_name='resources', value_name='weight')


"""### Get the technologies those developers are use to develop the web"""
def generate_tech_used(project_id,ha_date,country_id):
    client = bigquery.Client(project=project_id)
    tech_used= f'''
    SELECT
      COUNT(url) AS websites,
      app
    FROM
      `httparchive.technologies.{ha_date}_mobile`
    WHERE
        RTRIM(url,'/') IN (SELECT origin FROM `{project_id}.{country_id}.origins`)
    GROUP BY app
    '''
    return client.query(tech_used).to_dataframe()

# Check the API adoptions
def generate_api_adopted(project_id,country_id,origins_view):
    client = bigquery.Client(project=project_id)
    api_adoptions = f'''
    SELECT
      COUNTIF(REGEXP_CONTAINS(body, r"^<script.*async")) AS script_async,
      COUNTIF(REGEXP_CONTAINS(body, r"^<script.*defer")) AS script_defer,
      COUNTIF(REGEXP_CONTAINS(body, r"^<link.*preload")) AS asset_preload,
      COUNTIF(REGEXP_CONTAINS(body, r"^<link.*prefetch")) AS asset_prefetch,
      COUNTIF(REGEXP_CONTAINS(body, r"^<link.*preconnect")) AS preconnect,
      COUNTIF(REGEXP_CONTAINS(body, r"^<link.*dns-prefetch")) AS dns_preconnect,
      COUNTIF(REGEXP_CONTAINS(body, r"^IntersectionObserver")) AS intersection_observer,
      COUNTIF(REGEXP_CONTAINS(body, r"^NetworkInformation.effectiveType")) AS network_information,
      COUNTIF(REGEXP_CONTAINS(body, r"^navigator.serviceWorker.register")) AS service_worker,
      COUNTIF(REGEXP_CONTAINS(body, r"<picture>")) AS image_picture,
      COUNTIF(REGEXP_CONTAINS(body, r"srcset=")) AS image_srcset
    FROM `httparchive.latest.response_bodies_mobile`
    WHERE
      RTRIM(page,'/') IN (SELECT origin FROM `{project_id}.{country_id}.origins`)
    '''
    df_apis = client.query(api_adoptions).to_dataframe()

    return pd.melt(df_apis, value_vars=['script_async','script_defer','asset_preload','asset_prefetch','preconnect','dns_preconnect','intersection_observer','network_information','service_worker','image_picture','image_srcset'],var_name='APIs', value_name='websites')
