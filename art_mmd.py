import logging
import csv

from datetime import timedelta, datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="fyj-342011"
GS_PATH = "data/"

ART_BUCKET_NAME = 'art_mmd'
HTS_BUCKET_NAME = 'hts_etl/HTS'
VLS_BUCKET_NAME = 'vl_etl'

ART_STAGING_DATASET = "ART_MMD"
HTS_STAGING_DATASET = "HTS"
VLS_STAGING_DATASET = "VLS"

LOCATION = "us"


default_args = {
    'owner': 'Alex Twenji',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'start_date':  datetime(2022,7,27),
    'retry_delay': timedelta(minutes=3),
}

with DAG('icdr', schedule_interval='@once', default_args=default_args) as dag:

# ART_MMD Pipeline

    load_dataset_ART = GCSToBigQueryOperator(
        task_id = 'load_dataset_ART',
        bucket = ART_BUCKET_NAME,
        source_objects = ['*.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.staging_ART_MMD',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {"name": "DOB", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CCC", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PatientPK", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "AgeEnrollment", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "AgeARTStart", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "AgeLastVisit", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "SiteCode", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "FacilityName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RegistrationDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "PatientSource", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PreviousARTStartDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "StartARTAtThisFAcility", "type": "DATE", "mode": "NULLABLE"},
        {"name": "StartARTDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "PreviousARTUse", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "PreviousARTPurpose", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PreviousARTRegimen", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DateLastUsed", "type": "STRING", "mode": "NULLABLE"},
        {"name": "StartRegimen", "type": "STRING", "mode": "NULLABLE"},
        {"name": "StartRegimenLine", "type": "STRING", "mode": "NULLABLE"},
        {"name": "LastARTDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "LastRegimen", "type": "STRING", "mode": "NULLABLE"},
        {"name": "LastRegimenLine", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ExpectedReturn", "type": "DATE", "mode": "NULLABLE"},
        {"name": "LastVisit", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Duration", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ExitDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "ExitReason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Date_Created", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "Date_Last_Modified", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
        )

    deduplicate_ART = BigQueryOperator(
        task_id='deduplicate_ART',
        sql='''
        #standardSQL
        SELECT DISTINCT * from `fyj-342011.ART_MMD.staging_ART_MMD`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.staging_ART_MMD_deduplicate',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    return_heirarchy = BigQueryOperator(
        task_id='ART_return_dates_heirarchy',
        sql='''
        #standardSQL
        SELECT *,
        DATE_DIFF(ExpectedReturn, LastARTDate, year) AS years,
        DATE_DIFF(ExpectedReturn, LastARTDate, month) AS months,
        DATE_DIFF(ExpectedReturn, LastARTDate, day) AS days,
        FROM `fyj-342011.ART_MMD.staging_ART_MMD_deduplicate`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.staging_ART_MMD_MMD',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    clean_regimen = BigQueryOperator(
        task_id='clean_regimen_lines',
        sql='''
        #standardSQL
        SELECT *,CASE
          WHEN LastRegimenLine = "First line" THEN "1st line"
          WHEN LastRegimenLine = "Second line" THEN "2nd line"
          WHEN LastRegimenLine = "Third line" THEN "3rd line"
          ELSE "Uncategorized"
          END AS LastRegimenLineClean,
          CASE
          WHEN StartRegimenLine = "First line" THEN "1st line"
          WHEN StartRegimenLine = "Second line" THEN "2nd line"
          WHEN StartRegimenLine = "Third line" THEN "3rd line"
          ELSE "Uncategorized"
          END AS StartRegimenLineClean
          FROM `fyj-342011.ART_MMD.staging_ART_MMD_MMD`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.staging_ART_MMD_MMD_Regimen',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    date_visit = BigQueryOperator(
        task_id='date_enrichment',
        sql='''
        #standardSQL
        SELECT *,
        DATE_ADD(LastVisit, INTERVAL Duration day) AS DateExpected
        FROM `fyj-342011.ART_MMD.staging_ART_MMD_MMD_Regimen`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.staging_ART_MMD_CurrentOnTreatment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    tx_curr = BigQueryOperator(
        task_id='current_on_treatment_enrichment',
        sql='''
        #standardSQL
        SELECT *,
        DATE_DIFF(CURRENT_DATE("UTC"), DateExpected, day) AS CurrentDays
        FROM `fyj-342011.ART_MMD.staging_ART_MMD_CurrentOnTreatment`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.staging_ART_MMD_CurrentOnTreatment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    tx_curr2 = BigQueryOperator(
        task_id='further_current_on_treatment_enrichment',
        sql='''
        #standardSQL
        SELECT *, CASE
          WHEN ExitReason IS NOT NULL THEN "NO"
          WHEN CurrentDays > 30 THEN "NO"
          WHEN CurrentDays < 31 THEN "Yes"
          END AS CurrentOnTreatment
          FROM `fyj-342011.ART_MMD.staging_ART_MMD_CurrentOnTreatment`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.staging_ART_MMD_CurrentOnTreatment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    mfl_ART = BigQueryOperator(
        task_id='ART_joining_MFL_Codes',
        sql='''
        #standardSQL
        SELECT MFL_Codes.SiteCode, MFL_Codes.county_name, MFL_Codes.constituency_name, MFL_Codes.sub_county_name,
          MFL_Codes.ward_name, MFL_Codes.lat, MFL_Codes.long, Staging.DOB, Staging.Gender, Staging.CCC as PatientID,
          Staging.PatientPK, Staging.AgeEnrollment, Staging.AgeARTStart, Staging.AgeLastVisit, Staging.FacilityName,
          Staging.RegistrationDate, Staging.PatientSource, Staging.PreviousARTStartDate, Staging.StartARTAtThisFAcility,
          Staging.StartARTDate, Staging.PreviousARTUse, Staging.PreviousARTPurpose, Staging.PreviousARTRegimen, Staging.DateLastUsed,
          Staging.StartRegimen, Staging.StartRegimenLine, Staging.LastARTDate, Staging.LastRegimen, Staging.LastRegimenLine, Staging.ExpectedReturn, Staging.LastVisit, Staging.Duration,
          Staging.ExitDate, Staging.ExitReason, Staging.Date_Created, Staging.Date_Last_Modified, Staging.years, Staging.months, Staging.days,
          Staging.LastRegimenLineClean, Staging.StartRegimenLineClean, Staging.DateExpected, Staging.CurrentDays, Staging.CurrentOnTreatment
          FROM `fyj-342011.ART_MMD.MFL_Codes` as MFL_Codes
          INNER JOIN `fyj-342011.ART_MMD.staging_ART_MMD_CurrentOnTreatment` as Staging
          ON MFL_Codes.SiteCode = Staging.SiteCode
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_Warehouse_test',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    dates_ART = BigQueryOperator(
        task_id='ART_enriching_joined_table',
        sql='''
        #standardSQL
        SELECT *,
            FORMAT_DATETIME("%Y", LastARTDate) AS LastARTYear,
            FORMAT_DATETIME("%B", LastARTDate) AS LastARTMonth,
            EXTRACT(DAY FROM LastARTDate) AS LastARTDay,
            FORMAT_DATETIME("%Y", StartARTDate) AS StartARTYear,
            FORMAT_DATETIME("%B", StartARTDate) AS StartARTMonth,
            EXTRACT(DAY FROM StartARTDate) AS StartARTDay,
        FROM `fyj-342011.ART_MMD.ART_MMD_Warehouse_test`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_Warehouse_test',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    warehouse_ART = BigQueryOperator(
        task_id='ART_MMD_data_warehouse',
        sql='''
        #standardSQL
        select distinct * from `fyj-342011.ART_MMD.ART_MMD_Warehouse_test`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_Warehouse',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

# VLS PIPELINE

    load_dataset_VLS = GCSToBigQueryOperator(
        task_id = 'load_dataset_VLS',
        bucket = VLS_BUCKET_NAME,
        source_objects = ['*.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.staging_VLS',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {"name": "Mfl_code", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "patient_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ccc_number", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DOB", "type": "DATE", "mode": "NULLABLE"},
        {"name": "ageInYears", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "date_test_result_requested", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date_test_result_received", "type": "STRING", "mode": "NULLABLE"},
        {"name": "lab_test", "type": "STRING", "mode": "NULLABLE"},
        {"name": "urgency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "order_reason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "test_result", "type": "STRING", "mode": "NULLABLE"},
        ]
        )

    deduplicate_VLS = BigQueryOperator(
        task_id='deduplicate_VLS',
        sql='''
        #standardSQL
        SELECT DISTINCT * from `fyj-342011.VLS.staging_VLS`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.staging_deduplicate',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    denullification_VLS = BigQueryOperator(
        task_id='denullification_VLS',
        sql='''
        #standardSQL
        SELECT * from
          (SELECT * from `fyj-342011.VLS.staging_deduplicate`
           WHERE  ccc_number IS NOT NULL)
        WHERE ((Mfl_code IS NOT NULL) and (ccc_number IS NOT NULL))
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.staging_NULLS',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    viral_load = BigQueryOperator(
        task_id='viral_load_only',
        sql='''
        #standardSQL
        SELECT * FROM `fyj-342011.VLS.staging_NULLS`
        WHERE lab_test = "VIRAL LOAD"
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.staging_viral_load',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    latest_date = BigQueryOperator(
        task_id='latest_vl_result',
        sql='''
        #standardSQL
        SELECT MFL_code, ccc_number, MAX(CAST(date_test_result_received as DATE)) AS results_date
        FROM `fyj-342011.VLS.staging_viral_load`
        GROUP BY Mfl_code, ccc_number
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.staging_recent_dates',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    single_records = BigQueryOperator(
        task_id='single_patient_records',
        sql='''
        #standardSQL
        SELECT RD.Mfl_code as SiteCode, RD.ccc_number, RD.results_date as vl_results_date, Staging.Gender,
        Staging.DOB, Staging.ageInYears as vl_ageInYears, Staging.date_test_result_requested as vl_date_test_result_requested,
        Staging.lab_test as vl_lab_test, Staging.urgency as vl_urgency, Staging.order_reason as vl_order_reason,
        Staging.test_result as vl_test_result
          FROM `fyj-342011.VLS.staging_recent_dates` as RD
          LEFT JOIN `fyj-342011.VLS.staging_viral_load` as Staging
          ON RD.ccc_number = Staging.ccc_number
          WHERE CAST(RD.results_date AS STRING) = Staging.date_test_result_received
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.staging_patient_single_records',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    valid_tests = BigQueryOperator(
        task_id='valid_results',
        sql='''
        #standardSQL
        SELECT *, CASE
          WHEN days.days_since_test < 366 THEN "yes"
          WHEN days.days_since_test > 365 THEN "no"
          END AS vl_valid
          from(
          SELECT *,
          DATE_DIFF(CURRENT_DATE("UTC"), vl_results_date, day) AS days_since_test
          FROM `fyj-342011.VLS.staging_patient_single_records`) as days
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.staging_valid_results',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    VLS_Warehouse = BigQueryOperator(
        task_id='VLS_Warehouse',
        sql='''
        #standardSQL
        SELECT * FROM `fyj-342011.VLS.staging_valid_results`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.VLS_Warehouse',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    art_vls_1 = BigQueryOperator(
        task_id='merge_art_vls',
        sql='''
        #standardSQL
        SELECT ART.SiteCode, ART.county_name, ART.constituency_name, ART.sub_county_name, ART.ward_name, ART.lat,
          ART.long, ART.DOB, ART.Gender, ART.PatientID, ART.PatientPK, ART.AgeEnrollment, ART.AgeARTStart, ART.AgeLastVisit,
          ART.FacilityName, ART.RegistrationDate, ART.PatientSource, ART.PreviousARTStartDate, ART.StartARTAtThisFAcility,
          ART.StartARTDate, ART.PreviousARTUse, ART.PreviousARTPurpose, ART.PreviousARTRegimen, ART.DateLastUsed,
          ART.StartRegimen, ART.StartRegimenLine, ART.LastARTDate, ART.LastRegimen, ART.LastRegimenLine, ART.ExpectedReturn,
          ART.LastVisit, ART.Duration, ART.ExitDate, ART.ExitReason, ART.Date_Created, ART.Date_Last_Modified,
          ART.years, ART.months, ART.days, ART.LastRegimenLineClean, ART.StartRegimenLineClean, ART.DateExpected,
          ART.CurrentDays, ART.CurrentOnTreatment, ART.LastARTYear, ART.LastARTMonth, ART.LastARTDay, ART.StartARTYear,
          ART.StartARTMonth, ART.StartARTDay, VLS.vl_results_date, VLS.vl_ageInYears, VLS.vl_date_test_result_requested,
          VLS.vl_lab_test, VLS.vl_urgency, VLS.vl_order_reason, VLS.vl_test_result, VLS.days_since_test as vl_days_since_test,
          VLS.vl_valid
          FROM `fyj-342011.ART_MMD.ART_MMD_Warehouse` as ART
          LEFT JOIN `fyj-342011.VLS.VLS_Warehouse` as VLS
          ON ART.PatientID = VLS.ccc_number
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.ART_MMD_Warehouse',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    art_vls_2 = BigQueryOperator(
        task_id='viral_load_suppression',
        sql='''
        #standardSQL
        SELECT *, CASE
          WHEN load_numbers < 1000 AND CurrentOnTreatment = "Yes" THEN "yes"
          WHEN load_numbers >= 1000 AND CurrentOnTreatment = "Yes" THEN "no"
          WHEN load_numbers IS NULL AND CurrentOnTreatment = "Yes" THEN "vl data not available"
          ELSE "not TX_Curr"
          END AS viral_load_suppressed,
          FROM (
            SELECT *, CASE
              WHEN vl_test_result = "LDL" THEN 0
              WHEN vl_test_result != "LDL" THEN cast(vl_test_result as DECIMAL)
              END AS load_numbers,
            FROM `fyj-342011.VLS.ART_MMD_Warehouse`)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_Warehouse',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    art_vls_3 = BigQueryOperator(
        task_id='art_vls_summary_table',
        sql='''
        #standardSQL
        SELECT
           SUM(CASE WHEN CurrentOnTreatment = "Yes" THEN 1 ELSE 0 END) AS TxCurr
           ,SUM(CASE WHEN CurrentOnTreatment = "Yes" AND vl_valid = "yes" THEN 1 ELSE 0 END) AS ValidVL
           ,SUM(CASE WHEN viral_load_suppressed = "yes" AND vl_valid = "yes"
                AND CurrentOnTreatment = "Yes" THEN 1 ELSE 0 END) AS Suppressed
           ,SUM(CASE WHEN viral_load_suppressed = "no" AND vl_valid = "yes"
                AND CurrentOnTreatment = "Yes" THEN 1 ELSE 0 END) AS Unsuppressed
           ,SUM(CASE WHEN vl_valid = "no" AND CurrentOnTreatment = "Yes" THEN 1 ELSE 0 END) AS DueVL
        FROM `fyj-342011.ART_MMD.ART_MMD_Warehouse`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_Warehouse_summary',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

# HTS PIPELINE
    load_dataset_HTS = GCSToBigQueryOperator(
        task_id = 'load_dataset_HTS',
        bucket = HTS_BUCKET_NAME,
        source_objects = ['*.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.staging_HTS',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {"name": "patient_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "Mfl_code", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "DOB", "type": "DATE", "mode": "NULLABLE"},
        {"name": "ageInYears", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "Gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Entry_point", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ever_tested", "type": "STRING", "mode":"NULLABLE"},
        {"name": "prev_test_results", "type": "STRING", "mode":"NULLABLE"},
        {"name": "Prev_test_date", "type": "STRING", "mode":"NULLABLE"},
        {"name": "elegible_for_test", "type": "STRING", "mode":"NULLABLE"},
        # {"name": "visit_date", "type":"STRING", "mode": "NULLABLE"}, #
        {"name": "patient_consented", "type": "STRING", "mode": "NULLABLE"},
        {"name": "client_tested_as", "type": "STRING", "mode": "NULLABLE"},
        {"name": "approach", "type": "STRING", "mode": "NULLABLE"},
        {"name": "test_1_kit_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "test_1_result", "type": "STRING", "mode": "NULLABLE"},
        {"name": "test_2_kit_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "test_2_result", "type": "STRING", "mode": "NULLABLE"},
        {"name": "final_test_result", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date_tested", "type": "STRING", "mode": "NULLABLE"},
        {"name": "patient_given_result", "type": "STRING", "mode": "NULLABLE"},
        {"name": "referral_for", "type": "STRING", "mode": "NULLABLE"},
        {"name": "referral_facility", "type": "STRING", "mode": "NULLABLE"},
        {"name": "remarks", "type": "STRING", "mode": "NULLABLE"}, #
        {"name": "facility_linked_to", "type": "STRING", "mode": "NULLABLE"},
        {"name": "enrollment_date", "type": "STRING", "mode": "NULLABLE"},
        {"name": "art_start_date", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ccc_number", "type": "STRING", "mode": "NULLABLE"},
        # {"name": "siteCode", "type": "STRING", "mode": "NULLABLE"}, #
        # {"name": "FacilityName", "type": "STRING", "mode": "NULLABLE"},#
        ]
        )

    deduplicate_HTS = BigQueryOperator(
        task_id='deduplicate_HTS',
        sql='''
        #standardSQL
        #HTS
        SELECT DISTINCT * from `fyj-342011.HTS.staging_HTS`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.staging_1_deduplicate',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    mfl_HTS = BigQueryOperator(
        task_id='HTS_joining_MFL_Codes',
        sql='''
        #standardSQL
        SELECT MFL_Codes.SiteCode, MFL_Codes.county_name, MFL_Codes.sub_county_name, MFL_Codes.lat, MFL_Codes.long,
          MFL_codes.officialname as facility_name, Staging.patient_id, Staging.DOB, Staging.Gender, Staging.ageInYears,
          Staging.Entry_point as entrypoint, Staging.patient_consented, Staging.client_tested_as, Staging.approach,
          Staging.test_1_kit_name, Staging.test_1_result, Staging.test_2_kit_name, Staging.test_2_result,
          Staging.final_test_result, Staging.date_tested, Staging.patient_given_result, Staging.referral_for,
          Staging.referral_facility, Staging.remarks, Staging.facility_linked_to,
          Staging.enrollment_date, Staging.art_start_date, Staging.ccc_number
          FROM `fyj-342011.ART_MMD.MFL_Codes` as MFL_Codes
          INNER JOIN `fyj-342011.HTS.staging_1_deduplicate` as Staging
          ON MFL_Codes.SiteCode =  Staging.Mfl_code
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.staging_1_latlong',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    dates_HTS = BigQueryOperator(
        task_id='HTS_enriching_joined_table',
        sql='''
        #standardSQL
        SELECT *,
        DATE_DIFF(cast (art_start_date as date), cast (date_tested as date), day) AS LinkageDays,
        EXTRACT(YEAR FROM cast (date_tested as date)) AS date_tested_Year,
        EXTRACT(QUARTER FROM cast (date_tested as date)) AS date_tested_Quarter,
        EXTRACT(MONTH FROM cast (date_tested as date)) AS date_tested_Month,
        EXTRACT(YEAR FROM cast (art_start_date as date)) AS art_start_date_Year,
        EXTRACT(QUARTER FROM cast (art_start_date as date)) AS art_start_date_Quarter,
        EXTRACT(MONTH FROM cast (art_start_date as date)) AS art_start_date_Month,
        EXTRACT(YEAR FROM cast (enrollment_date as date)) AS enrollment_date_Year,
        EXTRACT(QUARTER FROM cast (enrollment_date as date)) AS enrollment_date_Quarter,
        EXTRACT(MONTH FROM cast (enrollment_date as date)) AS enrollment_date_Month
        FROM `fyj-342011.HTS.staging_1_latlong`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.staging_1_dates',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    entrypoint_1 = BigQueryOperator(
        task_id='HTS_enriching_entrypoint',
        sql='''
        #standardSQL
        SELECT *, CASE
          WHEN entrypoint = "CCC (comprehensive care center)" OR entrypoint = "CCC" THEN "CCC"
          WHEN entrypoint = "OPD (outpatient department)" OR entrypoint= "Out Patient Department(OPD)" THEN "OPD"
          WHEN entrypoint = "VCT center" OR entrypoint = "VCT" THEN "VCT"
          WHEN entrypoint = "Home based HIV testing program" THEN "Home Based Testing"
          WHEN entrypoint = "In Patient Department(IPD)" OR entrypoint= "INPATIENT CARE OR HOSPITALIZATION" THEN "IPD"
          WHEN entrypoint = "PMTCT ANC" OR entrypoint= "PMTCT MAT" OR entrypoint= "PMTCT Program" OR entrypoint= "PMTCT PNC" THEN "PMTCT"
          WHEN entrypoint = "OTHER NON-CODED" THEN "Other"
          WHEN entrypoint = "mobile VCT program" THEN "mobile VCT program"
          WHEN entrypoint = "Tuberculosis treatment program" THEN "Tuberculosis treatment program"
          WHEN entrypoint is null THEN null
          ELSE entrypoint
        END AS entrypointclean
        FROM `fyj-342011.HTS.staging_1_dates`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.HTS_entrypoints',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    entrypoint_2 = BigQueryOperator(
        task_id='HTS_enriching_entrypoint_2',
        sql='''
        #standardSQL
        SELECT *, CASE
          WHEN entrypoint = "CCC (comprehensive care center)" OR entrypoint = "CCC" THEN "0"
          WHEN entrypoint = "OPD (outpatient department)" OR entrypoint= "Out Patient Department(OPD)" THEN "0"
          WHEN entrypoint = "VCT center" OR entrypoint = "VCT" THEN "0"
          WHEN entrypoint = "Home based HIV testing program" THEN "0"
          WHEN entrypoint = "In Patient Department(IPD)" OR entrypoint= "INPATIENT CARE OR HOSPITALIZATION" THEN "0"
          WHEN entrypoint = "PMTCT ANC" OR entrypoint= "PMTCT MAT" OR entrypoint= "PMTCT Program" OR entrypoint= "PMTCT PNC" THEN "0"
          WHEN entrypoint = "OTHER NON-CODED" THEN "0"
          WHEN entrypoint = "mobile VCT program" THEN "0"
          WHEN entrypoint = "Tuberculosis treatment program" THEN "0"
          WHEN entrypoint is null THEN null
          ELSE entrypoint
        END AS entrypointclean2
        FROM `fyj-342011.HTS.HTS_entrypoints`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.HTS_entrypoints',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    entrypoint_3 = BigQueryOperator(
        task_id='HTS_enriching_entrypoint_3',
        sql='''
        #standardSQL
        SELECT *, CASE
          WHEN entrypointclean2 = "0" THEN entrypointclean
          WHEN entrypointclean2 is null THEN null
          ELSE "Other"
        END AS entrypointclean3
        FROM `fyj-342011.HTS.HTS_entrypoints`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.HTS_entrypoints',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    warehouse_HTS = BigQueryOperator(
        task_id='HTS_data_warehouse',
        sql='''
        #standardSQL
        SELECT *
        FROM `fyj-342011.HTS.HTS_entrypoints`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.HTS_Warehouse',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    HTS_summary_1 = BigQueryOperator(
        task_id='HTS_summary',
        sql='''
        #standardSQL
        SELECT * FROM (SELECT *, CASE
        WHEN LinkageDays = 0
          AND final_test_result = "Positive"
          THEN 'Same Day'
        WHEN LinkageDays > 0
          AND LinkageDays < 15
          AND final_test_result = "Positive" THEN ">1 day <2 weeks"
        WHEN LinkageDays >14
          AND final_test_result = "Positive" THEN ">2 weeks"
        WHEN LinkageDays < 0
          AND final_test_result = "Positive" THEN "Clerical Error"
        WHEN LinkageDays is null
          AND final_test_result = "Positive" THEN "Not Linked"
        END AS hts_cascade
        FROM `fyj-342011.HTS.HTS_Warehouse`)
        WHERE hts_cascade is not null
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.HTS_Warehouse_summary',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    HTS_summary_2 = BigQueryOperator(
        task_id='HTS_warehouse_summary',
        sql='''
        #standardSQL
        SELECT
             SUM(CASE WHEN hts_cascade IS NOT NULL THEN 1 ELSE 0 END) AS totalPositive
            ,SUM(CASE WHEN hts_cascade ="Same Day" THEN 1 ELSE 0 END) AS sameDay
            ,SUM(CASE WHEN hts_cascade = ">1 day <2 weeks" THEN 1 ELSE 0 END) AS oneDayToTwoWeeks
            ,SUM(CASE WHEN hts_cascade = ">2 weeks" THEN 1 ELSE 0 END) AS moreThanTwoWeeks
            ,SUM(CASE WHEN hts_cascade = "Clerical Error" THEN 1 ELSE 0 END) AS clericalError
            ,SUM(CASE WHEN hts_cascade = "Not Linked" THEN 1 ELSE 0 END) AS notLinked
        FROM `fyj-342011.HTS.HTS_Warehouse_summary`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.HTS_Warehouse_summary',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )


# ART_MMD Pipeline
load_dataset_ART >> deduplicate_ART >> return_heirarchy >> clean_regimen >> date_visit >> tx_curr
tx_curr >> tx_curr2 >> mfl_ART >> dates_ART >> warehouse_ART

# VLS Pipeline
warehouse_ART >> load_dataset_VLS >> deduplicate_VLS >> denullification_VLS >> viral_load >> latest_date
latest_date >> single_records >> valid_tests >> VLS_Warehouse >> art_vls_1 >> art_vls_2 >> art_vls_3

# HTS Pipeline
load_dataset_HTS >> deduplicate_HTS >> mfl_HTS >> dates_HTS >> entrypoint_1 >> entrypoint_2
entrypoint_2 >> entrypoint_3 >> warehouse_HTS >> HTS_summary_1 >> HTS_summary_2
