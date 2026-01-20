"""
AWS Lambda Function: SPDST Report Refresh Orchestrator

This Lambda function manages the SPDST report refresh process by:
1. Checking prerequisite conditions (Glue jobs status, file availability)
2. Monitoring data pipeline completion status
3. Triggering report generation Glue jobs when ready
4. Handling alerts and status tracking through SNS and RDS audit tables

Dependencies:
- Requires PostgreSQL connections to Redshift and RDS
- Uses AWS Services: Secrets Manager, Glue, S3, SNS
- Environment Variables must be properly configured
"""

import os
import json
import boto3
import psycopg2
from datetime import datetime, timedelta, date
import re
import ast
import sys

def get_db_connection(secret_name):
    """
    Retrieve database credentials from AWS Secrets Manager and establish a connection.

    Args:
        secret_name (str): Name of the secret in AWS Secrets Manager.

    Returns:
        connection: Database connection object.
    """
    region_name = "us-east-1"
    secrets_client = boto3.client('secretsmanager', region_name=region_name)

    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Successfully retrieved credentials for {secret_name}")
            creds = ast.literal_eval(response['SecretString'])

            dbname = creds.get("redshift_database") or creds.get("engine")  # Handle Redshift & RDS
            port = creds.get("redshift_port") or creds.get("port")
            username = creds.get("redshift_username") or creds.get("username")
            password = creds.get("redshift_password") or creds.get("password")
            host = creds.get("redshift_host") or creds.get("host")

            if not all([dbname, port, username, password, host]):
                raise KeyError(f"Missing required database credentials in {secret_name}")

            conn_string = f"dbname='{dbname}' port='{port}' user='{username}' password='{password}' host='{host}'"
            connection = psycopg2.connect(conn_string)
            print("Connection Successful")
            return connection

        else:
            print(f"Failed to retrieve credentials for {secret_name}")
            sys.exit(f"Failed to retrieve credentials for {secret_name}")
    except Exception as e:
        print(f"Error getting DB connection: {e}")
        raise

def send_sns_message(sns_client, sns_arn, subject, message):
    """
    Send a message to an SNS topic.

    Args:
        sns_client: Boto3 SNS client.
        sns_arn (str): SNS topic ARN.
        subject (str): Subject of the SNS message.
        message (dict): Message body.
    """
    try:
        response = sns_client.publish(TargetArn=sns_arn, Message=json.dumps(message), Subject=subject)

        # Print success message with MessageId
        print(f"SNS Message Published Successfully! Message ID: {response['MessageId']}, {subject}")
    except Exception as e:
        print(f"Error sending SNS message: {e}")
        raise

def check_s3_files(s3_client, bucket, folder):
    """
    Check for files in a specified S3 bucket and folder.

    Args:
        s3_client: Boto3 S3 client.
        bucket (str): S3 bucket name.
        folder (str): S3 folder path.

    Returns:
        list: List of files in the specified S3 folder.
    """
    try:
        all_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder, MaxKeys=350)
        return [obj['Key'].split('/')[-1].split('.')[0][:-15] for obj in all_objects.get('Contents', []) if not obj['Key'].startswith('SC360metadata_')]
    except Exception as e:
        print(f"Error checking S3 files: {e}")
        raise

def verify_file_existence(s3_client, env, reportregion, batch_run_date, pfile):
    """
    Verify if a specific file exists in S3 LandingZone or ArchiveZone.

    Args:
        s3_client: Boto3 S3 client.
        env (str): Deployment environment.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Date for dt= partition.
        pfile (str): Filename to search for.

    Returns:
        bool: True if file exists, False otherwise.
    """
    try:
        landing_bucket = f'sc360-{env}-{reportregion.lower()}-bucket'
        landing_folder = f'LandingZone/dt={batch_run_date}/'
        archive_folder = f'ArchiveZone/dt={batch_run_date}/'

        landing_files = check_s3_files(s3_client, landing_bucket, landing_folder)
        archive_files = check_s3_files(s3_client, landing_bucket, archive_folder)

        if pfile in landing_files or pfile in archive_files:
            return True
        return False
    except Exception as e:
        print(f"Error verifying file existence: {e}")
        raise

def check_glue_jobs(glue_client, dependent_jobs):
    """
    Check if any dependent Glue jobs are currently running.

    Args:
        glue_client: Boto3 Glue client.
        dependent_jobs (list): List of Glue job names to check.

    Returns:
        bool: True if all jobs are stopped, False if any are running.
    """
    try:
        for job in dependent_jobs:
            response = glue_client.get_job_runs(JobName=job)
            status = response['JobRuns'][0]['JobRunState']
            print(f"Execution status of {job}: {status}")
            if status in ['STARTING', 'RUNNING', 'STOPPING']:
                return False
        return True
    except Exception as e:
        print(f"Error checking Glue jobs: {e}")
        raise

def fetch_execution_time_window(rds_cursor, reportregion):
    """
    Calculate the expected execution time window.

    Args:
        rds_cursor: RDS database cursor.
        reportregion (str): Report region name.

    Returns:
        tuple: Start time, end time, and batch run date.
    """
    try:
        batch_run_date = datetime.today().date()
        rds_cursor.execute(
            """SELECT Expected_Start_time 
            FROM audit.Master_Data_For_Report_Refresh 
            WHERE regionname = %s 
            AND report_source LIKE %s;""",
            (reportregion, '%SPDST%')
        )

        expectedstart = rds_cursor.fetchall()[0][0]
        print(f"Checking expectedstart  {expectedstart}")
        
        rds_cursor.execute(
            """SELECT Average_runtime 
            FROM audit.Master_Data_For_Report_Refresh 
            WHERE regionname = %s 
            AND report_source LIKE %s;""",
            (reportregion, '%SPDST%')
        )
        averagerun = rds_cursor.fetchall()[0][0]
        
        final_time = datetime.strptime(str(expectedstart), '%H:%M:%S') + timedelta(minutes=averagerun)
        end_time = str(final_time)[11:]
        join_start = datetime.strptime(f"{batch_run_date} {expectedstart}", "%Y-%m-%d %H:%M:%S")
        join_end = datetime.strptime(f"{batch_run_date} {end_time}", "%Y-%m-%d %H:%M:%S")
        print(f"Checking expectedstart  {join_start}")
        return join_start, join_end
    except Exception as e:
        print(f"Error fetching execution time window: {e}")
        raise

def check_existing_execution_status(rds_cursor, reportregion, batch_run_date):
    """
    Check existing execution status and return relevant information.

    Args:
        rds_cursor: RDS database cursor.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Batch run date.

    Returns:
        tuple: Execution exists (bool), execution status (str or None).
    """
    try:
        print(f"Checking existing execution status for {batch_run_date}")
        rds_cursor.execute(
            "SELECT COUNT(*) FROM audit.sc360_reportrefreshtrigger_log WHERE regionname=%s AND batchrundate=%s AND report_source=%s;",
            (reportregion, batch_run_date, 'SPDST')
        )
        existing_entries = rds_cursor.fetchall()

        if existing_entries[0][0] > 0:
            rds_cursor.execute(
                """SELECT execution_status 
                FROM audit.sc360_reportrefreshtrigger_log 
                WHERE regionname = %s 
                AND batchrundate = %s 
                AND report_source = %s;""",
                (reportregion, batch_run_date, 'SPDST')
            )
            execution_status = rds_cursor.fetchall()[0][0]
            print(f"Execution exists: True, Status: {execution_status}")
            return True, execution_status
        print(f"Execution exists: False, Status: None")
        return False, None
    except Exception as e:
        print(f"Error checking existing execution status: {e}")
        raise

def handle_duplicate_execution(glue_client, rds_cursor, rds_connection, reportregion, batch_run_date, execution_status, current_time, join_start):
    """
    Handle duplicate execution cases.

    Args:
        glue_client: Boto3 Glue client.
        rds_cursor: RDS database cursor.
        rds_connection: RDS database connection.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Batch run date.
        execution_status (str): Current execution status.
        current_time (datetime): Current time.
        join_start (datetime): Expected start time.

    Returns:
        bool: True if duplicate execution is handled, False otherwise.
    """
    try:
        if execution_status in ('Finished', 'Submitted'):
            print(f"Report Refresh already triggered for {batch_run_date}")
            return True

        dependent_jobs = [
            os.environ['dependent_job1']
        ]
        
        if not check_glue_jobs(glue_client, dependent_jobs):
            print("Dependent jobs still running")
            if current_time > join_start:
                rds_cursor.execute(
                    """UPDATE audit.sc360_reportrefreshtrigger_log 
                    SET execution_status = %s, error_message = %s 
                    WHERE batchrundate = %s 
                    AND regionname = %s 
                    AND report_source = %s;""",
                    ('Delay', 'Other Region Report Refresh is already running', batch_run_date, reportregion, 'SPDST')
                )
            rds_connection.commit()
            return True
        return False
    except Exception as e:
        print(f"Error handling duplicate execution: {e}")
        raise

def retrieve_priority_files(rds_cursor, reportregion):
    """
    Retrieve priority files list.

    Args:
        rds_cursor: RDS database cursor.
        reportregion (str): Report region name.

    Returns:
        list: List of priority files.
    """
    try:
        print("Fetching priority files...")
        rds_cursor.execute(
            """SELECT filename 
            FROM audit.fileproperty_check_new 
            WHERE priorityflag = %s 
            AND region = %s 
            AND data_source NOT LIKE %s;""",
            ('YES', reportregion, 'BMT%')
        )
        priority_files = [row[0] for row in rds_cursor.fetchall()]
        print(f"Priority files retrieved: {priority_files}")
        return priority_files
    except Exception as e:
        print(f"Error retrieving priority files: {e}")
        raise

def check_loaded_curated_files(rds_cursor, reportregion, batch_run_date, priority_files):
    """
    Check loaded files in the curated layer.

    Args:
        rds_cursor: RDS database cursor.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Batch run date.

    Returns:
        list: List of loaded curated files.
    """
    try:
        print("Checking loaded curated files...")
        placeholders = ', '.join(['%s'] * len(priority_files))
        query = f"""
            SELECT DISTINCT filename 
            FROM audit.sc360_audit_log 
            WHERE batchrundate = %s 
            AND regionname = %s 
            AND processname = %s 
            AND executionstatus = %s 
            AND filename IN ({placeholders});
        """
        # Execute with dynamic file list
        rds_cursor.execute(query, (batch_run_date, reportregion, 'RedshiftCuratedLoad', 'Succeeded', *priority_files))
        loadedcuratedfiles = [row[0] for row in rds_cursor.fetchall()]
        print(f"Loaded curated files: {loadedcuratedfiles}")
        return loadedcuratedfiles
    except Exception as e:
        print(f"Error checking loaded curated files: {e}")
        raise

def map_files_to_stored_procedures(rds_cursor, reportregion, loadedcuratedfiles):
    """
    Map files to stored procedures.

    Args:
        rds_cursor: RDS database cursor.
        reportregion (str): Report region name.
        loadedcuratedfiles (list): List of loaded curated files.

    Returns:
        list: List of tuples containing loaded files and their corresponding stored procedures.
    """
    try:
        print("Mapping files to stored procedures...")
        procs_list = []
        for loadedfile in loadedcuratedfiles:
            rds_cursor.execute(
                """SELECT DISTINCT stored_procedure_name 
                FROM audit.sps_batch_master_table_updated 
                WHERE regionname = %s 
                AND source_filename LIKE %s;""",
                (reportregion, f"%{loadedfile}%")
            )
            file_stored_procs = rds_cursor.fetchall()
            if file_stored_procs:
                procs_list.append([loadedfile, file_stored_procs[0][0]])
        print(f"Procedure List:{procs_list}")
        return procs_list
    except Exception as e:
        print(f"Error mapping files to stored procedures: {e}")
        raise

def verify_published_layer_completion(rds_cursor, reportregion, batch_run_date, procs_list):
    """
    Verify published layer completion.

    Args:
        rds_cursor: RDS database cursor.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Batch run date.
        procs_list (list): List of tuples containing loaded files and their corresponding stored procedures.

    Returns:
        list: List of final files that have completed published layer processing.
    """
    try:
        print("Verifying published layer completion...")
        final_files = []
        for procs in procs_list:
            # Adjust stored procedure name if it does not end with '();'
            if not re.findall(r" \(\);$", procs[1]):
                procs[1] = procs[1].replace("('", "(''").replace("')", "'')").replace("''", "'")

            # Query to check if the process has succeeded
            rds_cursor.execute(
                """SELECT COUNT(*) FROM audit.sc360_audit_log 
                WHERE batchrundate = %s 
                AND regionname = %s 
                AND processname = %s 
                AND executionstatus = %s 
                AND scriptpath = %s;""",
                (batch_run_date, reportregion, 'RedshiftPublishedLoad', 'Succeeded', procs[1])
            )
            final_load_files_list = rds_cursor.fetchall()

            # Append to final_files if the process succeeded
            if final_load_files_list[0][0] > 0:
                final_files.append(procs[0])

        print(f"Final files after published layer check: {final_files}")
        return final_files
    except Exception as e:
        print(f"Error verifying published layer completion: {e}")
        raise

def handle_priority_files(s3_client, env, reportregion, rds_cursor, rds_connection, batch_run_date, priority_files, final_files):
    """
    Handle priority files and update status.

    Args:
        s3_client: Boto3 S3 client.
        env (str): Deployment environment.
        reportregion (str): Report region name.
        rds_cursor: RDS database cursor.
        rds_connection: RDS database connection.
        batch_run_date (datetime.date): Batch run date.
        priority_files (list): List of priority files.
        final_files (list): List of final files that have completed published layer processing.

    Returns:
        tuple: Lists of files not received, failed files, and the count of processed files.
    """
    try:
        print("Handling priority files processing...")

        filesnotreceived = []
        filesfailed = []
        pfcount = 0
        for pfile in priority_files:
            if pfile in final_files:
                # Check S3 for missing files
                filereceivedflag = verify_file_existence(s3_client, env, reportregion, batch_run_date, pfile)
                print(f"File Received Flag for {pfile}: {filereceivedflag}")

                if filereceivedflag:
                    print(f"{pfile} is loaded into s3.")
                else:
                    print(f"{pfile} has not loaded into s3.")
                
                # Check for failed processes
                rds_cursor.execute(
                    """SELECT DISTINCT processname FROM audit.sc360_audit_log 
                    WHERE batchrundate = %s 
                    AND executionstatus = %s 
                    AND filename = %s;""",
                    (batch_run_date, 'Failed', pfile)
                )
                failedprocesses = rds_cursor.fetchall()

                if not failedprocesses and not filereceivedflag:
                    filesnotreceived.append(pfile)
                    print(f"Files not received: {filesnotreceived}, Failed files: {filesfailed}")
                else:
                    pfcount += 1  # File is either received or failed processes exist
                    print(f"Processed Files received: {pfcount}")
            else:
                filesnotreceived.append(pfile)
                print(f"{pfile} has not been loaded into published layer: {final_files}")
        return filesnotreceived, filesfailed, pfcount
    except Exception as e:
        print(f"Error handling priority files: {e}")
        raise

def determine_time_period_status(current_time, cutoff_strt_hour, cutoff_strt_min, cutoff_end_hour, cutoff_end_min):
    """
    Determine the current time period status.

    Args:
        current_time (datetime): Current time.
        cutoff_strt_hour (int): Cutoff start hour.
        cutoff_strt_min (int): Cutoff start minute.
        cutoff_end_hour (int): Cutoff end hour.
        cutoff_end_min (int): Cutoff end minute.

    Returns:
        str: 'YES' if current time is within the cutoff period, 'NO' otherwise.
    """
    try:
        snstimeperiod = 'YES' if (
            (current_time.hour > cutoff_strt_hour or 
             (current_time.hour == cutoff_strt_hour and current_time.minute >= cutoff_strt_min)) and 
            (current_time.hour < cutoff_end_hour or 
             (current_time.hour == cutoff_end_hour and current_time.minute <= cutoff_end_min))
        ) else 'NO'
        return snstimeperiod
    except Exception as e:
        print(f"Error determining time period status: {e}")
        raise

def check_bmt_report_completion(rds_cursor, reportregion, batch_run_date, rds_connection, current_time, join_start):
    """
    Check BMT report completion.

    Args:
        rds_cursor: RDS database cursor.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Batch run date.
        join_start (datetime): Expected start time.
        rds_connection: RDS database connection.
        current_time (datetime): Current time.

    Returns:
        int: 1 if BMT report is completed, 0 otherwise.
    """
    try:
        print(f"Checking BMT report completion status for the date {batch_run_date}...")
        rds_cursor.execute(
            """SELECT COUNT(*) 
            FROM audit.sc360_reportrefreshtrigger_log 
            WHERE regionname = %s 
            AND batchrundate = %s 
            AND report_source = %s 
            AND execution_Status = %s;""",
            (reportregion, batch_run_date, 'BMT', 'Finished')
        )

        BMTRefreshcount = rds_cursor.fetchall()
        BMTFlag = 1 if BMTRefreshcount[0][0] > 0 else 0
        if BMTFlag == 0 and current_time > join_start:
            print("BMT Report Refresh not Triggered/Finished for the day")
            rds_cursor.execute(
                """UPDATE audit.sc360_reportrefreshtrigger_log 
                SET execution_status = %s, error_message = %s 
                WHERE batchrundate = %s 
                AND regionname = %s 
                AND report_source = %s;""",
                ('Delay', 'BMT Report Refresh not Triggered/Finished for the day', batch_run_date, reportregion, 'SPDST')
            )
            rds_connection.commit()
        return BMTFlag
    except Exception as e:
        print(f"Error checking BMT report completion: {e}")
        raise

def trigger_glue_job(glue_client, rds_cursor, rds_connection, reportregion, batch_run_date, glue_job):
    """
    Trigger Glue job and update status.

    Args:
        glue_client: Boto3 Glue client.
        rds_cursor: RDS database cursor.
        rds_connection: RDS database connection.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Batch run date.
        glue_job (str): Glue job name.
    """
    try:
        print("All conditions met. Triggering Glue job...")
        glue_client.start_job_run(JobName=glue_job,Arguments={'--reportregionname': reportregion, '--identifier': 'SPDST'})  # Override the existing --env parameter
        d = datetime.utcnow()
        
        rds_cursor.execute(
            """UPDATE audit.sc360_reportrefreshtrigger_log 
            SET Actual_Start_time = %s, execution_status = %s, error_message = %s 
            WHERE batchrundate = %s 
            AND regionname = %s 
            AND report_source = %s;""",
            (d, 'Submitted', 'NA', batch_run_date+timedelta(days=1), reportregion, 'SPDST')
        )
        rds_connection.commit()
        print("Glue job started")
    except Exception as e:
        print(f"Error triggering Glue job: {e}")
        raise

def handle_cutoff_time_reached(rds_cursor, rds_connection, reportregion, batch_run_date, filesnotreceived, filesfailed, env, sns_client, sns_arn, current_time, join_start):
    """
    Handle cutoff time reached with missing files.

    Args:
        rds_cursor: RDS database cursor.
        rds_connection: RDS database connection.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Batch run date.
        filesnotreceived (list): List of files not received.
        filesfailed (list): List of failed files.
        env (str): Deployment environment.
        sns_client: Boto3 SNS client.
        sns_arn (str): SNS topic ARN.
        current_time (datetime): Current time.
        join_start (datetime): Expected start time.
    """
    try:
        print("Cutoff time reached, handling missing files alert...")

        error_msg = f"Cut off time Reached: Missing {len(filesnotreceived)} files"
        
        if current_time > join_start:
            rds_cursor.execute(
                """UPDATE audit.sc360_reportrefreshtrigger_log 
                SET error_message = %s, execution_status = %s 
                WHERE batchrundate = %s 
                AND regionname = %s 
                AND report_source = %s;""",
                (error_msg, 'Delay', batch_run_date+timedelta(days=1), reportregion, 'SPDST')
            )
            rds_connection.commit()

        # Send appropriate alerts
        if filesnotreceived:
            send_sns_message(sns_client, sns_arn, f"** {reportregion} SPDST Priority Files Missing**", {"Env": env, "File": filesnotreceived, "Process Name": "Files Not Received", "Region": reportregion})

        if filesfailed:
            send_sns_message(sns_client, sns_arn, f"** {reportregion} SPDST Priority File Failure**", {"Env": env, "File": filesfailed, "Process Name": "NA", "Region": reportregion})
    except Exception as e:
        print(f"Error handling cutoff time reached: {e}")
        raise

def update_pending_status(rds_cursor, rds_connection, reportregion, batch_run_date, filesnotreceived):
    """
    Update status for pending files.

    Args:
        rds_cursor: RDS database cursor.
        rds_connection: RDS database connection.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Batch run date.
        filesnotreceived (list): List of files not received.
    """
    try:

        error_msg = f"Pending files: {len(filesnotreceived)}"
        rds_cursor.execute(
            """UPDATE audit.sc360_reportrefreshtrigger_log 
            SET execution_status = %s, error_message = %s 
            WHERE batchrundate = %s 
            AND regionname = %s 
            AND report_source = %s;""",
            ('Delay', error_msg, batch_run_date+timedelta(days=1), reportregion, 'SPDST')
        )
        rds_connection.commit()
    except Exception as e:
        print(f"Error updating pending status: {e}")
        raise

def handle_delay_notifications(current_time, delay_cutoff_strt_hour, delay_cutoff_strt_min, delay_cutoff_end_hour, delay_cutoff_end_min, env, reportregion, sns_client, sns_arn):
    """
    Handle delay notifications.

    Args:
        current_time (datetime): Current time.
        delay_cutoff_strt_hour (int): Delay cutoff start hour.
        delay_cutoff_strt_min (int): Delay cutoff start minute.
        delay_cutoff_end_hour (int): Delay cutoff end hour.
        delay_cutoff_end_min (int): Delay cutoff end minute.
        env (str): Deployment environment.
        reportregion (str): Report region name.
        sns_client: Boto3 SNS client.
        sns_arn (str): SNS topic ARN.
    """
    try:
        print("Handling delay notifications...")
        delay_snstimeperiod = 'YES' if (
            (current_time.hour > delay_cutoff_strt_hour or 
             (current_time.hour == delay_cutoff_strt_hour and current_time.minute >= delay_cutoff_strt_min)) and 
            (current_time.hour < delay_cutoff_end_hour or 
             (current_time.hour == delay_cutoff_end_hour and current_time.minute <= delay_cutoff_end_min))
        ) else 'NO'

        if delay_snstimeperiod == 'YES':
            sns_message = {
                "Env": env,
                "Report_Source": 'SPDST',
                "Region": reportregion,
                "Message": 'Potential delay in SPDST report generation'
            }
            sns_subject = f"*** Delay in {reportregion} SPDST Report Refresh ***"
            response = sns_client.publish(
                TargetArn=sns_arn, 
                Message=json.dumps(sns_message), 
                Subject=sns_subject
            )
            print(f"SNS Message Published Successfully! Message ID: {response['MessageId']}, {sns_subject}")
    except Exception as e:
        print(f"Error handling delay notifications: {e}")
        raise

def create_initial_log_entry(rds_cursor, rds_connection, batch_run_date, reportregion, glue_job, join_start, join_end):
    """
    Create initial log entry for execution.

    Args:
        rds_cursor: RDS database cursor.
        rds_connection: RDS database connection.
        batch_run_date (datetime.date): Batch run date.
        reportregion (str): Report region name.
        glue_job (str): Glue job name.
        join_start (datetime): Expected start time.
        join_end (datetime): Expected end time.
    """
    try:
        execution_status = 'Yet to start'
        error_message = 'SPDST report refresh pending initialization'
        
        rds_cursor.execute(
            """INSERT INTO audit.sc360_reportrefreshtrigger_log (
                batchrundate, regionname, execution_status, 
                gluejob, report_source, Expected_Start_time, 
                Expected_End_time, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);""",
            (batch_run_date+ timedelta(days=1), reportregion, execution_status, glue_job, 'SPDST', join_start, join_end, error_message)
        )
        rds_connection.commit()
        print("Initial log entry created")
    except Exception as e:
        print(f"Error creating initial log entry: {e}")
        raise

def check_priority_apj_sfdc_check(rds_cursor, reportregion, batch_run_date, current_time, env, sns_client, sns_arn):
    """
    Check if APJ OPEN SFDC file is loaded to published and handle accordingly.

    Args:
        rds_cursor: RDS database cursor.
        reportregion (str): Report region name.
        batch_run_date (datetime.date): Batch run date.
        current_time (datetime): Current time.
        env (str): Deployment environment.
        sns_client: Boto3 SNS client.
        sns_arn (str): SNS topic ARN.
    """
    try:
        # Check if the current time is within the specified range for APJ OPEN SFDC check (3:05 to 3:30 UTC)
        if int(current_time.hour) == 3 and 5 < int(current_time.minute) < 30:
            rds_cursor.execute("""SELECT COUNT(*) FROM audit.sc360_audit_log 
                                  WHERE filename='APJ_OPEN_SFDC' 
                                  AND processname='RedshiftPublishedLoad' 
                                  AND batchrundate=%s 
                                  AND executionstatus='Succeeded';""", 
                                (batch_run_date,))
            sfdc_check = rds_cursor.fetchone()[0]
            if sfdc_check > 0:
                return

            s3_client = boto3.client('s3')
            landing_bucket = f'sc360-{env}-{reportregion.lower()}-bucket'
            landing_folder = f'LandingZone/dt={batch_run_date + timedelta(days=1)}/'
            archive_folder = f'ArchiveZone/dt={batch_run_date + timedelta(days=1)}/'
            landing_files = check_s3_files(s3_client, landing_bucket, landing_folder)
            archive_files = check_s3_files(s3_client, landing_bucket, archive_folder)

            if 'APJ_OPEN_SFDC' in landing_files or 'APJ_OPEN_SFDC' in archive_files:
                return

            rds_cursor.execute("""SELECT DISTINCT processname 
                                  FROM audit.sc360_audit_log 
                                  WHERE filename='APJ_OPEN_SFDC' 
                                  AND batchrundate=%s 
                                  AND executionstatus='Failed';""", 
                                (batch_run_date,))
            failed_processes = rds_cursor.fetchall()
            if failed_processes:
                print('Sending sns for APJ open sfdc failed file')
                send_sns_message(sns_client, sns_arn, '*** APJ Priority File Failure ***', 
                                 {"Env": env, "Priority File Failed": ['APJ_OPEN_SFDC Failed'], "Process Name failed, If any": failed_processes, "Region": reportregion})
            else:
                print('Sending sns for APJ open sfdc missing file')
                send_sns_message(sns_client, sns_arn, '*** APJ SPDST Priority File Missing ***', 
                                 {"Env": env, "Missing Priority File": ['APJ_OPEN_SFDC'], "Process Name failed, If any": 'NA', "Region": reportregion})
        else:
            print('Outside APJ Open SFDC sns time range')
    except Exception as e:
        print(f"Error in check_priority_apj_sfdc_check: {e}")
        raise

def lambda_handler(event, context):
    """
    Main Lambda handler for SPDST Report Refresh orchestration.

    Workflow:
    1. Initialize connections and environment variables
    2. Check current execution status in audit tables
    3. Validate prerequisite conditions:
        - Dependent Glue jobs status
        - Priority file availability
        - BMT report completion
    4. Trigger report generation Glue job when ready
    5. Handle alerts and status updates based on cutoff times
    6. Maintain audit log for monitoring and troubleshooting

    Args:
        event (dict): Lambda event payload.
        context (dict): Lambda context object.

    Returns:
        None
    """
    try:
        # Initialize environment variables and AWS clients
        current_time = datetime.utcnow()
        reportregion = os.environ['reportregion']
        env = os.environ['env']
        glue_job = os.environ['glue_job']
        sns_arn = os.environ['sns_arn']
        delay_sns_arn = os.environ['delay_sns_arn']
        
        glue_client = boto3.client('glue')
        s3_client = boto3.client('s3')
        sns_client = boto3.client('sns')

        # Establish database connections
        redshift_connection = get_db_connection(os.environ['redshift_secret_name'])
        rds_connection = get_db_connection(os.environ['rds_secret_name'])
        redshift_cursor = redshift_connection.cursor()
        rds_cursor = rds_connection.cursor()

        # Print environment and region information
        print(f"Starting Lambda function for environment: {env}, region: {reportregion}")

        # Determine batch run date
        batch_run_date = date.today() - timedelta(days=1)

        # Check if APJ OPEN SFDC file is loaded to published
        check_priority_apj_sfdc_check(rds_cursor, reportregion, batch_run_date, current_time, env, sns_client, sns_arn)

        # Fetch execution time window
        join_start, join_end = fetch_execution_time_window(rds_cursor, reportregion)
        
        # Check existing execution status
        execution_exists, execution_status = check_existing_execution_status(rds_cursor, reportregion, batch_run_date+ timedelta(days=1))
        
        if execution_exists:
            # Handle duplicate execution scenarios
            if handle_duplicate_execution(glue_client, rds_cursor, rds_connection, reportregion, batch_run_date+ timedelta(days=1), execution_status, current_time, join_start):
                print("Duplicate execution found and handled. Exiting Lambda.")
                return

            # Retrieve priority files
            priority_files = retrieve_priority_files(rds_cursor, reportregion)
            
            # Check loaded files in curated layer
            loadedcuratedfiles = check_loaded_curated_files(rds_cursor, reportregion, batch_run_date, priority_files)
            
            # Map curated files to stored procedures
            procs_list = map_files_to_stored_procedures(rds_cursor, reportregion, loadedcuratedfiles)
            
            # Verify if files have been loaded to the published layer
            final_files = verify_published_layer_completion(rds_cursor, reportregion, batch_run_date, procs_list)
            
            # Handle priority files and update status
            filesnotreceived, filesfailed, pfcount = handle_priority_files(s3_client, env, reportregion, rds_cursor, rds_connection, batch_run_date, priority_files, final_files)

            # Determine current time period status
            snstimeperiod = determine_time_period_status(current_time, int(os.environ['cutoff_strt_hour']), int(os.environ['cutoff_strt_minute']), int(os.environ['cutoff_end_hour']), int(os.environ['cutoff_end_minute']))
            
            # Check BMT report completion
            BMTFlag = check_bmt_report_completion(rds_cursor, reportregion, batch_run_date+ timedelta(days=1), rds_connection, current_time, join_start)

            print(f"Processed files count:{pfcount}, priority file count:{len(priority_files)}, BMTFlag:{BMTFlag}, SNS Time period status:{snstimeperiod}, Missing Priority Files: {filesnotreceived}")
            
            # Main decision logic
            if pfcount == len(priority_files) and BMTFlag == 1:
                # All conditions met - trigger Glue job
                trigger_glue_job(glue_client, rds_cursor, rds_connection, reportregion, batch_run_date, glue_job)
            elif pfcount != len(priority_files) and snstimeperiod == 'YES':
                # Handle cutoff time reached with missing files
                handle_cutoff_time_reached(rds_cursor, rds_connection, reportregion, batch_run_date, filesnotreceived, filesfailed, env, sns_client, sns_arn, current_time, join_start)
            else:
                # Monitoring ongoing - update status
                if current_time > join_start:
                    update_pending_status(rds_cursor, rds_connection, reportregion, batch_run_date, filesnotreceived)
            
            # Handle delay notifications
            handle_delay_notifications(current_time, int(os.environ['delay_cutoff_strt_hour']), int(os.environ['delay_cutoff_strt_minute']), int(os.environ['delay_cutoff_end_hour']), int(os.environ['delay_cutoff_end_minute']), env, reportregion, sns_client, delay_sns_arn)
        else:
            # Initial execution - create log entry
            create_initial_log_entry(rds_cursor, rds_connection, batch_run_date, reportregion, glue_job, join_start, join_end)

        # Cleanup and completion
        redshift_connection.close()
        rds_connection.close()
        print("Lambda execution completed for", reportregion)
    
    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        raise