import os
from dotenv import load_dotenv
import sys
load_dotenv()
PROJECT_PATH=os.environ.get('PROJECT_PATH_SERVER')
sys.path.append(PROJECT_PATH)
from modules.api import PinterestApi
from sqlops.schema import LoadTableSchema
from sqlops.crud import InsertQuery, DeleteQuery
from sqlops.engine import Engine
from sqlops.flows import add_missing_column
from sqlops.tables.sql_logger import SQLLogger
from modules.fileLogger import *
from datetime import datetime as dt
import datetime
from dateutil import parser
import json
from modules.sendgrid import email_trigger, SendgridCredentials, EmailTriggerFields, format_exception_email

# SQL Credentials
SQL_HOST=os.environ.get('SQL_HOST')
SQL_DB=os.environ.get('SQL_DB')
SQL_USER=os.environ.get('SQL_USER')
SQL_PSWD=os.environ.get('SQL_PSWD')

MIDDLEWARE_NAME="Pinterest"
JOB_NAME="Campaigns"
JOB_TYPE=f"All Campaing List"
TABLENAME="powerbi_python.pinterest_campaigns"
EMAIL_LIST=os.environ['EMAIL_TRIGGER_EMAIL_LIST']


LOG_FILE_PATH=f"{os.getcwd()}/Logs/bulkimport"

# SQL Logger
engine = Engine(host=SQL_HOST,
                    db=SQL_DB,
                    user=SQL_USER,
                    pswd=SQL_PSWD)
conn = engine.get_engine()
sql_logger = SQLLogger(conn)

# Email Trigger Configuration
sendgrid_credentials = SendgridCredentials(
                                        secret_key=os.environ['SENDGRID_SECRET_KEY'],
                                        from_email=os.environ['SENDGRID_FROM_EMAIL']
                                    )


def main():
    #==================================== Getting Data from HubSpot API ====================================
    print(f"Calling Pinterest Campaigns list")
    api_obj = PinterestApi(
                        base_url=os.environ['PINTEREST_BASE_URL'],
                        redirect_uri=os.environ['PINTEREST_REDIRECT_URI'],
                        app_id=os.environ['PINTEREST_APP_ID'],
                        app_secret=os.environ['PINTEREST_APP_SECRET'],
                        account_id=os.environ['PINTEREST_ACCOUNT_ID']
                        )
    response = api_obj.get_campaign_list(page_size=100,access_token=os.environ['PINTEREST_ACCESS_TOKEN'])
    # with open(f'test.json','w') as f:
    #     json.dump(response,fp=f,indent=3)
    # with open(f'{PROJECT_PATH}/samples/{start_date_str}.json','w') as f:
    #     json.dump(temp_data_final,fp=f,indent=3)

    #==================================== Getting Data from GA4 API ====================================
    # ==================================== Importing Data to SQL ====================================
    print('Initiating SQL Engine...',flush=True)
    engine = Engine(host=SQL_HOST,
                    db=SQL_DB,
                    user=SQL_USER,
                    pswd=SQL_PSWD)
    conn = engine.get_engine()
    print('Validating Schema...',flush=True)
    add_missing_column(
                    response[0],
                    str(TABLENAME).split('.')[1],
                    engine,'mysql'
                    )
    schema = LoadTableSchema(conn)
    table = schema.load_table("pinterest_campaigns")
    print('SQL Import Job Started...',flush=True)
    imported_records_count = 0
    for record in response:
        delete_query = DeleteQuery(table).delete_by_id(id_column_name='id',id_value=record['id'])
        insert_query = InsertQuery(table).insert(record)
        engine.execute_query(delete_query,delete=True)
        engine.execute_query(insert_query,insert=True)
        imported_records_count += 1
    # #==================================== Importing Data to SQL ====================================
    print(f"Total Records Imported: {imported_records_count}")
    return imported_records_count

if __name__ == '__main__':
    try:
        imported_records_count = main()
        sql_logger.import_to_sql(
                                middleware_name=MIDDLEWARE_NAME,
                                job_name=JOB_NAME,
                                job_type=JOB_TYPE,
                                table_name=TABLENAME,
                                log_type="INFO",
                                log_description=f"Successfully Executed | Records Imported: {imported_records_count}"
                                )
        myLogger(level='info',msg=f"{imported_records_count} imported to {str(TABLENAME).split('.')[1]} table",folder_path=LOG_FILE_PATH)
    except Exception as e:
        formatted_log = format_exception_logfile(e)
        sql_logger.import_to_sql(
                                middleware_name=MIDDLEWARE_NAME,
                                job_name=JOB_NAME,
                                job_type=JOB_TYPE,
                                table_name=TABLENAME,
                                log_type="ERROR",
                                log_description=formatted_log
                                )
        myLogger(level='error',msg=formatted_log,folder_path=LOG_FILE_PATH)
        email_trigger_config = EmailTriggerFields(
                                                Subject="Test Company Middleware Notification",
                                                MiddlewareName=MIDDLEWARE_NAME,
                                                JobName=JOB_NAME,
                                                JobType=JOB_TYPE,
                                                TableName=TABLENAME,
                                                LogType="Error",
                                                LogDescription=format_exception_email(e)
                                                )
        email_trigger(EMAIL_LIST,email_trigger_config,sendgrid_credentials)
# main()