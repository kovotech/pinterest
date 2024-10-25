import os
from dotenv import load_dotenv
import sys
load_dotenv()
PROJECT_PATH=os.environ.get('PROJECT_PATH_SERVER')
sys.path.append(PROJECT_PATH)
from modules.api import PinterestApi
from sqlops.schema import LoadTableSchema
from sqlops.crud import InsertQuery, DataFrameCrudOps, SelectQuery, DeleteQuery
from sqlops.engine import Engine
from sqlops.flows import add_missing_column
from sqlops.tables.sql_logger import SQLLogger
from modules.fileLogger import *
from transform.pinterest_campaign_insight import transform
from datetime import datetime as dt
import datetime
from dateutil import parser
import pandas as pd
from modules.sendgrid import email_trigger, SendgridCredentials, EmailTriggerFields, format_exception_email

# SQL Credentials
SQL_HOST=os.environ.get('SQL_HOST')
SQL_DB=os.environ.get('SQL_DB')
SQL_USER=os.environ.get('SQL_USER')
SQL_PSWD=os.environ.get('SQL_PSWD')

# previous_date = dt.today()-datetime.timedelta(days=2)
# previous_date_str = dt.strftime(previous_date,'%Y-%m-%d')
START_DATE='2024-08-24'
END_DATE='2024-08-24'

MIDDLEWARE_NAME="Pinterest"
JOB_NAME="Campaigns Insight"
JOB_TYPE=f"BulkImport [{START_DATE}-{END_DATE}]"
TABLENAME="powerbi_python.pinterest_campaign_insight"
EMAIL_LIST=os.environ['EMAIL_TRIGGER_EMAIL_LIST']

RESPONSE_FIELDS = ["CAMPAIGN_ID","CAMPAIGN_NAME","AD_ACCOUNT_ID","ADVERTISER_ID","TOTAL_IMPRESSION","TOTAL_CLICKTHROUGH","TOTAL_REPIN_RATE","TOTAL_ENGAGEMENT","TOTAL_CONVERSIONS","SPEND_IN_DOLLAR"]

LOG_FILE_PATH=f"{os.getcwd()}/Logs/bulkimport"

engine = Engine(host=SQL_HOST,
                    db=SQL_DB,
                    user=SQL_USER,
                    pswd=SQL_PSWD)
conn = engine.get_engine()

# SQL Logger
sql_logger = SQLLogger(conn)

# Email Trigger Configuration
sendgrid_credentials = SendgridCredentials(
                                        secret_key=os.environ['SENDGRID_SECRET_KEY'],
                                        from_email=os.environ['SENDGRID_FROM_EMAIL']
                                    )


def main(engine:Engine):
    #==================================== Getting Data from HubSpot API ====================================
    start_date_ = parser.parse(START_DATE)
    end_date_ = parser.parse(END_DATE)
    all_api_response_data = []
    conn = engine.get_engine()
    campaign_ids = list(
                        DataFrameCrudOps.get_data_by_query(
                            SelectQuery(
                                        LoadTableSchema(conn).load_table('pinterest_campaigns')).select(),engine)['id']
                                        )
    while start_date_ <= end_date_:
        print("===============================================",flush=True)
        start_date_str = dt.strftime(start_date_,'%Y-%m-%d')
        # print(label_list)
        print(f"Calling Pinterest Campaigns list for date: {start_date_str}")
        api_obj = PinterestApi(
                            base_url=os.environ['PINTEREST_BASE_URL'],
                            redirect_uri=os.environ['PINTEREST_REDIRECT_URI'],
                            app_id=os.environ['PINTEREST_APP_ID'],
                            app_secret=os.environ['PINTEREST_APP_SECRET'],
                            account_id=os.environ['PINTEREST_ACCOUNT_ID']
                            )
        response = api_obj.get_campaign_insights(
                                access_token=os.environ['PINTEREST_ACCESS_TOKEN'],
                                campaigns_ids=campaign_ids,
                                fields=RESPONSE_FIELDS,
                                start_date=start_date_str,
                                end_date=start_date_str
                                )
        # with open(f'test.json','w') as f:
        #     json.dump(response,fp=f,indent=3)
        for record in response:
            new_record = transform(record,start_date_str)
            all_api_response_data.append(new_record)
        # with open(f'{PROJECT_PATH}/samples/{start_date_str}.json','w') as f:
        #     json.dump(temp_data_final,fp=f,indent=3)
        start_date_ += datetime.timedelta(days=1)

    #==================================== Getting Data from GA4 API ====================================
    # ==================================== Importing Data to SQL ====================================
    print('Initiating SQL Engine...',flush=True)
    print('Validating Schema...',flush=True)
    add_missing_column(
                    all_api_response_data[0],
                    str(TABLENAME).split('.')[1],
                    engine,'mysql'
                    )
    schema = LoadTableSchema(conn)
    table = schema.load_table(str(TABLENAME).split('.')[1])
    print('SQL Import Job Started...',flush=True)
    import_record_count = 0
    for record in all_api_response_data:
        delete_query = DeleteQuery(table).delete_by_id(id_column_name='id',id_value=record['id'])
        insert_query = InsertQuery(table).insert(record)
        engine.execute_query(delete_query,delete=True)
        engine.execute_query(insert_query,insert=True)
        import_record_count += 1
    # #==================================== Importing Data to SQL ====================================
    print(f"Total Records Imported: {import_record_count}")
    return import_record_count

if __name__ == '__main__':
    try:
        imported_records_count = main(engine)
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
    # main(engine)