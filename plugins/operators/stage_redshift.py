'''
Implements
StageToRedshiftOperator
'''
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''
    Inherits BaseOperator
    and implements 
    StageToRedShiftOperator
    '''
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_source="",
                 json_paths="",
                 file_type="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_source = s3_source
        self.file_type = file_type
        self.json_paths = json_paths

    def execute(self, context):
        '''
        Get data on S3 bucket and
        insert into Redshift Stage Table
        '''
        self.log.info(f"Start Redshift Operator")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info(f"Delete data from staging table: {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run("DELETE FROM {0}".format(self.table))

        self.log.info(f"Copy data from S3 to the staging table: {self.table}")
        # Build copy option
        if self.file_type == "JSON":
            copy_query = """
                COPY {table}
                FROM '{s3_source}'
                ACCESS_KEY_ID '{access_key}'
                SECRET_ACCESS_KEY '{secret_key}'
                {file_type} '{json_paths}';
            """.format(table=self.table,
                       s3_source=self.s3_source,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                       file_type=self.file_type,
                       json_paths=self.json_paths)
        else:
            self.log.error("File type should be JSON")
            raise ValueError("File type should be JSON")

        redshift.run(copy_query)
        self.log.info(f"Success loading S3 data to Redshift table : {self.table}")