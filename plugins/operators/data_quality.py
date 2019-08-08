'''
Data quality
'''
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 value="",
                 data_quality_query="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.value = value
        self.data_quality_query = data_quality_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running quality checks for {}".format(self.tables))
        tables = self.tables
        for table in tables:
            quality_query = "{}{}".format(self.data_quality_query, table)
            record = redshift.get_records(quality_query)
            if ( int(record[0][0]) < self.value):
                raise ValueError(f"Data quality check failed in table: {table}")
                self.log.info(f"Quality checks fail in table: {table}")
            else:
                self.log.info(f"{table} pass in data quality check")
                
        self.log.info(f"Success Data Quality checks")
       
