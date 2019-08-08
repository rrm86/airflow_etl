'''
Implements
LoadFactOperator
'''
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''
    Inherits BaseOperator
    and implements 
    LoadFactOperator
    '''

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        '''
        Insert data
        into fact table
        '''
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Select, Join, filtring and insert data to staging tables to fact table: {self.table}")
        insert_query = "INSERT INTO {0} {1}".format(self.table,self.query)
        redshift.run(insert_query)
        
        self.log.info(f"Finished loading data from staging tables to the {self.table}")