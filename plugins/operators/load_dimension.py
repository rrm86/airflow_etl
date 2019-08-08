'''
Implements
LoadDimensionOperator
'''
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.mode = mode

    def execute(self, context):
        self.log.info(f"Start LoadDimensionOperator")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if(self.mode not in('append-only','delete-load')):
            raise ValueError(f"The mode : {self.mode} is not expect")
        else:
            self.log.info(f"Truncate table: {self.table}")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
            
        insert_query = '''
            INSERT INTO {0} {1}
        '''.format(self.table, self.query)
        self.log.info(f"Copy data From staging tables to dimension table: {self.table}")
        redshift.run(insert_query)
        self.log.info(f"Success load data to the {self.table} table")
