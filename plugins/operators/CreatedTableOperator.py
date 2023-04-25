from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreatedTableOperator(BaseOperator):

    @apply_defaults
    
    def __init__(self, create_table_dict, redshift_conn_id='', *args, **kwargs):
        super(CreatedTableOperator, self).__init__(*args, **kwargs)
        self.create_table_dict = create_table_dict
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """Loop focused on executing all SQL create tables included on the dict"""
        redshift = PostgresHook(self.redshift_conn_id)
  
        self.log.info(f'CreatedTableOperator is creating:')
        
        for table,sql in self.create_table_dict.items():
            self.log.info(f'\t{table}')
            redshift.run(sql)