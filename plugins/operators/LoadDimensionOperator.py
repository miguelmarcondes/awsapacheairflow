from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self):
        """
        Executes creation and insertion of Dimension Table in Redshift
        """

        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f'LoadDimensionOperator - Running SQL query focused on creating {self.table} table.')
        redshift.run(self.sql)
        self.log.info(f'LoadDimensionOperator - Finished running SQL query focused on creating {self.table} table.')
