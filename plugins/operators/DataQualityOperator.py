from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table, columns in self.tables.items():
            for column in columns:
                sql = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
                expected_result = 0

                records = redshift_hook.get_records(sql)[0]

                if expected_result != records[0]:
                    raise ValueError(f"Data quality check failed for table {table} column {column}. Expected {expected_result}, but got {records[0]}")
                else:
                    self.log.info(f"Data quality check passed for table {table} column {column}. Expected {expected_result} and got {records[0]}")