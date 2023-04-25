from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} 'auto';
    """
    #template variable used for formatting s3 path in execute function
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 file_format='',
                 table='',
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.table = table

    def execute(self, context):
        """
        Executes formatted COPY command to stage data from S3 to Redshift.
        context -- DAG context dictionary
        """

        self.log.info('StageToRedshiftOperator instantiating AWS and Redshift connection variables')
        redshift = PostgresHook(self.redshift_conn_id)
        aws = AwsHook(self.aws_credentials_id)
        credentials = aws.get_credentials()

        #Formats s3 key with context dictionary
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        
        formatted_copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format
        )
        
        redshift.run(formatted_copy_sql)