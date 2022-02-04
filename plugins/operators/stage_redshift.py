import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Copies json data from s3 to staging tables in redshift
    
    """
    ui_color = '#358140'
    
    template_fields = ("s3_key","execution_date")
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS json '{}'
    """
    
    copy_sql_partitioned = """
        COPY {} 
        FROM '{}/{}/{}/'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS json '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_json_option="",
                 region="",
                 use_partitioned_data="",
                 execution_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.copy_json_option = copy_json_option
        self.region = region
        self.execution_date = execution_date
        self.use_partitioned_data = use_partitioned_data

    def execute(self, context):
        # Get the aws credentials to access s3 bucket
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # Create a redshift hook 
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Truncate table
        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")
        
        # Copy the data from s3 to redshift
        self.log.info("Copying data from S3 to Redshift")
        
        # Generating path with provided s3_key
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        # If use_partitioned_data true, prepare paths using execution_date
                
        if self.use_partitioned_data == True:
            # Convert execution date string to datetime object in Y-M-D format
            execution_date_dt = datetime.datetime.strptime( self.execution_date, \
                                                    '%Y-%m-%d')
     
            formatted_sql = StageToRedshiftOperator.copy_sql_partitioned.format(
                self.table,
                s3_path,
                execution_date_dt.year,
                execution_date_dt.month,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.copy_json_option
            )
            self.log.info(f"Formatted SQL Query {formatted_sql}")
        else: 
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.copy_json_option
            )
        
        self.log.info(f"**Formatted sql: {formatted_sql}")

        redshift.run(formatted_sql)






