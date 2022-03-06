from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    #this command will be used to generate the actual code by replacing all {} with actual values
    sql_to_copy_to_redshift = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_connection_id = 'redshift',
                 aws_credentials_id = 'aws_credentials',
                 s3_bucket = 'udacity-dend',
                 s3_folder = '',
                 region = 'us-west-2',
                 extra_params = '',
                 truncate_table=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_connection_id = redshift_connection_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.region = region
        self.extra_params = extra_params
        self.truncate_table = truncate_table
        
    def execute(self, context):
        #Use AWS Hook to get the credentials 
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        #Use Postgres Hook to run actual SQL commands on Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_connection_id)

        #Generate the S3 path. Bucket is the same but folder determines log/song data
        s3_full_path = f"s3://{self.s3_bucket}/{self.s3_folder}"
        
        #Truncate table if needed
        if self.truncate_table:
            self.log.info(f"Truncating table {self.table}")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")
            
        parameterized_sql = StageToRedshiftOperator.sql_to_copy_to_redshift.format(
            self.table,
            s3_full_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.extra_params
        )
        
        self.log.info(f"Executing query to copy data from S3 to Redshift table {self.table}")
        redshift_hook.run(parameterized_sql)
        self.log.info(f"S3 to Redshift table {self.table} copy complete")





