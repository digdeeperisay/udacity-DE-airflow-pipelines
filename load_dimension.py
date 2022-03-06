from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_connection_id = 'redshift',
                 aws_credentials_id = 'aws_credentials',
                 load_sql_stmt = '',
                 region = 'us-west-2',
                 truncate_table=False,             
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_connection_id = redshift_connection_id
        self.aws_credentials_id = aws_credentials_id
        self.load_sql_stmt = load_sql_stmt
        self.region = region
        self.truncate_table = truncate_table

    def execute(self, context):
        #Use AWS Hook to get the credentials 
#         aws_hook = AwsHook(self.aws_credentials_id)
#         credentials = aws_hook.get_credentials()
        
        #Use Postgres Hook to run actual SQL commands on Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        
        #Truncate table if needed
        if self.truncate_table:
            self.log.info(f"Truncating table {self.table}")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")
           
        #Insert data into dimension table
        self.log.info(f"Inserting data into {self.table} table")
        redshift_hook.run(self.load_sql_stmt)
        self.log.info(f"{self.table} dimension table populated")