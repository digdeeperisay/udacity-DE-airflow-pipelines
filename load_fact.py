from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_connection_id = 'redshift',
                 aws_credentials_id = 'aws_credentials',
                 load_sql_stmt = '',
                 region = 'us-west-2',
                 truncate_table=False,             
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
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
            self.log.info(f"Truncating songplays table")
            redshift_hook.run(f"TRUNCATE TABLE songplays")
           
        #Insert data into fact table
        self.log.info(f"Inserting data into songplays table")
        redshift_hook.run(self.load_sql_stmt)
        self.log.info(f"Songplays fact table populated")