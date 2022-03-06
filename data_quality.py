from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id = 'redshift',
                 aws_credentials_id = 'aws_credentials',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_connection_id = redshift_connection_id
        self.aws_credentials_id = aws_credentials_id
        self.checks = checks

    def execute(self, context):
        #Confirm tests are given
        if not self.checks:
            self.log.info(f"No data quality checks given")
            return
        
        #Use AWS Hook to get the credentials 
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        #Use Postgres Hook to run actual SQL commands on Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        
        #Empty list to track failed tests
        fail = []
        
        #Run tests
        for check in self.checks:
            sql = check['query']
            expected = check['result']
            
            output = redshift_hook.get_records(sql)[0][0]
            self.log.info(f"output value is {output}")
            self.log.info(f"expected value is {expected}")

            if output != expected:
                fail.append(sql)
        
        if fail:
            self.log.info(f"Below tests failed")
            self.log.info(fail)
        else:
            self.log.info(f"Hurray! All tests passed")