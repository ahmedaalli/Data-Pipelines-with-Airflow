from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    copy_sql = """
        COPY  {} 
        FROM '{}'
        REGION 'us-west-2'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(
        self,
        aws_conn_id="",
        redshift_conn_id="",
        table="",
        s3_bucket="",
        json_path="",
        *args,
        **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("connection complete")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket,
            credentials.access_key,
            credentials.secret_key,
            self.json_path,
        )
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(formatted_sql)
        self.log.info("data is transfered successfully")
