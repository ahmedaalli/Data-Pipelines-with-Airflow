from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self, redshift_conn_id="", table="", sql="", append_only=False, *args, **kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("connection completed")
        insertsql = "insert into  {} \n {}".format(self.table, self.sql)
        self.log.info("copying started")
        redshift.run(insertsql)
        self.log.info("data is transfered successfully")
