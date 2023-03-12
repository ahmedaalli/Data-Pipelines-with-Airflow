from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", data_quality_check=[], *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_check = data_quality_check

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("connection completed")
        self.log.info("data quality checks starts")

        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM songplays")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(
                f"Data quality check failed. songplays returned no results"
            )
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. songplays contained 0 rows")
            self.log.info(
                f"Data quality on table songplays check passed with {num_records} records"
            )

        if self.data_quality_check:
            check_index = 0
            try:
                for check in self.data_quality_check:
                    check_query = check["data_check_sql"]
                    check_result = redshift_hook.get_records(check_query)
                    expected_result = check.get(
                        "expected_value", 0
                    )  # get check['expected_result'] if exists, else assume 0

                    if check_result == expected_result:
                        self.log.info(
                            f"additional data quality ckeck {check_index} passed"
                        )
                    else:
                        self.log.info(
                            f"additional data quality ckeck failed, returned:{check_result[0][0]},expected:{expected_result}"
                        )
                    check_index += 1
            except NameError:
                raise NameError("wrong data type for data qualtiy checks")
