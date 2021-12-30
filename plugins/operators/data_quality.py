from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_cases=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for case in self.test_cases:
            result = int(redshift_hook.get_first(sql=case['sql'][0]))

            if case['comparison'] == '>':
                if result <= case['expected_result']:
                    raise AssertionError(f"Data quality check failed. The result of {case['sql'][0]} must be greater than {case['expected_result']}")
            elif case['comparison'] == '==':
                if result != case['expected_result']:
                    raise AssertionError(f"Data quality check failed. The result of {case['sql'][0]} must be {case['expected_result']}")

            self.log.info(f"Data quality passed on SQL: {case['sql'][0]}")