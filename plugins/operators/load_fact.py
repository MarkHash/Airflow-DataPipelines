from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from plugins.helpers import sql_queries

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_table_sql="",
                 insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_sql = create_table_sql
        self.insert_sql = insert_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run(self.create_table_sql)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Inserting data to songplays table")
        redshift.run(self.insert_sql)
        self.log.info("Inserting data to {} was successful".format(self.table))