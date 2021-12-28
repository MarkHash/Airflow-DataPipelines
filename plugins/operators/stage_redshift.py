import typing_extensions
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers.sql_queries import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_directory="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_directory=s3_directory

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        self.log.info("table: {}, s3_directory: {}, access_key: {}, secret_key: {}".format(self.table, self.s3_directory, credentials.access_key, credentials.secret_key))
        if self.table == "staging_events":
            formatted_sql = SqlQueries.EVENTS_COPY_SQL.format(
                self.table,
                self.s3_directory,
                credentials.access_key,
                credentials.secret_key
            )
        elif self.table == "staging_songs":
            formatted_sql = SqlQueries.SONGS_COPY_SQL.format(
                self.table,
                self.s3_directory,
                credentials.access_key,
                credentials.secret_key
            )
        else:
            self.log.info("Data import failed.")

        redshift.run(formatted_sql)
        self.log.info("Data import to {} was successful".format(self.table))