import typing_extensions
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from plugins.helpers import sql_queries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        if self.table == "staging_events":
            formatted_sql = sql_queries.EVENTS_COPY_SQL.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key)
        elif self.table == "staging_songs":
            formatted_sql = sql_queries.SONGS_COPY_SQL.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key)
        else:
            self.log.info("Data import failed.")

        redshift.run(formatted_sql)
        self.log.info("Data import to {} was successful".format(self.table))