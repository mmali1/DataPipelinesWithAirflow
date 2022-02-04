from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads fact table in Redshift from data in staging table(s)
    """
    ui_color = '#F98866'

    insert_query = """
                    INSERT INTO {}
                    {}
                   """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading data in fact table {self.table}")
        
        formatted_sql = LoadFactOperator.insert_query.format(self.table,\
                                          self.select_sql)
        
        self.log.info(f"Formatted sql : {formatted_sql}")
        redshift.run(formatted_sql)
        self.log.info(f"Finished loading {self.table}")
