from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension tables from staging tables to redshift
    """
    ui_color = '#80BD9E'
    
    insert_sql = """
                    INSERT INTO {} 
                    {}
                 """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 append_insert=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.append_insert = append_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        if self.append_insert:
            formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, \
                                              self.select_sql)     
        else:
            self.log.info("Deleting data from dimension table in Redshift")      
            redshift.run(f"DELETE FROM {self.table}")  
            formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, \
                                              self.select_sql)
        
        self.log.info(f"Formatted sql : {formatted_sql}") 
        self.log.info(f"Loading data into dimension table {self.table}")
        redshift.run(formatted_sql)
