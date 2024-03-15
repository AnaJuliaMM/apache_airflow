from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults
import pandas as pd 

class BigDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self,  file_path, destination_path, separator=';', file_type='parquet', *args, **kwargs):
        super().__init__( *args, **kwargs)
        self.file_path = file_path
        self.destination_path = destination_path
        self.separator = separator 
        self.file_type = file_type
        
    def execute(self, context):
        dataframe = pd.read_csv(self.file_path, sep=self.separator)
        if self.file_type == 'parquet':
            dataframe.to_parquet(self.destination_path)
        elif self.file_type == 'json':
            dataframe.to_json(self.destination_path)
        else:
            raise ValueError('Tipo inválidp')