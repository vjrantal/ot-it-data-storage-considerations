import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from readers.data_reader import DataReader

class ParquetReader(DataReader):
  def read(self, root_path: str) -> pd.DataFrame:
    table = pq.read_table(root_path)
    df = table.to_pandas()
    return df
