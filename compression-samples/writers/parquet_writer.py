import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from writers.data_writer import DataWriter

class ParquetWriter(DataWriter):
  def convert(self, df: pd.DataFrame, partitions: list[str], root_path: str):
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(table, root_path=root_path,
                    partition_cols=partitions)

if __name__ == "__main__":
  root_path = "parquet_data"
  data = [{"col_1": 3, "col_2": 4}, {"col_1": 1, "col_2": 2}, {"col_1": 5, "col_2": 6}]
  df = pd.DataFrame(data)

  avro_writer = ParquetWriter(df)
  avro_writer.convert(df, ['col_1'], root_path)